import { isAggregateFunc, isKnownFunction, isWindowFunc, niladicFuncs, validateFunctionArgs } from '../validation/functions.js'
import { ParseError, UnknownFunctionError } from '../validation/parseErrors.js'
import { parseExpression } from './expression.js'
import { consume, current, expect, match } from './state.js'

/**
 * @import { ExprNode, OrderByItem, ParserState } from '../types.js'
 */

/**
 * @param {ParserState} state
 * @param {number} positionStart
 * @returns {ExprNode}
 */
export function parseFunctionCall(state, positionStart) {
  const funcTok = consume(state)
  const funcName = funcTok.originalValue ?? funcTok.value
  const funcNameUpper = funcName.toUpperCase()

  // Validate function existence early for better error messages
  if (!isKnownFunction(funcNameUpper, state.functions)) {
    throw new UnknownFunctionError({ funcName, ...funcTok })
  }

  // Niladic datetime functions (no parentheses required per ANSI SQL)
  const parens = current(state)
  if (niladicFuncs.includes(funcNameUpper) && parens.type !== 'paren' || parens.value !== '(') {
    return {
      type: 'function',
      funcName,
      args: [],
      positionStart,
      positionEnd: state.lastPos,
    }
  }

  expect(state, 'paren', '(')

  /** @type {ExprNode[]} */
  const args = []
  /** @type {true | undefined} */
  let distinct

  // Check for DISTINCT or ALL keyword (for aggregate functions like COUNT(DISTINCT x))
  if (match(state, 'keyword', 'DISTINCT')) {
    distinct = true
  } else {
    match(state, 'keyword', 'ALL')
  }

  // Parse function arguments
  while (true) {
    const next = current(state)
    if (next.type === 'paren' && next.value === ')') break

    // Handle COUNT(*) - treat * as a special identifier
    if (match(state, 'operator', '*')) {
      args.push({
        type: 'star',
        positionStart: next.positionStart,
        positionEnd: state.lastPos,
      })
    } else {
      args.push(parseExpression(state))
    }
    if (!match(state, 'comma')) break
  }
  expect(state, 'paren', ')')

  // Validate star argument at parse time (only COUNT supports *)
  const hasStar = args.length === 1 && args[0].type === 'star'
  if (hasStar && funcNameUpper !== 'COUNT') {
    throw new ParseError({
      message: `${funcName} cannot be applied to "*"`,
      positionStart,
      positionEnd: state.lastPos,
    })
  }
  if (hasStar && distinct) {
    throw new ParseError({
      message: 'COUNT(DISTINCT *) is not allowed',
      positionStart,
      positionEnd: state.lastPos,
    })
  }

  // Check for WITHIN GROUP (ORDER BY expr) clause — standard SQL ordered-set aggregate syntax.
  // Supported for PERCENTILE_CONT: PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY expr)
  const withinTok = current(state)
  if (match(state, 'keyword', 'WITHIN')) {
    if (funcNameUpper !== 'PERCENTILE_CONT') {
      throw new ParseError({
        message: `WITHIN GROUP is only supported for PERCENTILE_CONT, not "${funcName}"`,
        ...withinTok,
      })
    }
    if (args.length !== 1) {
      throw new ParseError({
        message: `${funcName}: cannot combine WITHIN GROUP with a value argument`,
        ...withinTok,
      })
    }
    expect(state, 'keyword', 'GROUP')
    expect(state, 'paren', '(')
    expect(state, 'keyword', 'ORDER')
    expect(state, 'keyword', 'BY')
    args.push(parseExpression(state))
    expect(state, 'paren', ')')
  }

  // Validate argument count at parse time
  validateFunctionArgs(funcNameUpper, args.length, positionStart, state.lastPos, state.functions)

  // Check for FILTER clause (only valid for aggregate functions)
  /** @type {ExprNode | undefined} */
  let filter
  const filterTok = current(state)
  if (match(state, 'keyword', 'FILTER')) {
    if (!isAggregateFunc(funcNameUpper)) {
      throw new ParseError({
        message: `FILTER cannot be applied to non-aggregate function "${funcName}"`,
        ...filterTok,
      })
    }
    expect(state, 'paren', '(')
    expect(state, 'keyword', 'WHERE')
    filter = parseExpression(state)
    expect(state, 'paren', ')')
  }

  // Check for OVER clause
  const overTok = current(state)
  const hasOver = overTok.type === 'identifier' && overTok.value.toUpperCase() === 'OVER'

  if (hasOver) {
    if (!isWindowFunc(funcNameUpper)) {
      throw new ParseError({
        message: `Window functions are not supported: ${funcName}(...) OVER (...)`,
        positionStart,
        positionEnd: overTok.positionEnd,
      })
    }
    if (filter) {
      throw new ParseError({
        message: `FILTER cannot be combined with OVER for "${funcName}"`,
        positionStart,
        positionEnd: overTok.positionEnd,
      })
    }
    consume(state)
    const { partitionBy, orderBy } = parseWindowSpec(state, positionStart)
    return {
      type: 'window',
      funcName,
      args,
      partitionBy,
      orderBy,
      positionStart,
      positionEnd: state.lastPos,
    }
  }

  if (isWindowFunc(funcNameUpper)) {
    throw new ParseError({
      message: `${funcName}() requires an OVER clause at position ${positionStart}`,
      positionStart,
      positionEnd: state.lastPos,
    })
  }

  return {
    type: 'function',
    funcName,
    args,
    distinct,
    filter,
    positionStart,
    positionEnd: state.lastPos,
  }
}

/**
 * Parses the window spec after OVER: ( [PARTITION BY expr[, ...]] [ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST][, ...]] )
 *
 * @param {ParserState} state
 * @param {number} positionStart - start position of the enclosing function call (for OrderByItem positions)
 * @returns {{ partitionBy: ExprNode[], orderBy: OrderByItem[] }}
 */
function parseWindowSpec(state, positionStart) {
  expect(state, 'paren', '(')
  /** @type {ExprNode[]} */
  const partitionBy = []
  /** @type {OrderByItem[]} */
  const orderBy = []

  const partitionTok = current(state)
  if (partitionTok.type === 'identifier' && partitionTok.value.toUpperCase() === 'PARTITION') {
    consume(state)
    expect(state, 'keyword', 'BY')
    while (true) {
      partitionBy.push(parseExpression(state))
      if (!match(state, 'comma')) break
    }
  }

  if (match(state, 'keyword', 'ORDER')) {
    expect(state, 'keyword', 'BY')
    while (true) {
      const expr = parseExpression(state)
      /** @type {'ASC' | 'DESC'} */
      let direction = 'ASC'
      if (match(state, 'keyword', 'ASC')) {
        direction = 'ASC'
      } else if (match(state, 'keyword', 'DESC')) {
        direction = 'DESC'
      }
      /** @type {'FIRST' | 'LAST' | undefined} */
      let nulls
      if (match(state, 'keyword', 'NULLS')) {
        const tok = consume(state)
        const upper = tok.value.toUpperCase()
        if (tok.type === 'identifier' && upper === 'FIRST') {
          nulls = 'FIRST'
        } else if (tok.type === 'identifier' && upper === 'LAST') {
          nulls = 'LAST'
        } else {
          throw new ParseError({
            message: `Expected FIRST or LAST after NULLS at position ${tok.positionStart}`,
            positionStart: tok.positionStart,
            positionEnd: tok.positionEnd,
          })
        }
      }
      orderBy.push({ expr, direction, nulls, positionStart, positionEnd: state.lastPos })
      if (!match(state, 'comma')) break
    }
  }

  expect(state, 'paren', ')')
  return { partitionBy, orderBy }
}
