import { isAggregateFunc, isKnownFunction, niladicFuncs, validateFunctionArgs } from '../validation/functions.js'
import { ParseError, syntaxError, unknownFunctionError } from '../validation/parseErrors.js'
import { parseExpression } from './expression.js'
import { consume, current, expect, match } from './state.js'

/**
 * @import { ExprNode, ParserState } from '../types.js'
 */

/**
 * @param {ParserState} state
 * @param {number} positionStart
 * @returns {ExprNode}
 */
export function parseFunctionCall(state, positionStart) {
  const funcTok = consume(state)
  const funcName = funcTok.value
  const funcNameUpper = funcName.toUpperCase()

  // Validate function existence early for better error messages
  if (!isKnownFunction(funcNameUpper, state.functions)) {
    throw unknownFunctionError({ funcName, ...funcTok })
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

  // Validate argument count at parse time
  validateFunctionArgs(funcNameUpper, args.length, positionStart, state.lastPos, state.functions)

  // Check for FILTER clause (only valid for aggregate functions)
  /** @type {ExprNode | undefined} */
  let filter
  const filterTok = current(state)
  if (match(state, 'keyword', 'FILTER')) {
    if (!isAggregateFunc(funcNameUpper)) {
      throw syntaxError({
        expected: 'aggregate function for FILTER clause',
        received: `FILTER on non-aggregate function "${funcName}"`,
        ...filterTok,
      })
    }
    expect(state, 'paren', '(')
    expect(state, 'keyword', 'WHERE')
    filter = parseExpression(state)
    expect(state, 'paren', ')')
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
