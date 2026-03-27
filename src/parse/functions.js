import { isAggregateFunc, validateFunctionArgCount as validateFunctionArgs } from '../validation/functions.js'
import { ParseError, syntaxError } from '../validation/parseErrors.js'
import { parseExpression } from './expression.js'
import { consume, current, expect, match } from './state.js'

/**
 * @import { ExprNode, ParserState } from '../types.js'
 */

/**
 * Parses a function call after the function name has been identified.
 * Expects the current token to be '('.
 *
 * @param {ParserState} state
 * @param {string} funcName - The function name
 * @param {number} positionStart - Start position of the function name
 * @returns {ExprNode}
 */
export function parseFunctionCall(state, funcName, positionStart) {
  const funcNameUpper = funcName.toUpperCase()
  consume(state) // '(' checked by caller

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
  if (current(state).type !== 'paren' || current(state).value !== ')') {
    while (true) {
      // Handle COUNT(*) - treat * as a special identifier
      const starTok = current(state)
      if (match(state, 'operator', '*')) {
        args.push({
          type: 'star',
          positionStart: starTok.positionStart,
          positionEnd: state.lastPos,
        })
      } else {
        args.push(parseExpression(state))
      }
      if (!match(state, 'comma')) break
    }
  }
  expect(state, 'paren', ')')
  const functionEnd = state.lastPos

  // Validate star argument at parse time (only COUNT supports *)
  const hasStar = args.length === 1 && args[0].type === 'star'
  if (hasStar && funcNameUpper !== 'COUNT') {
    throw new ParseError({
      message: `${funcName} cannot be applied to "*"`,
      positionStart,
      positionEnd: functionEnd,
    })
  }
  if (hasStar && distinct) {
    throw new ParseError({
      message: 'COUNT(DISTINCT *) is not allowed',
      positionStart,
      positionEnd: functionEnd,
    })
  }

  // Validate argument count at parse time
  validateFunctionArgs(funcNameUpper, args.length, positionStart, functionEnd, state.functions)

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
