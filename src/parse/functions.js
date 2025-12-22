import { argCountParseError } from '../parseErrors.js'
import { validateFunctionArgCount } from '../validation.js'
import { parseExpression } from './expression.js'
import { consume, current, expect, lastPosition, match } from './state.js'

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
  consume(state) // '('

  /** @type {ExprNode[]} */
  const args = []
  let distinct = false

  // Check for DISTINCT or ALL keyword (for aggregate functions like COUNT(DISTINCT x))
  if (current(state).type === 'keyword' && current(state).value === 'DISTINCT') {
    consume(state)
    distinct = true
  } else if (current(state).type === 'keyword' && current(state).value === 'ALL') {
    consume(state)
  }

  if (current(state).type !== 'paren' || current(state).value !== ')') {
    while (true) {
      // Handle COUNT(*) - treat * as a special identifier
      if (current(state).type === 'operator' && current(state).value === '*') {
        const starTok = current(state)
        consume(state)
        args.push({
          type: 'identifier',
          name: '*',
          positionStart: starTok.positionStart,
          positionEnd: lastPosition(state),
        })
      } else {
        args.push(parseExpression(state))
      }
      if (!match(state, 'comma')) break
    }
  }

  expect(state, 'paren', ')')

  // Validate argument count at parse time
  const funcNameUpper = funcName.toUpperCase()
  const validation = validateFunctionArgCount(funcNameUpper, args.length, state.functions)
  if (!validation.valid) {
    throw argCountParseError({
      funcName,
      expected: validation.expected,
      received: args.length,
      positionStart,
      positionEnd: lastPosition(state),
    })
  }

  return {
    type: 'function',
    name: funcName,
    args,
    distinct: distinct || undefined,
    positionStart,
    positionEnd: lastPosition(state),
  }
}
