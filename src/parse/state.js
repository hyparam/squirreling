import { SyntaxError } from '../validation/parseErrors.js'

/**
 * @import { ParserState, Token, TokenType } from '../types.js'
 * @import { ParseError } from '../validation/parseErrors.js'
 */

/**
 * @param {ParserState} state
 * @returns {Token}
 */
export function current(state) {
  return state.tokens[state.pos]
}

/**
 * @param {ParserState} state
 * @param {number} offset
 * @returns {Token}
 */
export function peekToken(state, offset) {
  const idx = state.pos + offset
  if (idx >= state.tokens.length) {
    return state.tokens[state.tokens.length - 1]
  }
  return state.tokens[idx]
}

/**
 * @param {ParserState} state
 * @returns {Token}
 */
export function consume(state) {
  const tok = current(state)
  state.lastPos = tok.positionEnd
  if (state.pos < state.tokens.length - 1) {
    state.pos++
  }
  return tok
}

/**
 * @param {ParserState} state
 * @param {TokenType} type
 * @param {string} [expected]
 * @returns {boolean}
 */
export function match(state, type, expected) {
  const tok = current(state)
  if (tok.type !== type) return false
  if (expected && tok.value !== expected) return false
  consume(state)
  return true
}

/**
 * @param {ParserState} state
 * @param {TokenType} type
 * @param {string} [expected]
 * @returns {Token}
 */
export function expect(state, type, expected) {
  const tok = current(state)
  if (tok.type !== type || expected && tok.value !== expected) {
    throw parseError(state, expected ?? type)
  }
  consume(state)
  return tok
}

/**
 * Helper function to create consistent parser error messages.
 * @param {ParserState} state
 * @param {string} expected - Description of what was expected
 * @returns {ParseError}
 */
export function parseError(state, expected) {
  const tok = current(state)
  const prevToken = state.tokens[state.pos - 1]
  const after = prevToken?.originalValue ?? prevToken?.value
  return new SyntaxError({ expected, after, ...tok })
}
