import { syntaxError } from '../parseErrors.js'

/**
 * @import { ParserState, Token, TokenType } from '../types.js'
 * @import { ParseError } from '../parseErrors.js'
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
    state.pos += 1
  }
  return tok
}

/**
 * @param {ParserState} state
 * @param {TokenType} type
 * @param {string} [value]
 * @returns {boolean}
 */
export function match(state, type, value) {
  const tok = current(state)
  if (tok.type !== type) return false
  if (typeof value === 'string' && tok.value !== value) return false
  consume(state)
  return true
}

/**
 * @param {ParserState} state
 * @param {TokenType} type
 * @param {string} value
 * @returns {Token}
 */
export function expect(state, type, value) {
  const tok = current(state)
  if (tok.type !== type || tok.value !== value) {
    throw parseError(state, value)
  }
  consume(state)
  return tok
}

/**
 * @param {ParserState} state
 * @returns {Token}
 */
export function expectIdentifier(state) {
  const tok = current(state)
  if (tok.type !== 'identifier') {
    throw parseError(state, 'identifier')
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
  const after = prevToken ? prevToken.originalValue ?? prevToken.value : undefined
  const received = tok.type === 'eof' ? 'end of query' : `"${tok.originalValue ?? tok.value}"`
  return syntaxError({ expected, received, positionStart: tok.positionStart, positionEnd: tok.positionEnd, after })
}
