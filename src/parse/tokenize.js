import {
  invalidLiteralError,
  unexpectedCharError,
  unterminatedError,
} from '../errors.js'

/**
 * @import { Token } from '../types.d.ts'
 */

const KEYWORDS = new Set([
  'SELECT',
  'FROM',
  'WHERE',
  'AND',
  'OR',
  'NOT',
  'IS',
  'GROUP',
  'BY',
  'HAVING',
  'ORDER',
  'ASC',
  'DESC',
  'NULLS',
  'LIMIT',
  'OFFSET',
  'AS',
  'ALL',
  'DISTINCT',
  'TRUE',
  'FALSE',
  'NULL',
  'LIKE',
  'IN',
  'EXISTS',
  'BETWEEN',
  'CASE',
  'WHEN',
  'THEN',
  'ELSE',
  'END',
  'JOIN',
  'INNER',
  'LEFT',
  'RIGHT',
  'FULL',
  'OUTER',
  'ON',
  'INTERVAL',
  'DAY',
  'MONTH',
  'YEAR',
  'HOUR',
  'MINUTE',
  'SECOND',
])

/**
 * @param {string} sql
 * @returns {Token[]}
 */
export function tokenize(sql) {
  /** @type {Token[]} */
  const tokens = []
  const { length } = sql
  let i = 0

  /**
   * @returns {string}
   */
  function peek() {
    if (i >= length) return ''
    return sql[i]
  }

  /**
   * @returns {string}
   */
  function nextChar() {
    if (i >= length) return ''
    const ch = sql[i]
    i++
    return ch
  }

  /**
   * @param {number} startPos
   * @param {string} prefix
   * @returns {Token}
   */
  function parseNumber(startPos, prefix = '') {
    let text = prefix
    while (isDigit(peek())) {
      text += nextChar()
    }
    if (peek() === '.') {
      text += nextChar()
      while (isDigit(peek())) {
        text += nextChar()
      }
    }
    // exponent
    if (peek() === 'e' || peek() === 'E') {
      text += nextChar()
      if (peek() === '+' || peek() === '-') {
        text += nextChar()
      }
      while (isDigit(peek())) {
        text += nextChar()
      }
    }
    // bigint suffix
    if (peek() === 'n') {
      text += nextChar()
      try {
        return {
          type: 'number',
          value: text,
          position: startPos,
          numericValue: BigInt(text.slice(0, -1)),
        }
      } catch {
        throw invalidLiteralError({ type: 'bigint', value: text.slice(0, -1), position: startPos })
      }
    }
    if (isAlpha(peek())) {
      throw invalidLiteralError({ type: 'number', value: text + peek(), position: startPos })
    }
    const num = parseFloat(text)
    if (isNaN(num)) {
      throw invalidLiteralError({ type: 'number', value: text, position: startPos })
    }
    return {
      type: 'number',
      value: text,
      position: startPos,
      numericValue: num,
    }
  }

  while (i < length) {
    const ch = peek()

    if (isWhitespace(ch)) {
      nextChar()
      continue
    }

    // line comment --
    if (ch === '-' && i + 1 < length && sql[i + 1] === '-') {
      while (i < length && sql[i] !== '\n') {
        i++
      }
      continue
    }

    // block comment /* ... */
    if (ch === '/' && i + 1 < length && sql[i + 1] === '*') {
      i += 2
      while (i < length) {
        if (sql[i] === '*' && i + 1 < length && sql[i + 1] === '/') {
          i += 2
          break
        }
        i++
      }
      continue
    }

    const pos = i

    // negative numbers (when not subtraction)
    if (ch === '-' && i + 1 < length && isDigit(sql[i + 1])) {
      const lastToken = tokens[tokens.length - 1]
      const isValueBefore = lastToken && (
        lastToken.type === 'identifier' ||
        lastToken.type === 'number' ||
        lastToken.type === 'string' ||
        lastToken.type === 'paren' && lastToken.value === ')'
      )
      if (!isValueBefore) {
        nextChar() // consume '-'
        tokens.push(parseNumber(pos, '-'))
        continue
      }
    }

    // numbers
    if (isDigit(ch)) {
      tokens.push(parseNumber(pos))
      continue
    }

    // identifiers / keywords
    if (isAlpha(ch)) {
      let text = ''
      while (isAlphaNumeric(peek())) {
        text += nextChar()
      }
      const upper = text.toUpperCase()
      if (KEYWORDS.has(upper)) {
        tokens.push({
          type: 'keyword',
          value: upper,
          originalValue: text,
          position: pos,
        })
      } else {
        tokens.push({
          type: 'identifier',
          value: text,
          position: pos,
        })
      }
      continue
    }

    // string literals: single quotes
    if (ch === '\'') {
      const quote = nextChar()
      let text = ''
      while (i <= length) {
        if (i === length) {
          throw unterminatedError('string', pos)
        }
        const c = nextChar()
        if (c === quote) {
          // check for escaped quote
          if (peek() === quote) {
            text += quote
            nextChar()
            continue
          }
          break
        }
        text += c
      }
      tokens.push({
        type: 'string',
        value: text,
        position: pos,
      })
      continue
    }

    // quoted identifiers: double quotes
    if (ch === '"') {
      const quote = nextChar()
      let text = ''
      while (i <= length) {
        if (i === length) {
          throw unterminatedError('identifier', pos)
        }
        const c = nextChar()
        if (c === quote) {
          // check for escaped quote
          if (peek() === quote) {
            text += quote
            nextChar()
            continue
          }
          break
        }
        text += c
      }
      tokens.push({
        type: 'identifier',
        value: text,
        position: pos,
      })
      continue
    }

    // two-character operators
    if (ch === '<' || ch === '>' || ch === '!' || ch === '=') {
      let op = nextChar()
      if ((op === '<' || op === '>' || op === '!') && peek() === '=') {
        op += nextChar()
      } else if (op === '<' && peek() === '>') {
        op += nextChar()
      }
      tokens.push({
        type: 'operator',
        value: op,
        position: pos,
      })
      continue
    }

    // single-char operators
    if (ch === '*' || ch === '+' || ch === '-' || ch === '/' || ch === '%') {
      nextChar()
      tokens.push({
        type: 'operator',
        value: ch,
        position: pos,
      })
      continue
    }

    if (ch === ',') {
      nextChar()
      tokens.push({
        type: 'comma',
        value: ',',
        position: pos,
      })
      continue
    }

    if (ch === '.') {
      nextChar()
      tokens.push({
        type: 'dot',
        value: '.',
        position: pos,
      })
      continue
    }

    if (ch === '(' || ch === ')') {
      nextChar()
      tokens.push({
        type: 'paren',
        value: ch,
        position: pos,
      })
      continue
    }

    if (ch === ';') {
      nextChar()
      tokens.push({
        type: 'semicolon',
        value: ';',
        position: pos,
      })
      continue
    }

    if (tokens.length === 0) {
      throw unexpectedCharError(ch, pos, true)
    }
    throw unexpectedCharError(ch, pos)
  }

  tokens.push({
    type: 'eof',
    value: '',
    position: length,
  })

  return tokens
}

/**
 * @param {string} ch
 * @returns {boolean}
 */
function isWhitespace(ch) {
  return ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r'
}

/**
 * @param {string} ch
 * @returns {boolean}
 */
function isDigit(ch) {
  return ch >= '0' && ch <= '9'
}

/**
 * @param {string} ch
 * @returns {boolean}
 */
function isAlpha(ch) {
  return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch === '_' || ch === '$'
}

/**
 * @param {string} ch
 * @returns {boolean}
 */
function isAlphaNumeric(ch) {
  return isAlpha(ch) || isDigit(ch)
}
