import { KEYWORDS } from '../validation/keywords.js'
import { InvalidLiteralError, ParseError, UnexpectedCharError } from '../validation/parseErrors.js'

/**
 * @import { Token } from '../types.d.ts'
 */

const NUMBER_REGEX = /^-?(?:\d+n|(?:\d+\.?\d*|\d*\.\d+)(?:[eE][+-]?\d+)?)/

/**
 * @param {string} query
 * @returns {Token[]}
 */
export function tokenizeSql(query) {
  /** @type {Token[]} */
  const tokens = []
  const len = query.length
  let i = 0 // current position in query string

  /**
   * @returns {string}
   */
  function peek() {
    return query[i]
  }

  /**
   * @returns {string}
   */
  function nextChar() {
    return query[i++]
  }

  /**
   * @returns {Token}
   */
  function parseNumber() {
    const positionStart = i
    let value = query.slice(i).match(NUMBER_REGEX)?.[0] ?? ''
    i += value.length
    // check for invalid characters immediately following the number
    const ch = peek()
    if (!value || isAlphaNumeric(ch) || ch === '.') {
      const after = query.slice(i).match(/^-?(?:[0-9a-zA-Z_$.]*[0-9][eE][+-]?[0-9])?[0-9a-zA-Z_$.]*/)?.[0] ?? ch
      value += after
      i += after.length
      throw new InvalidLiteralError({ expected: 'number', value, positionStart, positionEnd: i })
    }
    if (value.endsWith('n')) {
      return {
        type: 'number',
        value,
        numericValue: BigInt(value.slice(0, -1)),
        positionStart,
        positionEnd: i,
      }
    }
    return {
      type: 'number',
      value,
      numericValue: Number(value),
      positionStart,
      positionEnd: i,
    }
  }

  while (i < len) {
    const positionStart = i
    const ch = query[i]
    const next = query[i + 1]

    if (isWhitespace(ch)) {
      i++
      continue
    }

    // line comment --
    if (ch === '-' && next === '-') {
      while (i < len && query[i] !== '\n') {
        i++
      }
      continue
    }

    // block comment /* ... */
    if (ch === '/' && next === '*') {
      i += 3
      while (i < len) {
        if (query[i - 1] === '*' && query[i] === '/') {
          i++
          break
        }
        i++
      }
      continue
    }

    // negative numbers (when not subtraction)
    if (ch === '-' && (isDigit(next) || next === '.' && isDigit(query[i + 2]))) {
      const lastToken = tokens[tokens.length - 1]
      const isValueBefore = lastToken && (
        lastToken.type === 'identifier' ||
        lastToken.type === 'number' ||
        lastToken.type === 'string' ||
        lastToken.type === 'paren' && lastToken.value === ')'
      )
      if (!isValueBefore) {
        tokens.push(parseNumber())
        continue
      }
    }

    // numbers
    if (isDigit(ch) || ch === '.' && isDigit(next)) {
      tokens.push(parseNumber())
      continue
    }

    // identifiers / keywords
    if (isAlpha(ch)) {
      do {
        i++
      } while (isAlphaNumeric(query[i]))
      const value = query.slice(positionStart, i)
      const upper = value.toUpperCase()
      if (KEYWORDS.has(upper)) {
        tokens.push({
          type: 'keyword',
          value: upper, // uppercase for keywords
          originalValue: value, // preserve user casing
          positionStart,
          positionEnd: i,
        })
      } else {
        tokens.push({
          type: 'identifier',
          value,
          positionStart,
          positionEnd: i,
        })
      }
      continue
    }

    // string literals (single quotes) and quoted identifiers (double quotes)
    if (ch === '\'' || ch === '"') {
      const type = ch === '\'' ? 'string' : 'identifier'
      const quote = nextChar()
      let value = ''
      while (i <= len) {
        if (i === len) {
          throw new ParseError({
            message: `Unterminated ${type} starting at position ${positionStart}`,
            positionStart,
            positionEnd: i,
          })
        }
        const c = nextChar()
        if (c === quote) {
          // check for escaped quote
          if (peek() === quote) {
            value += quote
            i++
            continue
          }
          // end quote
          break
        }
        value += c
      }
      tokens.push({ type, value, positionStart, positionEnd: i })
      continue
    }

    // operators
    if ('<>!=+-*/%'.includes(ch)) {
      let op = nextChar()
      if ((op === '<' || op === '>' || op === '!' || op === '=') && peek() === '=') {
        op += nextChar()
      } else if (op === '<' && peek() === '>') {
        op += nextChar()
      }
      tokens.push({
        type: 'operator',
        value: op,
        positionStart,
        positionEnd: i,
      })
      continue
    }

    if (ch === ',') {
      i++
      tokens.push({
        type: 'comma',
        value: ',',
        positionStart,
        positionEnd: i,
      })
      continue
    }

    if (ch === '.') {
      i++
      tokens.push({
        type: 'dot',
        value: '.',
        positionStart,
        positionEnd: i,
      })
      continue
    }

    if (ch === '(' || ch === ')') {
      i++
      tokens.push({
        type: 'paren',
        value: ch,
        positionStart,
        positionEnd: i,
      })
      continue
    }

    if (ch === ';') {
      i++
      tokens.push({
        type: 'semicolon',
        value: ';',
        positionStart,
        positionEnd: i,
      })
      continue
    }

    throw new UnexpectedCharError({ char: ch, positionStart, expectsSelect: !tokens.length })
  }

  tokens.push({
    type: 'eof',
    value: '',
    positionStart: len,
    positionEnd: len,
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
