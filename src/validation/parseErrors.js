import { FUNCTION_SIGNATURES } from './functions.js'

/**
 * Structured parse error with position range.
 */
export class ParseError extends Error {
  /**
   * @param {Object} options
   * @param {string} options.message - Human-readable error message
   * @param {number} options.positionStart
   * @param {number} options.positionEnd
   */
  constructor({ message, positionStart, positionEnd }) {
    super(message)
    this.name = 'ParseError'
    this.positionStart = positionStart
    this.positionEnd = positionEnd
  }
}

/**
 * General syntax error for unexpected tokens.
 *
 * @param {Object} options
 * @param {string} options.expected - Description of what was expected
 * @param {string} options.value - What was actually found
 * @param {number} options.positionStart
 * @param {number} options.positionEnd
 * @param {string} [options.after] - What token came before (for context)
 * @returns {ParseError}
 */
export function syntaxError({ expected, value, positionStart, positionEnd, after }) {
  after = after ? ` after "${after}"` : ''
  value = value ? `"${value}"` : 'end of query'
  return new ParseError({ message: `Expected ${expected}${after} but found ${value} at position ${positionStart}`, positionStart, positionEnd })
}

/**
 * Error for invalid literals (numbers, intervals, etc).
 *
 * @param {Object} options
 * @param {string} options.expected - Type of invalid literal (e.g., 'number', 'interval value', 'interval unit')
 * @param {string} options.value - The invalid value
 * @param {number} options.positionStart
 * @param {number} options.positionEnd
 * @param {string} [options.validValues] - List of valid values (for enums like interval units)
 * @returns {ParseError}
 */
export function invalidLiteralError({ expected, value, positionStart, positionEnd, validValues }) {
  const suffix = validValues ? `. Valid values: ${validValues}` : ''
  return new ParseError({ message: `Invalid ${expected} ${value} at position ${positionStart}${suffix}`, positionStart, positionEnd })
}

/**
 * Error for unexpected characters during tokenization.
 *
 * @param {Object} options
 * @param {string} options.char - The unexpected character
 * @param {number} options.positionStart
 * @param {boolean} options.expectsSelect - Whether SELECT was expected (first token)
 * @returns {ParseError}
 */
export function unexpectedCharError({ char, positionStart, expectsSelect }) {
  const positionEnd = positionStart + 1
  if (expectsSelect) {
    return new ParseError({ message: `Expected SELECT but found "${char}" at position ${positionStart}. Queries must start with SELECT or WITH.`, positionStart, positionEnd })
  }
  return new ParseError({ message: `Unexpected character "${char}" at position ${positionStart}`, positionStart, positionEnd })
}

/**
 * Error for unknown/unsupported functions.
 *
 * @param {Object} options
 * @param {string} options.funcName - The unknown function name
 * @param {number} options.positionStart
 * @param {number} options.positionEnd
 * @returns {ParseError}
 */
export function unknownFunctionError({ funcName, positionStart, positionEnd }) {
  const suggestions = suggestFunctions(funcName)
  let message = `Unknown function "${funcName}" at position ${positionStart}.`
  if (suggestions.length) {
    message += ` Did you mean ${suggestions.join(', ')}?`
  }
  return new ParseError({ message, positionStart, positionEnd })
}

/**
 * Suggests similar function names for an unknown function.
 * Combines prefix matches and edit distance into a single ranked list.
 *
 * @param {string} name - The unknown function name (uppercase)
 * @returns {string[]} Up to 4 suggested function names
 */
function suggestFunctions(name) {
  const upper = name.toUpperCase()
  const allNames = Object.keys(FUNCTION_SIGNATURES)

  // Find shared prefix (e.g. JSON_, ST_)
  const underscoreIdx = upper.indexOf('_')
  const prefix = underscoreIdx > 0 ? upper.slice(0, underscoreIdx + 1) : ''

  const maxDist = Math.max(3, Math.floor(upper.length / 2))
  const results = allNames
    .map(n => {
      const dist = editDistance(upper, n)
      const hasPrefix = prefix && n.startsWith(prefix)
      return { name: n, dist, hasPrefix }
    })
    .filter(s => s.dist <= maxDist || s.hasPrefix)
    .sort((a, b) => a.dist - b.dist)
  return results.slice(0, 4).map(s => s.name)
}

/**
 * Levenshtein edit distance between two strings.
 *
 * @param {string} a
 * @param {string} b
 * @returns {number}
 */
function editDistance(a, b) {
  const m = a.length
  const n = b.length
  const dp = Array.from({ length: m + 1 }, (_, i) => i)
  for (let j = 1; j <= n; j++) {
    let prev = dp[0]
    dp[0] = j
    for (let i = 1; i <= m; i++) {
      const tmp = dp[i]
      dp[i] = a[i - 1] === b[j - 1]
        ? prev
        : 1 + Math.min(prev, dp[i], dp[i - 1])
      prev = tmp
    }
  }
  return dp[m]
}
