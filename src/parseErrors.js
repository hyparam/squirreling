// ============================================================================
// PARSE ERRORS - Issues during SQL tokenization and parsing
// ============================================================================

import { FUNCTION_SIGNATURES } from './validationErrors.js'

/**
 * Structured parse error with position range.
 */
export class ParseError extends Error {
  /**
   * @param {Object} options
   * @param {string} options.message - Human-readable error message
   * @param {number} options.positionStart - Start position (0-based character offset)
   * @param {number} options.positionEnd - End position (exclusive, 0-based character offset)
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
 * @param {string} options.received - What was actually found
 * @param {number} options.positionStart - Start character position in query
 * @param {number} options.positionEnd - End character position in query
 * @param {string} [options.after] - What token came before (for context)
 * @returns {ParseError}
 */
export function syntaxError({ expected, received, positionStart, positionEnd, after }) {
  const afterClause = after ? ` after "${after}"` : ''
  return new ParseError({ message: `Expected ${expected}${afterClause} but found ${received} at position ${positionStart}`, positionStart, positionEnd })
}

/**
 * Error for unterminated literals (strings, identifiers).
 *
 * @param {Object} options
 * @param {'string' | 'identifier'} options.type - Type of unterminated literal
 * @param {number} options.positionStart - Starting position
 * @param {number} options.positionEnd - End position
 * @returns {ParseError}
 */
export function unterminatedError({ type, positionStart, positionEnd }) {
  const name = type === 'string' ? 'string literal' : 'identifier'
  return new ParseError({ message: `Unterminated ${name} starting at position ${positionStart}`, positionStart, positionEnd })
}

/**
 * Error for invalid literals (numbers, intervals, etc).
 *
 * @param {Object} options
 * @param {string} options.type - Type of invalid literal (e.g., 'number', 'interval value', 'interval unit')
 * @param {string} options.value - The invalid value
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @param {string} [options.validValues] - List of valid values (for enums like interval units)
 * @returns {ParseError}
 */
export function invalidLiteralError({ type, value, positionStart, positionEnd, validValues }) {
  const suffix = validValues ? `. Valid values: ${validValues}` : ''
  return new ParseError({ message: `Invalid ${type} ${value} at position ${positionStart}${suffix}`, positionStart, positionEnd })
}

/**
 * Error for unexpected characters during tokenization.
 *
 * @param {Object} options
 * @param {string} options.char - The unexpected character
 * @param {number} options.positionStart - Position in query
 * @param {boolean} [options.expectsSelect=false] - Whether SELECT was expected (first token)
 * @returns {ParseError}
 */
export function unexpectedCharError({ char, positionStart, expectsSelect = false }) {
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
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @param {string} [options.validFunctions] - List of valid functions
 * @returns {ParseError}
 */
export function unknownFunctionError({ funcName, positionStart, positionEnd, validFunctions }) {
  const supported = validFunctions ||
    'COUNT, SUM, AVG, MIN, MAX, UPPER, LOWER, CONCAT, LENGTH, SUBSTRING, TRIM, REPLACE, FLOOR, CEIL, ABS, MOD, EXP, LN, LOG10, POWER, SQRT, JSON_OBJECT, JSON_VALUE, JSON_QUERY, JSON_ARRAYAGG'

  return new ParseError({
    message: `Unknown function "${funcName}" at position ${positionStart}. Supported: ${supported}`,
    positionStart,
    positionEnd,
  })
}

/**
 * Error for wrong number of function arguments at parse time.
 *
 * @param {Object} options
 * @param {string} options.funcName - The function name
 * @param {number | string} options.expected - Expected count (number or range like "2 to 3")
 * @param {number} options.received - Actual argument count
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @returns {ParseError}
 */
export function argCountParseError({ funcName, expected, received, positionStart, positionEnd }) {
  const signature = FUNCTION_SIGNATURES[funcName] ?? ''
  let expectedStr = `${expected} arguments`
  if (expected === 0) expectedStr = 'no arguments'
  if (expected === 1) expectedStr = '1 argument'
  if (typeof expected === 'string' && expected.endsWith(' 1')) {
    expectedStr = `${expected} argument`
  }

  return new ParseError({
    message: `${funcName}(${signature}) function requires ${expectedStr}, got ${received}`,
    positionStart,
    positionEnd,
  })
}

/**
 * Error for missing required clause or structure.
 *
 * @param {Object} options
 * @param {string} options.missing - What is missing (e.g., 'WHEN clause', 'FROM clause', 'ON condition')
 * @param {string} options.context - Where it's missing from (e.g., 'CASE expression', 'SELECT statement', 'JOIN')
 * @param {number} [options.positionStart] - Start position in query
 * @param {number} [options.positionEnd] - End position in query
 * @returns {ParseError}
 */
export function missingClauseError({ missing, context, positionStart, positionEnd }) {
  return new ParseError({ message: `${context} requires ${missing}`, positionStart: positionStart ?? 0, positionEnd: positionEnd ?? 0 })
}

/**
 * Error for duplicate CTE names in WITH clause.
 *
 * @param {Object} options
 * @param {string} options.cteName - The duplicate CTE name
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @returns {ParseError}
 */
export function duplicateCTEError({ cteName, positionStart, positionEnd }) {
  return new ParseError({
    message: `CTE "${cteName}" is defined more than once at position ${positionStart}`,
    positionStart,
    positionEnd,
  })
}
