// ============================================================================
// PARSE ERRORS - Issues during SQL tokenization and parsing
// ============================================================================

/**
 * Structured parse error with position range.
 */
export class ParseError extends Error {
  /**
   * @param {string} message - Human-readable error message
   * @param {number} positionStart - Start position (0-based character offset)
   * @param {number} positionEnd - End position (exclusive, 0-based character offset)
   */
  constructor(message, positionStart, positionEnd) {
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
  return new ParseError(`Expected ${expected}${afterClause} but found ${received} at position ${positionStart}`, positionStart, positionEnd)
}

/**
 * Error for unterminated literals (strings, identifiers).
 *
 * @param {'string' | 'identifier'} type - Type of unterminated literal
 * @param {number} positionStart - Starting position
 * @param {number} positionEnd - End position
 * @returns {ParseError}
 */
export function unterminatedError(type, positionStart, positionEnd) {
  const name = type === 'string' ? 'string literal' : 'identifier'
  return new ParseError(`Unterminated ${name} starting at position ${positionStart}`, positionStart, positionEnd)
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
  return new ParseError(`Invalid ${type} ${value} at position ${positionStart}${suffix}`, positionStart, positionEnd)
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
    return new ParseError(`Expected SELECT but found "${char}" at position ${positionStart}. Queries must start with SELECT.`, positionStart, positionEnd)
  }
  return new ParseError(`Unexpected character "${char}" at position ${positionStart}`, positionStart, positionEnd)
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

  return new ParseError(
    `Unknown function "${funcName}" at position ${positionStart}. Supported: ${supported}`,
    positionStart,
    positionEnd
  )
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
  return new ParseError(`${context} requires ${missing}`, positionStart ?? 0, positionEnd ?? 0)
}
