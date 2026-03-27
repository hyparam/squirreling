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
 * @param {string} options.received - What was actually found
 * @param {number} options.positionStart
 * @param {number} options.positionEnd
 * @param {string} [options.after] - What token came before (for context)
 * @returns {ParseError}
 */
export function syntaxError({ expected, received, positionStart, positionEnd, after }) {
  const afterClause = after ? ` after "${after}"` : ''
  return new ParseError({ message: `Expected ${expected}${afterClause} but found ${received} at position ${positionStart}`, positionStart, positionEnd })
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
  // TODO: suggest similar function names based on edit distance
  return new ParseError({
    message: `Unknown function "${funcName}" at position ${positionStart}.`,
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
 * @param {number} options.positionStart
 * @param {number} options.positionEnd
 * @returns {ParseError}
 */
export function missingClauseError({ missing, context, positionStart, positionEnd }) {
  return new ParseError({ message: `${context} requires ${missing}`, positionStart, positionEnd })
}

/**
 * Error for duplicate CTE names in WITH clause.
 *
 * @param {Object} options
 * @param {string} options.cteName - The duplicate CTE name
 * @param {number} options.positionStart
 * @param {number} options.positionEnd
 * @returns {ParseError}
 */
export function duplicateCTEError({ cteName, positionStart, positionEnd }) {
  return new ParseError({
    message: `CTE "${cteName}" is defined more than once at position ${positionStart}`,
    positionStart,
    positionEnd,
  })
}
