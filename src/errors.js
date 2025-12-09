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

// ============================================================================
// EXECUTION ERRORS - Issues during query execution
// ============================================================================

/**
 * Structured execution error with position range.
 */
export class ExecutionError extends Error {
  /**
   * @param {string} message - Human-readable error message
   * @param {number} positionStart - Start position (0-based character offset)
   * @param {number} positionEnd - End position (exclusive, 0-based character offset)
   */
  constructor(message, positionStart, positionEnd) {
    super(message)
    this.name = 'ExecutionError'
    this.positionStart = positionStart
    this.positionEnd = positionEnd
  }
}

/**
 * Error for missing table.
 *
 * @param {Object} options
 * @param {string} options.tableName - The missing table name
 * @returns {Error}
 */
export function tableNotFoundError({ tableName }) {
  return new Error(`Table "${tableName}" not found. Check spelling or add it to the tables parameter.`)
}

/**
 * Error for invalid context (e.g., INTERVAL without date arithmetic).
 *
 * @param {Object} options
 * @param {string} options.item - What was used incorrectly
 * @param {string} options.validContext - Where it can be used
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @returns {ExecutionError}
 */
export function invalidContextError({ item, validContext, positionStart, positionEnd }) {
  return new ExecutionError(`${item} can only be used with ${validContext}`, positionStart, positionEnd)
}

/**
 * Error for unsupported operation combinations.
 *
 * @param {Object} options
 * @param {string} options.operation - The unsupported operation
 * @param {string} [options.hint] - How to fix it
 * @returns {Error}
 */
export function unsupportedOperationError({ operation, hint }) {
  const suffix = hint ? `. ${hint}` : ''
  return new Error(`${operation}${suffix}`)
}

// ============================================================================
// VALIDATION ERRORS - Function argument and type validation
// ============================================================================

/**
 * Function signatures for helpful error messages.
 * Maps function name to its parameter signature.
 * @type {Record<string, string>}
 */
const FUNCTION_SIGNATURES = {
  // String functions
  UPPER: 'string',
  LOWER: 'string',
  LENGTH: 'string',
  TRIM: 'string',
  REPLACE: 'string, search, replacement',
  SUBSTRING: 'string, start[, length]',
  SUBSTR: 'string, start[, length]',
  CONCAT: 'value1, value2[, ...]',

  // Date/time functions
  RANDOM: '',
  RAND: '',
  CURRENT_DATE: '',
  CURRENT_TIME: '',
  CURRENT_TIMESTAMP: '',

  // Math functions
  FLOOR: 'number',
  CEIL: 'number',
  CEILING: 'number',
  ABS: 'number',
  MOD: 'dividend, divisor',
  EXP: 'number',
  LN: 'number',
  LOG10: 'number',
  POWER: 'base, exponent',
  SQRT: 'number',

  // JSON functions
  JSON_VALUE: 'expression, path',
  JSON_QUERY: 'expression, path',
  JSON_OBJECT: 'key1, value1[, ...]',
  JSON_ARRAYAGG: 'expression',

  // Aggregate functions
  COUNT: 'expression',
  SUM: 'expression',
  AVG: 'expression',
  MIN: 'expression',
  MAX: 'expression',
}

/**
 * Error for wrong number of function arguments.
 *
 * @param {Object} options
 * @param {string} options.funcName - The function name
 * @param {number | string} options.expected - Expected count (number or range like "2 or 3")
 * @param {number} options.received - Actual argument count
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @returns {ExecutionError}
 */
export function argCountError({ funcName, expected, received, positionStart, positionEnd }) {
  const signature = FUNCTION_SIGNATURES[funcName] ?? ''
  let expectedStr = `${expected} arguments`
  if (expected === 0) expectedStr = 'no arguments'
  if (expected === 1) expectedStr = '1 argument'
  if (typeof expected === 'string' && expected.endsWith(' 1')) {
    expectedStr = `${expected} argument`
  }

  return new ExecutionError(`${funcName}(${signature}) function requires ${expectedStr}, got ${received}`, positionStart, positionEnd)
}

/**
 * Error for invalid argument type or value.
 *
 * @param {Object} options
 * @param {string} options.funcName - The function name
 * @param {string} options.message - Specific error message
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @param {string} [options.hint] - Recovery hint
 * @returns {ExecutionError}
 */
export function argValueError({ funcName, message, positionStart, positionEnd, hint }) {
  const signature = FUNCTION_SIGNATURES[funcName] ?? ''
  const suffix = hint ? `. ${hint}` : ''
  return new ExecutionError(`${funcName}(${signature}): ${message}${suffix}`, positionStart, positionEnd)
}

/**
 * Error for aggregate function misuse (e.g., SUM(*)).
 *
 * @param {Object} options
 * @param {string} options.funcName - The aggregate function
 * @param {string} options.issue - What's wrong (e.g., "(*) is not supported")
 * @returns {Error}
 */
export function aggregateError({ funcName, issue }) {
  return new Error(`${funcName}${issue}. Only COUNT supports *. Use a column name for ${funcName}.`)
}

/**
 * Error for unsupported CAST type.
 *
 * @param {Object} options
 * @param {string} options.toType - The unsupported target type
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @param {string} [options.fromType] - The source type (optional)
 * @returns {ExecutionError}
 */
export function castError({ toType, positionStart, positionEnd, fromType }) {
  const message = fromType
    ? `Cannot CAST ${fromType} to ${toType}`
    : `Unsupported CAST to type ${toType}`

  return new ExecutionError(`${message}. Supported types: TEXT, VARCHAR, INTEGER, INT, BIGINT, FLOAT, REAL, DOUBLE, BOOLEAN`, positionStart, positionEnd)
}
