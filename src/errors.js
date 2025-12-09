// ============================================================================
// PARSE ERRORS - Issues during SQL tokenization and parsing
// ============================================================================

/**
 * General syntax error for unexpected tokens.
 *
 * @param {Object} options
 * @param {string} options.expected - Description of what was expected
 * @param {string} options.received - What was actually found
 * @param {number} options.position - Character position in query
 * @param {string} [options.after] - What token came before (for context)
 * @returns {Error}
 */
export function syntaxError({ expected, received, position, after }) {
  const afterClause = after ? ` after "${after}"` : ''
  return new Error(`Expected ${expected}${afterClause} but found ${received} at position ${position}`)
}

/**
 * Error for unterminated literals (strings, identifiers).
 *
 * @param {'string' | 'identifier'} type - Type of unterminated literal
 * @param {number} position - Starting position
 * @returns {Error}
 */
export function unterminatedError(type, position) {
  const name = type === 'string' ? 'string literal' : 'identifier'
  return new Error(`Unterminated ${name} starting at position ${position}`)
}

/**
 * Error for invalid literals (numbers, intervals, etc).
 *
 * @param {Object} options
 * @param {string} options.type - Type of invalid literal (e.g., 'number', 'interval value', 'interval unit')
 * @param {string} options.value - The invalid value
 * @param {number} options.position - Position in query
 * @param {string} [options.validValues] - List of valid values (for enums like interval units)
 * @returns {Error}
 */
export function invalidLiteralError({ type, value, position, validValues }) {
  const suffix = validValues ? `. Valid values: ${validValues}` : ''
  return new Error(`Invalid ${type} ${value} at position ${position}${suffix}`)
}

/**
 * Error for unexpected characters during tokenization.
 *
 * @param {string} char - The unexpected character
 * @param {number} position - Position in query
 * @param {boolean} [expectsSelect=false] - Whether SELECT was expected (first token)
 * @returns {Error}
 */
export function unexpectedCharError(char, position, expectsSelect = false) {
  if (expectsSelect) {
    return new Error(`Expected SELECT but found "${char}" at position ${position}. Queries must start with SELECT.`)
  }
  return new Error(`Unexpected character "${char}" at position ${position}`)
}

/**
 * Error for unknown/unsupported functions.
 *
 * @param {string} funcName - The unknown function name
 * @param {number} [position] - Position in query (for parse errors)
 * @param {string} [validFunctions] - List of valid functions
 * @returns {Error}
 */
export function unknownFunctionError(funcName, position, validFunctions) {
  const supported = validFunctions ||
    'COUNT, SUM, AVG, MIN, MAX, UPPER, LOWER, CONCAT, LENGTH, SUBSTRING, TRIM, REPLACE, FLOOR, CEIL, ABS, MOD, EXP, LN, LOG10, POWER, SQRT, JSON_OBJECT, JSON_VALUE, JSON_QUERY, JSON_ARRAYAGG'

  if (position !== undefined) {
    return new Error(`Unknown function "${funcName}" at position ${position}. Supported: ${supported}`)
  }
  return new Error(`Unsupported function: ${funcName}. Supported: ${supported}`)
}

/**
 * Error for missing required clause or structure.
 *
 * @param {Object} options
 * @param {string} options.missing - What is missing (e.g., 'WHEN clause', 'FROM clause', 'ON condition')
 * @param {string} options.context - Where it's missing from (e.g., 'CASE expression', 'SELECT statement', 'JOIN')
 * @returns {Error}
 */
export function missingClauseError({ missing, context }) {
  return new Error(`${context} requires ${missing}`)
}

// ============================================================================
// EXECUTION ERRORS - Issues during query execution
// ============================================================================

/**
 * Error for missing table.
 *
 * @param {string} tableName - The missing table name
 * @returns {Error}
 */
export function tableNotFoundError(tableName) {
  return new Error(`Table "${tableName}" not found. Check spelling or add it to the tables parameter.`)
}

/**
 * Error for invalid context (e.g., INTERVAL without date arithmetic).
 *
 * @param {Object} options
 * @param {string} options.item - What was used incorrectly
 * @param {string} options.validContext - Where it can be used
 * @returns {Error}
 */
export function invalidContextError({ item, validContext }) {
  return new Error(`${item} can only be used with ${validContext}`)
}

/**
 * Error for unsupported operation combinations.
 *
 * @param {string} operation - The unsupported operation
 * @param {string} [hint] - How to fix it
 * @returns {Error}
 */
export function unsupportedOperationError(operation, hint) {
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
 * @param {string} funcName - The function name
 * @param {number | string} expected - Expected count (number or range like "2 or 3")
 * @param {number} received - Actual argument count
 * @returns {Error}
 */
export function argCountError(funcName, expected, received) {
  const signature = FUNCTION_SIGNATURES[funcName] ?? ''
  let expectedStr = `${expected} arguments`
  if (expected === 0) expectedStr = 'no arguments'
  if (expected === 1) expectedStr = '1 argument'
  if (typeof expected === 'string' && expected.endsWith(' 1')) {
    expectedStr = `${expected} argument`
  }

  return new Error(`${funcName}(${signature}) function requires ${expectedStr}, got ${received}`)
}

/**
 * Error for invalid argument type or value.
 *
 * @param {Object} options
 * @param {string} options.funcName - The function name
 * @param {string} options.message - Specific error message
 * @param {string} [options.hint] - Recovery hint
 * @returns {Error}
 */
export function argValueError({ funcName, message, hint }) {
  const signature = FUNCTION_SIGNATURES[funcName] ?? ''
  const suffix = hint ? `. ${hint}` : ''
  return new Error(`${funcName}(${signature}): ${message}${suffix}`)
}

/**
 * Error for aggregate function misuse (e.g., SUM(*)).
 *
 * @param {string} funcName - The aggregate function
 * @param {string} issue - What's wrong (e.g., "(*) is not supported")
 * @returns {Error}
 */
export function aggregateError(funcName, issue) {
  return new Error(`${funcName}${issue}. Only COUNT supports *. Use a column name for ${funcName}.`)
}

/**
 * Error for unsupported CAST type.
 *
 * @param {string} toType - The unsupported target type
 * @param {string} [fromType] - The source type (optional)
 * @returns {Error}
 */
export function castError(toType, fromType) {
  const message = fromType
    ? `Cannot CAST ${fromType} to ${toType}`
    : `Unsupported CAST to type ${toType}`

  return new Error(`${message}. Supported types: TEXT, VARCHAR, INTEGER, INT, BIGINT, FLOAT, REAL, DOUBLE, BOOLEAN`)
}
