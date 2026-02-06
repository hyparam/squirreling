import { ExecutionError } from './executionErrors.js'

// ============================================================================
// VALIDATION ERRORS - Function argument and type validation
// ============================================================================

/**
 * Function signatures for helpful error messages.
 * Maps function name to its parameter signature.
 * @type {Record<string, string>}
 */
export const FUNCTION_SIGNATURES = {
  // String functions
  UPPER: 'string',
  LOWER: 'string',
  LENGTH: 'string',
  TRIM: 'string',
  REPLACE: 'string, search, replacement',
  SUBSTRING: 'string, start[, length]',
  SUBSTR: 'string, start[, length]',
  CONCAT: 'value1, value2[, ...]',
  LEFT: 'string, length',
  RIGHT: 'string, length',
  INSTR: 'string, substring',

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
  ROUND: 'number[, decimals]',
  ABS: 'number',
  MOD: 'dividend, divisor',
  EXP: 'number',
  LN: 'number',
  LOG10: 'number',
  POWER: 'base, exponent',
  SQRT: 'number',
  SIN: 'radians',
  COS: 'radians',
  TAN: 'radians',
  COT: 'radians',
  ASIN: 'number',
  ACOS: 'number',
  ATAN: 'number',
  ATAN2: 'y, x',
  DEGREES: 'radians',
  RADIANS: 'degrees',
  PI: '',

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
  STDDEV_SAMP: 'expression',
  STDDEV_POP: 'expression',
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
 * @param {number} [options.rowIndex] - 1-based row number where error occurred
 * @returns {ExecutionError}
 */
export function argValueError({ funcName, message, positionStart, positionEnd, hint, rowIndex }) {
  const signature = FUNCTION_SIGNATURES[funcName] ?? ''
  const suffix = hint ? `. ${hint}` : ''
  return new ExecutionError({ message: `${funcName}(${signature}): ${message}${suffix}`, positionStart, positionEnd, rowIndex })
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
 * @param {number} [options.rowIndex] - 1-based row number where error occurred
 * @returns {ExecutionError}
 */
export function castError({ toType, positionStart, positionEnd, fromType, rowIndex }) {
  const message = fromType
    ? `Cannot CAST ${fromType} to ${toType}`
    : `Unsupported CAST to type ${toType}`

  return new ExecutionError({ message: `${message}. Supported types: TEXT, VARCHAR, INTEGER, INT, BIGINT, FLOAT, REAL, DOUBLE, BOOLEAN`, positionStart, positionEnd, rowIndex })
}
