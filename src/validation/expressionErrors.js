import { ExecutionError } from './executionErrors.js'
import { FUNCTION_SIGNATURES } from './functions.js'

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
  const signature = FUNCTION_SIGNATURES[funcName]?.signature ?? ''
  const suffix = hint ? `. ${hint}` : ''
  return new ExecutionError({ message: `${funcName}(${signature}): ${message}${suffix}`, positionStart, positionEnd, rowIndex })
}

/**
 * Error for aggregate function misuse.
 *
 * @param {Object} options
 * @param {string} options.funcName - The aggregate function
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @returns {ExecutionError}
 */
export function aggregateError({ funcName, positionStart, positionEnd }) {
  return new ExecutionError({
    message: `Aggregate function ${funcName} is not available in this context`,
    positionStart,
    positionEnd,
  })
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
