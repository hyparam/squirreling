import { FUNCTION_SIGNATURES } from './functions.js'

/**
 * Structured execution error with position range and optional row number.
 */
export class ExecutionError extends Error {
  /**
   * @param {Object} options
   * @param {string} options.message - Human-readable error message
   * @param {number} options.positionStart
   * @param {number} options.positionEnd
   * @param {number} [options.rowIndex] - 1-based row number where error occurred
   */
  constructor({ message, positionStart, positionEnd, rowIndex }) {
    const rowSuffix = rowIndex != null ? ` (row ${rowIndex})` : ''
    super(message + rowSuffix)
    this.name = this.constructor.name
    this.positionStart = positionStart
    this.positionEnd = positionEnd
    this.rowIndex = rowIndex
  }
}

/**
 * Error for invalid argument type or value.
 */
export class ArgValueError extends ExecutionError {
  /**
   * @param {Object} options
   * @param {string} options.funcName - The function name
   * @param {string} options.message - Specific error message
   * @param {number} options.positionStart
   * @param {number} options.positionEnd
   * @param {string} [options.hint] - Recovery hint
   * @param {number} [options.rowIndex] - 1-based row number where error occurred
   */
  constructor({ funcName, message, positionStart, positionEnd, hint, rowIndex }) {
    const funcNameUpper = funcName.toUpperCase()
    const signature = FUNCTION_SIGNATURES[funcNameUpper]?.signature ?? ''
    const suffix = hint ? `. ${hint}` : ''
    super({ message: `${funcName}(${signature}): ${message}${suffix}`, positionStart, positionEnd, rowIndex })
  }
}
