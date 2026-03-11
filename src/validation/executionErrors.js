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
    this.name = 'ExecutionError'
    this.positionStart = positionStart
    this.positionEnd = positionEnd
    this.rowIndex = rowIndex
  }
}

/**
 * Error for invalid context (e.g., INTERVAL without date arithmetic).
 *
 * @param {Object} options
 * @param {string} options.item - What was used incorrectly
 * @param {string} options.validContext - Where it can be used
 * @param {number} options.positionStart
 * @param {number} options.positionEnd
 * @param {number} options.rowIndex - 1-based row number where error occurred
 * @returns {ExecutionError}
 */
export function invalidContextError({ item, validContext, positionStart, positionEnd, rowIndex }) {
  return new ExecutionError({ message: `${item} can only be used with ${validContext}`, positionStart, positionEnd, rowIndex })
}
