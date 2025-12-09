// ============================================================================
// EXECUTION ERRORS - Issues during query execution
// ============================================================================

/**
 * Structured execution error with position range and optional row number.
 */
export class ExecutionError extends Error {
  /**
   * @param {Object} options
   * @param {string} options.message - Human-readable error message
   * @param {number} options.positionStart - Start position (0-based character offset)
   * @param {number} options.positionEnd - End position (exclusive, 0-based character offset)
   * @param {number} [options.rowNumber] - 1-based row number where error occurred
   */
  constructor({ message, positionStart, positionEnd, rowNumber }) {
    const rowSuffix = rowNumber != null ? ` (row ${rowNumber})` : ''
    super(message + rowSuffix)
    this.name = 'ExecutionError'
    this.positionStart = positionStart
    this.positionEnd = positionEnd
    this.rowNumber = rowNumber
  }
}

/**
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
 * @param {number} [options.rowNumber] - 1-based row number where error occurred
 * @returns {ExecutionError}
 */
export function invalidContextError({ item, validContext, positionStart, positionEnd, rowNumber }) {
  return new ExecutionError({ message: `${item} can only be used with ${validContext}`, positionStart, positionEnd, rowNumber })
}

/**
 * @param {Object} options
 * @param {string} options.operation - The unsupported operation
 * @param {string} [options.hint] - How to fix it
 * @returns {Error}
 */
export function unsupportedOperationError({ operation, hint }) {
  const suffix = hint ? `. ${hint}` : ''
  return new Error(`${operation}${suffix}`)
}

/**
 * @param {Object} options
 * @param {string} options.columnName - The missing column name
 * @param {string[]} options.availableColumns - List of available column names
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @param {number} [options.rowNumber] - 1-based row number where error occurred
 * @returns {ExecutionError}
 */
export function columnNotFoundError({ columnName, availableColumns, positionStart, positionEnd, rowNumber }) {
  const available = availableColumns.length > 0
    ? `. Available columns: ${availableColumns.join(', ')}`
    : ''
  return new ExecutionError({
    message: `Column "${columnName}" not found${available}`,
    positionStart,
    positionEnd,
    rowNumber,
  })
}
