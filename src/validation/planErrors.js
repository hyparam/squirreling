import { ExecutionError } from './executionErrors.js'

/**
 * Error for missing table references.
 */
export class TableNotFoundError extends ExecutionError {
  /**
   * @param {Object} options
   * @param {string} options.table - The missing table name
   * @param {Record<string, any>} options.tables - Available tables object
   * @param {number} [options.positionStart]
   * @param {number} [options.positionEnd]
   */
  constructor({ table, tables, positionStart, positionEnd }) {
    const names = tables ? Object.keys(tables) : []
    const available = names.length
      ? `. Available tables: ${names.join(', ')}`
      : ''
    super({
      message: `Table "${table}" not found${available}`,
      positionStart,
      positionEnd,
    })
  }
}

/**
 * Error for missing column references.
 */
export class ColumnNotFoundError extends ExecutionError {
  /**
   * @param {Object} options
   * @param {string} options.missingColumn - The missing column name
   * @param {string[]} options.availableColumns - List of available column names
   * @param {number} options.positionStart
   * @param {number} options.positionEnd
   * @param {number} [options.rowIndex] - 1-based row number where error occurred
   */
  constructor({ missingColumn, availableColumns, positionStart, positionEnd, rowIndex }) {
    const available = availableColumns.length > 0
      ? `. Available columns: ${availableColumns.join(', ')}`
      : ''
    super({
      message: `Column "${missingColumn}" not found${available}`,
      positionStart,
      positionEnd,
      rowIndex,
    })
  }
}
