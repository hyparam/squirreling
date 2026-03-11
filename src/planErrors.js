import { ExecutionError } from './executionErrors.js'

/**
 * @param {Object} options
 * @param {string} options.table - The missing table name
 * @param {Record<string, any>} [options.tables] - Available tables object
 * @param {number} [options.positionStart] - Start position in query
 * @param {number} [options.positionEnd] - End position in query
 * @returns {ExecutionError}
 */
export function tableNotFoundError({ table, tables, positionStart, positionEnd }) {
  const names = tables ? Object.keys(tables) : []
  const available = names.length
    ? `. Available tables: ${names.join(', ')}`
    : ''
  return new ExecutionError({
    message: `Table "${table}" not found${available}`,
    positionStart,
    positionEnd,
  })
}

/**
 * @param {Object} options
 * @param {string} options.columnName - The missing column name
 * @param {string[]} options.availableColumns - List of available column names
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @param {number} [options.rowIndex] - 1-based row number where error occurred
 * @returns {ExecutionError}
 */
export function columnNotFoundError({ columnName, availableColumns, positionStart, positionEnd, rowIndex }) {
  const available = availableColumns.length > 0
    ? `. Available columns: ${availableColumns.join(', ')}`
    : ''
  return new ExecutionError({
    message: `Column "${columnName}" not found${available}`,
    positionStart,
    positionEnd,
    rowIndex,
  })
}
