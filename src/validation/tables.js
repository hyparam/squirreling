import { ColumnNotFoundError, TableNotFoundError } from './planErrors.js'

/**
 * @import { AsyncDataSource, ExprNode, ScanOptions } from '../types.js'
 */

/**
 * @param {Object} options
 * @param {string} options.table - The name of the table to validate
 * @param {Record<string, AsyncDataSource>} options.tables - Object mapping table names to data sources
 * @param {number} [options.positionStart] - Optional start position for error reporting
 * @param {number} [options.positionEnd] - Optional end position for error reporting
 * @returns {AsyncDataSource}
 */
export function validateTable({ table, tables, positionStart, positionEnd } ) {
  const resolved = tables[table]
  if (!resolved) {
    throw new TableNotFoundError({ table, tables, positionStart, positionEnd })
  }
  return resolved
}

/**
 * Validates that a table exists and requested columns are available.
 *
 * @param {object} options
 * @param {string} [options.table]
 * @param {ScanOptions} options.hints
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @param {number} [options.positionStart]
 * @param {number} [options.positionEnd]
 */
export function validateScan({ table, hints, tables, positionStart, positionEnd }) {
  if (!tables) return
  const resolved = validateTable({ table, tables, positionStart, positionEnd })
  const missingColumn = hints.columns?.find(col => !resolved.columns.includes(col))
  if (missingColumn) {
    throw new ColumnNotFoundError({
      missingColumn,
      availableColumns: resolved.columns,
      positionStart,
      positionEnd,
    })
  }
}

/**
 * Validates that qualified identifiers reference known table aliases.
 *
 * @param {ExprNode} expr
 * @param {Record<string, any>} tables
 */
export function validateTableRefs(expr, tables) {
  if (!expr) return
  if (expr.type === 'identifier') {
    if (expr.prefix) {
      if (!(expr.prefix in tables)) {
        throw new TableNotFoundError({ table: expr.prefix, tables, positionStart: expr.positionStart, positionEnd: expr.positionStart + expr.prefix.length })
      }
    }
    return
  }
  if (expr.type === 'binary') {
    validateTableRefs(expr.left, tables)
    validateTableRefs(expr.right, tables)
  } else if (expr.type === 'unary') {
    validateTableRefs(expr.argument, tables)
  } else if (expr.type === 'function') {
    for (const arg of expr.args) {
      validateTableRefs(arg, tables)
    }
  } else if (expr.type === 'cast') {
    validateTableRefs(expr.expr, tables)
  } else if (expr.type === 'in valuelist') {
    validateTableRefs(expr.expr, tables)
    for (const val of expr.values) {
      validateTableRefs(val, tables)
    }
  } else if (expr.type === 'case') {
    validateTableRefs(expr.caseExpr, tables)
    for (const w of expr.whenClauses) {
      validateTableRefs(w.condition, tables)
      validateTableRefs(w.result, tables)
    }
    validateTableRefs(expr.elseResult, tables)
  }
}
