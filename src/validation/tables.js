import { ExecutionError } from './executionErrors.js'

/**
 * @import { AsyncDataSource, ExprNode, ScanOptions } from '../types.js'
 */

/**
 * @param {Object} options
 * @param {string} options.table - The name of the table to validate
 * @param {string} [options.qualified] - The qualified identifier used in the query (for error messages)
 * @param {Record<string, AsyncDataSource>} options.tables - Object mapping table names to data sources
 * @param {number} [options.positionStart] - Optional start position for error reporting
 * @param {number} [options.positionEnd] - Optional end position for error reporting
 * @returns {AsyncDataSource}
 */
export function validateTable({ table, qualified, tables, positionStart, positionEnd } ) {
  const resolved = tables[table]
  if (!resolved) {
    throw new TableNotFoundError({ table, qualified, tables, positionStart, positionEnd })
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
 * Throws if the expression references any column identifier. Used for
 * expressions that have no row scope (e.g. table function arguments in FROM).
 *
 * @param {ExprNode} expr
 * @param {string} context - context for the error message (e.g. function name)
 */
export function validateNoIdentifiers(expr, context) {
  if (!expr) return
  if (expr.type === 'identifier') {
    const name = expr.prefix ? `${expr.prefix}.${expr.name}` : expr.name
    throw new ExecutionError({
      message: `${context} argument cannot reference column "${name}" — ${context} arguments must be constant (lateral/correlated ${context} is not supported)`,
      positionStart: expr.positionStart,
      positionEnd: expr.positionEnd,
    })
  }
  if (expr.type === 'binary') {
    validateNoIdentifiers(expr.left, context)
    validateNoIdentifiers(expr.right, context)
  } else if (expr.type === 'unary') {
    validateNoIdentifiers(expr.argument, context)
  } else if (expr.type === 'function') {
    for (const arg of expr.args) {
      validateNoIdentifiers(arg, context)
    }
  } else if (expr.type === 'cast') {
    validateNoIdentifiers(expr.expr, context)
  } else if (expr.type === 'in valuelist') {
    validateNoIdentifiers(expr.expr, context)
    for (const val of expr.values) {
      validateNoIdentifiers(val, context)
    }
  } else if (expr.type === 'in') {
    // LHS is in our scope; subquery is self-contained and planned separately.
    validateNoIdentifiers(expr.expr, context)
  } else if (expr.type === 'case') {
    validateNoIdentifiers(expr.caseExpr, context)
    for (const w of expr.whenClauses) {
      validateNoIdentifiers(w.condition, context)
      validateNoIdentifiers(w.result, context)
    }
    validateNoIdentifiers(expr.elseResult, context)
  }
  // subquery / exists / not exists are self-contained — their identifiers
  // resolve to their own FROM and are validated when the subquery is planned.
}

/**
 * Validates that qualified identifiers reference known table aliases.
 *
 * @param {ExprNode} expr
 * @param {Record<string, any>} tables
 */
export function validateTableRefs(expr, tables) {
  if (!expr) return
  if (expr.type === 'identifier' && expr.prefix && !(expr.prefix in tables)) {
    throw new TableNotFoundError({
      table: expr.prefix,
      qualified: expr.prefix + '.' + expr.name,
      tables,
      positionStart: expr.positionStart,
      positionEnd: expr.positionStart + expr.prefix.length,
    })
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

/**
 * Error for missing table references.
 */
export class TableNotFoundError extends ExecutionError {
  /**
   * @param {Object} options
   * @param {string} options.table - The missing table name
   * @param {string} [options.qualified] - The identifier used in the query
   * @param {Record<string, any>} options.tables - Available tables object
   * @param {number} [options.positionStart]
   * @param {number} [options.positionEnd]
   */
  constructor({ table, qualified, tables, positionStart, positionEnd }) {
    const usage = qualified ? ` in "${qualified}"` : ''
    const available = tables
      ? `. Available tables: ${Object.keys(tables).join(', ')}`
      : ''
    super({
      message: `Table "${table}" not found${usage}${available}`,
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
