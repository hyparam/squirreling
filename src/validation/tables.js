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
 * When `outerScope` is provided, identifiers that resolve to an outer query's
 * alias (correlated reference) are allowed — they will be evaluated against
 * the outer row at runtime.
 *
 * @param {ExprNode} expr
 * @param {string} context - context for the error message (e.g. function name)
 * @param {string[]} [outerScope] - aliases of outer queries
 */
export function validateNoIdentifiers(expr, context, outerScope) {
  if (!expr) return
  if (expr.type === 'identifier') {
    if (outerScope?.length) {
      // Correlated reference: prefix matches an outer alias, or unqualified
      // (resolved against the outer row at runtime).
      if (!expr.prefix || outerScope.includes(expr.prefix)) return
    }
    const name = expr.prefix ? `${expr.prefix}.${expr.name}` : expr.name
    throw new ExecutionError({
      message: `${context} argument cannot reference column "${name}" — use JOIN ${context}(...) to reference columns from another table`,
      positionStart: expr.positionStart,
      positionEnd: expr.positionEnd,
    })
  }
  if (expr.type === 'binary') {
    validateNoIdentifiers(expr.left, context, outerScope)
    validateNoIdentifiers(expr.right, context, outerScope)
  } else if (expr.type === 'unary') {
    validateNoIdentifiers(expr.argument, context, outerScope)
  } else if (expr.type === 'function') {
    for (const arg of expr.args) {
      validateNoIdentifiers(arg, context, outerScope)
    }
  } else if (expr.type === 'cast') {
    validateNoIdentifiers(expr.expr, context, outerScope)
  } else if (expr.type === 'in valuelist') {
    validateNoIdentifiers(expr.expr, context, outerScope)
    for (const val of expr.values) {
      validateNoIdentifiers(val, context, outerScope)
    }
  } else if (expr.type === 'in') {
    // LHS is in our scope; subquery is self-contained and planned separately.
    validateNoIdentifiers(expr.expr, context, outerScope)
  } else if (expr.type === 'case') {
    validateNoIdentifiers(expr.caseExpr, context, outerScope)
    for (const w of expr.whenClauses) {
      validateNoIdentifiers(w.condition, context, outerScope)
      validateNoIdentifiers(w.result, context, outerScope)
    }
    validateNoIdentifiers(expr.elseResult, context, outerScope)
  }
  // subquery / exists / not exists are self-contained — their identifiers
  // resolve to their own FROM and are validated when the subquery is planned.
}

/**
 * Validates that qualified identifiers reference known table aliases.
 * A `prefix` may also be a bare column name in scope, in which case the
 * identifier is struct-field access (e.g. `item.name` reads field `name`
 * from a struct-valued column `item`).
 *
 * @param {ExprNode} expr
 * @param {Record<string, any>} tables
 * @param {Set<string>} [scopeColumns] - bare column names in scope, used to
 *   recognize struct-field dot access on a column rather than a table
 */
export function validateTableRefs(expr, tables, scopeColumns) {
  if (!expr) return
  if (expr.type === 'identifier' && expr.prefix && !(expr.prefix in tables) && !scopeColumns?.has(expr.prefix)) {
    throw new TableNotFoundError({
      table: expr.prefix,
      qualified: expr.prefix + '.' + expr.name,
      tables,
      positionStart: expr.positionStart,
      positionEnd: expr.positionStart + expr.prefix.length,
    })
  }
  if (expr.type === 'binary') {
    validateTableRefs(expr.left, tables, scopeColumns)
    validateTableRefs(expr.right, tables, scopeColumns)
  } else if (expr.type === 'unary') {
    validateTableRefs(expr.argument, tables, scopeColumns)
  } else if (expr.type === 'function') {
    for (const arg of expr.args) {
      validateTableRefs(arg, tables, scopeColumns)
    }
  } else if (expr.type === 'window') {
    for (const arg of expr.args) validateTableRefs(arg, tables, scopeColumns)
    for (const p of expr.partitionBy) validateTableRefs(p, tables, scopeColumns)
    for (const o of expr.orderBy) validateTableRefs(o.expr, tables, scopeColumns)
  } else if (expr.type === 'cast') {
    validateTableRefs(expr.expr, tables, scopeColumns)
  } else if (expr.type === 'in valuelist') {
    validateTableRefs(expr.expr, tables, scopeColumns)
    for (const val of expr.values) {
      validateTableRefs(val, tables, scopeColumns)
    }
  } else if (expr.type === 'case') {
    validateTableRefs(expr.caseExpr, tables, scopeColumns)
    for (const w of expr.whenClauses) {
      validateTableRefs(w.condition, tables, scopeColumns)
      validateTableRefs(w.result, tables, scopeColumns)
    }
    validateTableRefs(expr.elseResult, tables, scopeColumns)
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
