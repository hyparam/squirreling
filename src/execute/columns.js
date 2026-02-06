/**
 * @import { ExprNode, SelectStatement, SelectColumn } from '../types.js'
 */

/**
 * Extracts column names needed from a SELECT statement.
 *
 * @param {SelectStatement} select
 * @returns {string[] | undefined} array of column names, or undefined if all columns needed
 */
export function extractColumns(select) {
  // If any column is SELECT *, we need all columns
  if (select.columns.some(col => col.kind === 'star')) {
    return undefined
  }

  /** @type {Set<string>} */
  const columns = new Set()

  // Columns from SELECT list
  for (const col of select.columns) {
    collectColumnsFromSelectColumn(col, columns)
  }

  // Columns from WHERE
  collectColumnsFromExpr(select.where, columns)

  // Columns from ORDER BY
  for (const item of select.orderBy) {
    collectColumnsFromExpr(item.expr, columns)
  }

  // Columns from GROUP BY
  for (const expr of select.groupBy) {
    collectColumnsFromExpr(expr, columns)
  }

  // Columns from HAVING
  collectColumnsFromExpr(select.having, columns)

  return [...columns]
}

/**
 * Collects column names from a SELECT column
 *
 * @param {SelectColumn} col
 * @param {Set<string>} columns
 */
function collectColumnsFromSelectColumn(col, columns) {
  if (col.kind === 'derived') {
    collectColumnsFromExpr(col.expr, columns)
  }
  // 'star' columns handled separately (returns undefined for all columns)
}

/**
 * Recursively collects column names (identifiers) from an expression
 *
 * @param {ExprNode | undefined} expr
 * @param {Set<string>} columns
 */
function collectColumnsFromExpr(expr, columns) {
  if (!expr) return
  if (expr.type === 'identifier' && expr.name !== '*') {
    columns.add(expr.name)
  } else if (expr.type === 'literal') {
    // No columns
  } else if (expr.type === 'binary') {
    collectColumnsFromExpr(expr.left, columns)
    collectColumnsFromExpr(expr.right, columns)
  } else if (expr.type === 'unary') {
    collectColumnsFromExpr(expr.argument, columns)
  } else if (expr.type === 'function') {
    for (const arg of expr.args) {
      collectColumnsFromExpr(arg, columns)
    }
  } else if (expr.type === 'cast') {
    collectColumnsFromExpr(expr.expr, columns)
  } else if (expr.type === 'in valuelist') {
    collectColumnsFromExpr(expr.expr, columns)
    for (const val of expr.values) {
      collectColumnsFromExpr(val, columns)
    }
  } else if (expr.type === 'in') {
    collectColumnsFromExpr(expr.expr, columns)
    // Subquery columns are from a different scope, don't collect
  } else if (expr.type === 'exists' || expr.type === 'not exists') {
    // Subquery columns are from a different scope, don't collect
  } else if (expr.type === 'case') {
    if (expr.caseExpr) {
      collectColumnsFromExpr(expr.caseExpr, columns)
    }
    for (const when of expr.whenClauses) {
      collectColumnsFromExpr(when.condition, columns)
      collectColumnsFromExpr(when.result, columns)
    }
    if (expr.elseResult) {
      collectColumnsFromExpr(expr.elseResult, columns)
    }
  }
}
