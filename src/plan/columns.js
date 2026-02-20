/**
 * @import { ExprNode, SelectStatement } from '../types.js'
 */

/**
 * Extracts per-table column names needed from a SELECT statement with joins.
 * Returns a Map from table alias to column names, or undefined if all columns needed.
 *
 * @param {SelectStatement} select
 * @returns {Map<string, string[] | undefined>}
 */
export function extractColumns(select) {
  /** @type {Map<string, string[] | undefined>} */
  const result = new Map()

  // Build alias list from FROM + JOINs
  const fromAlias = select.from.kind === 'table'
    ? select.from.alias ?? select.from.table
    : select.from.alias
  const aliases = [fromAlias]
  for (const join of select.joins) {
    aliases.push(join.alias ?? join.table)
  }

  // If any unqualified SELECT * exists, all tables need all columns
  if (select.columns.some(col => col.kind === 'star' && !col.table)) {
    /** @type {Map<string, string[] | undefined>} */
    const result = new Map()
    for (const alias of aliases) {
      result.set(alias, undefined)
    }
    return result
  }

  // Track per-table columns needed; undefined means all columns (table.*)
  /** @type {Map<string, Set<string> | undefined>} */
  const perTable = new Map(aliases.map(alias => [alias, new Set()]))

  // Collect all identifiers from all clauses
  /** @type {Set<string>} */
  const identifiers = new Set()
  for (const col of select.columns) {
    if (col.kind === 'star' && col.table) {
      // SELECT table.* means all columns needed
      perTable.set(col.table, undefined)
    } else if (col.kind === 'derived') {
      collectColumnsFromExpr(col.expr, identifiers)
    }
  }
  collectColumnsFromExpr(select.where, identifiers)
  for (const item of select.orderBy) {
    collectColumnsFromExpr(item.expr, identifiers)
  }
  for (const expr of select.groupBy) {
    collectColumnsFromExpr(expr, identifiers)
  }
  collectColumnsFromExpr(select.having, identifiers)
  for (const join of select.joins) {
    collectColumnsFromExpr(join.on, identifiers)
  }

  // Partition identifiers by table prefix
  for (const name of identifiers) {
    const dotIndex = name.indexOf('.')
    if (dotIndex >= 0) {
      // Qualified: add to matching table only
      const tablePrefix = name.substring(0, dotIndex)
      const columnName = name.substring(dotIndex + 1)
      const set = perTable.get(tablePrefix)
      if (set) set.add(columnName)
    } else {
      // Unqualified: add to all tables (ambiguous)
      for (const [, set] of perTable) {
        if (set) set.add(name)
      }
    }
  }

  // Build result map: convert Sets to arrays, undefined for all-columns tables
  for (const alias of aliases) {
    const set = perTable.get(alias)
    result.set(alias, set ? [...set] : undefined)
  }
  return result
}

/**
 * Recursively collects column names (identifiers) from an expression
 *
 * @param {ExprNode} expr
 * @param {Set<string>} columns
 */
function collectColumnsFromExpr(expr, columns) {
  if (!expr) return
  if (expr.type === 'identifier') {
    columns.add(expr.name)
  } else if (expr.type === 'binary') {
    collectColumnsFromExpr(expr.left, columns)
    collectColumnsFromExpr(expr.right, columns)
  } else if (expr.type === 'unary') {
    collectColumnsFromExpr(expr.argument, columns)
  } else if (expr.type === 'function') {
    for (const arg of expr.args) {
      collectColumnsFromExpr(arg, columns)
    }
    collectColumnsFromExpr(expr.filter, columns)
  } else if (expr.type === 'cast') {
    collectColumnsFromExpr(expr.expr, columns)
  } else if (expr.type === 'in valuelist') {
    collectColumnsFromExpr(expr.expr, columns)
    for (const val of expr.values) {
      collectColumnsFromExpr(val, columns)
    }
  } else if (expr.type === 'in') {
    collectColumnsFromExpr(expr.expr, columns)
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
  // No columns: count(*), literal, interval, exists, not exists, subquery
}
