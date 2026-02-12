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
  // Build alias list from FROM + JOINs
  const fromAlias = select.from.kind === 'table'
    ? select.from.alias ?? select.from.table
    : select.from.alias
  /** @type {string[]} */
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

  // Track which tables need all columns (SELECT table.*)
  /** @type {Set<string>} */
  const allColumnsNeeded = new Set()
  for (const col of select.columns) {
    if (col.kind === 'star' && col.table) {
      allColumnsNeeded.add(col.table)
    }
  }

  // Collect all identifiers from all clauses
  /** @type {Set<string>} */
  const identifiers = new Set()
  for (const col of select.columns) {
    if (col.kind === 'derived') {
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

  // Initialize per-table sets (skip tables needing all columns)
  /** @type {Map<string, Set<string>>} */
  const perTable = new Map()
  for (const alias of aliases) {
    if (!allColumnsNeeded.has(alias)) {
      perTable.set(alias, new Set())
    }
  }

  // Partition identifiers by table prefix
  for (const name of identifiers) {
    const dotIndex = name.indexOf('.')
    if (dotIndex >= 0) {
      // Qualified: add to matching table only
      const tablePrefix = name.substring(0, dotIndex)
      const columnName = name.substring(dotIndex + 1)
      const set = perTable.get(tablePrefix)
      if (set) {
        set.add(columnName)
      }
    } else {
      // Unqualified: add to all tables (ambiguous)
      for (const [, set] of perTable) {
        set.add(name)
      }
    }
  }

  // Build result map: convert Sets to arrays, undefined for all-columns tables
  /** @type {Map<string, string[] | undefined>} */
  const result = new Map()
  for (const alias of aliases) {
    if (allColumnsNeeded.has(alias)) {
      result.set(alias, undefined)
    } else {
      const set = perTable.get(alias)
      result.set(alias, set ? [...set] : undefined)
    }
  }
  return result
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
