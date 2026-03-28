/**
 * @import { ExprNode, FromSubquery, FromTable, SelectStatement } from '../types.js'
 */

/**
 * @param {FromTable | FromSubquery} from
 * @returns {string}
 */
export function fromAlias(from) {
  return from.alias ?? (from.type === 'table' ? from.table : 'table')
}

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
  const aliases = [fromAlias(select.from)]
  for (const join of select.joins) {
    aliases.push(join.alias ?? join.table)
  }

  // If any unqualified SELECT * exists, all tables need all columns
  if (select.columns.some(col => col.type === 'star' && !col.table)) {
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

  // Collect ORDER BY identifiers, excluding SELECT aliases (their underlying
  // columns are already collected from select.columns expressions above)
  /** @type {Set<string>} */
  const selectAliases = new Set()

  for (const col of select.columns) {
    if (col.type === 'star' && col.table) {
      // SELECT table.* means all columns needed
      perTable.set(col.table, undefined)
    } else if (col.type === 'derived') {
      collectColumnsFromExpr(col.expr, identifiers)
      if (col.alias) {
        selectAliases.add(col.alias)
      }
    }
  }
  collectColumnsFromExpr(select.where, identifiers)

  for (const item of select.orderBy) {
    collectColumnsFromExpr(item.expr, identifiers, selectAliases)
  }
  for (const expr of select.groupBy) {
    collectColumnsFromExpr(expr, identifiers)
  }
  collectColumnsFromExpr(select.having, identifiers, selectAliases)
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
    } else if (aliases.length > 1) {
      // Unqualified in a JOIN: can't disambiguate, request all columns from all tables
      for (const alias of aliases) {
        perTable.set(alias, undefined)
      }
    } else {
      // Unqualified, single table: add to that table
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
 * @param {Set<string>} [aliases] - aliases to exclude from columns
 */
function collectColumnsFromExpr(expr, columns, aliases) {
  if (!expr) return
  if (expr.type === 'identifier') {
    if (!aliases?.has(expr.name)) {
      columns.add(expr.name)
    }
  } else if (expr.type === 'binary') {
    collectColumnsFromExpr(expr.left, columns, aliases)
    collectColumnsFromExpr(expr.right, columns, aliases)
  } else if (expr.type === 'unary') {
    collectColumnsFromExpr(expr.argument, columns, aliases)
  } else if (expr.type === 'function') {
    for (const arg of expr.args) {
      collectColumnsFromExpr(arg, columns, aliases)
    }
    collectColumnsFromExpr(expr.filter, columns, aliases)
  } else if (expr.type === 'cast') {
    collectColumnsFromExpr(expr.expr, columns, aliases)
  } else if (expr.type === 'in valuelist') {
    collectColumnsFromExpr(expr.expr, columns, aliases)
    for (const val of expr.values) {
      collectColumnsFromExpr(val, columns, aliases)
    }
  } else if (expr.type === 'in') {
    collectColumnsFromExpr(expr.expr, columns, aliases)
  } else if (expr.type === 'case') {
    if (expr.caseExpr) {
      collectColumnsFromExpr(expr.caseExpr, columns, aliases)
    }
    for (const when of expr.whenClauses) {
      collectColumnsFromExpr(when.condition, columns, aliases)
      collectColumnsFromExpr(when.result, columns, aliases)
    }
    if (expr.elseResult) {
      collectColumnsFromExpr(expr.elseResult, columns, aliases)
    }
  }
  // No columns: count(*), literal, interval, exists, not exists, subquery
}
