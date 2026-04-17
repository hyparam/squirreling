import { derivedAlias } from '../expression/alias.js'

/**
 * @import { AsyncDataSource, ExprNode, FromSubquery, FromTable, IdentifierNode, SelectStatement, Statement } from '../types.js'
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
 * @param {object} options
 * @param {SelectStatement} options.select
 * @param {IdentifierNode[]} [options.parentColumns] - columns needed by the parent query
 * @returns {Map<string, string[] | undefined>}
 */
export function extractColumns({ select, parentColumns }) {
  /** @type {Map<string, string[] | undefined>} */
  const result = new Map()

  // Build alias list from FROM + JOINs
  const aliases = [fromAlias(select.from)]
  for (const join of select.joins) {
    aliases.push(join.alias ?? join.table)
  }

  // If any unqualified SELECT * exists, all tables need all columns
  if (select.columns.some(col => col.type === 'star' && !col.table)) {
    if (!parentColumns) {
      /** @type {Map<string, string[] | undefined>} */
      const result = new Map()
      for (const alias of aliases) {
        result.set(alias, undefined)
      }
      return result
    }
    // With parentColumns, fall through to collect internal clause columns
    // and seed with what the parent needs
  }

  // Track per-table columns needed; undefined means all columns (table.*)
  /** @type {Map<string, Set<string> | undefined>} */
  const perTable = new Map(aliases.map(alias => [alias, new Set()]))

  // Collect all identifiers from all clauses
  // For SELECT *, parent column names are real table columns, so seed them
  // directly. For non-star queries, parent names may be aliases and are
  // handled below by filtering derived columns and collecting from expressions.
  const hasStar = select.columns.some(col => col.type === 'star' && !col.table)
  // Exclude parent names that match a derived alias in this SELECT — those are
  // produced by projection (e.g. `SELECT *, a+b AS c`), not by the source.
  /** @type {Set<string>} */
  const derivedAliases = new Set()
  for (const col of select.columns) {
    if (col.type === 'derived') {
      derivedAliases.add(col.alias ?? derivedAlias(col.expr))
    }
  }
  /** @type {IdentifierNode[]} */
  const identifiers = hasStar && parentColumns
    ? parentColumns.filter(id => !derivedAliases.has(id.name))
    : []

  // Collect ORDER BY identifiers, excluding SELECT aliases (their underlying
  // columns are already collected from select.columns expressions above)
  /** @type {Set<string>} */
  const selectAliases = new Set()

  for (const col of select.columns) {
    if (col.type === 'star' && col.table) {
      // SELECT table.* means all columns needed
      perTable.set(col.table, undefined)
    } else if (col.type === 'derived') {
      // When parentColumns is set, skip columns the parent doesn't need
      if (parentColumns) {
        const outputName = col.alias ?? derivedAlias(col.expr)
        if (!parentColumns.some(id => id.name === outputName)) continue
      }
      // Exclude earlier SELECT aliases so they aren't treated as source columns
      collectColumnsFromExpr(col.expr, identifiers, selectAliases)
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
    collectColumnsFromExpr(expr, identifiers, selectAliases)
  }
  collectColumnsFromExpr(select.having, identifiers, selectAliases)
  for (const join of select.joins) {
    collectColumnsFromExpr(join.on, identifiers)
  }

  // Partition identifiers by table prefix
  for (const { prefix, name } of identifiers) {
    if (prefix) {
      // Qualified: add to matching table only
      const set = perTable.get(prefix)
      if (set) set.add(name)
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
 * Recursively collects identifier nodes from an expression
 *
 * @param {ExprNode} expr
 * @param {IdentifierNode[]} columns
 * @param {Set<string>} [aliases] - aliases to exclude from columns
 */
function collectColumnsFromExpr(expr, columns, aliases) {
  if (!expr) return
  if (expr.type === 'identifier') {
    if (expr.prefix || !aliases?.has(expr.name)) {
      columns.push(expr)
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
  // Subqueries: collect prefixed identifiers for correlated column detection.
  // Only prefixed identifiers are collected because correlated outer references
  // are always qualified (e.g. users.id, a.session_id). Unprefixed identifiers
  // from the inner query would incorrectly be attributed to the outer table.
  if (expr.type === 'subquery' || expr.type === 'in' || expr.type === 'exists' || expr.type === 'not exists') {
    if (expr.type === 'in') {
      collectColumnsFromExpr(expr.expr, columns, aliases)
    }
    const sub = expr.subquery
    if (sub) {
      /** @type {IdentifierNode[]} */
      const inner = []
      collectColumnsFromStatement(sub, inner)
      for (const id of inner) {
        if (id.prefix) columns.push(id)
      }
    }
  }
  // No columns: count(*), literal, interval
}

/**
 * Collects identifiers from a subquery statement for correlated column detection.
 *
 * @param {Statement} stmt
 * @param {IdentifierNode[]} columns
 */
function collectColumnsFromStatement(stmt, columns) {
  if (stmt.type === 'compound') {
    collectColumnsFromStatement(stmt.left, columns)
    collectColumnsFromStatement(stmt.right, columns)
    return
  }
  if (stmt.type === 'with') {
    collectColumnsFromStatement(stmt.query, columns)
    return
  }
  for (const col of stmt.columns) {
    if (col.type === 'derived') collectColumnsFromExpr(col.expr, columns)
  }
  collectColumnsFromExpr(stmt.where, columns)
  if (stmt.from?.type === 'subquery') {
    collectColumnsFromStatement(stmt.from.query, columns)
  }
  for (const join of stmt.joins) collectColumnsFromExpr(join.on, columns)
  for (const expr of stmt.groupBy) collectColumnsFromExpr(expr, columns)
  collectColumnsFromExpr(stmt.having, columns)
  for (const item of stmt.orderBy) collectColumnsFromExpr(item.expr, columns)
}

/**
 * Infers output columns for set-operation validation.
 *
 * @param {object} options
 * @param {Statement} options.stmt
 * @param {Map<string, string[]>} [options.cteColumns]
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @returns {string[]}
 */
export function inferStatementColumns({ stmt, cteColumns, tables }) {
  if (stmt.type === 'with') {
    return inferStatementColumns({ stmt: stmt.query, cteColumns, tables })
  }
  if (stmt.type === 'compound') {
    return inferStatementColumns({ stmt: stmt.left, cteColumns, tables })
  }

  const sourceColumns = inferSelectSourceColumns({ select: stmt, cteColumns, tables })
  /** @type {string[]} */
  const result = []

  for (const col of stmt.columns) {
    if (col.type === 'star') {
      result.push(...sourceColumns)
    } else {
      result.push(col.alias ?? derivedAlias(col.expr))
    }
  }

  return result
}

/**
 * Infers the source columns available before SELECT projection.
 * Mirrors the column ordering used by join row materialization.
 *
 * @param {object} options
 * @param {SelectStatement} options.select
 * @param {Map<string, string[]>} [options.cteColumns]
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @returns {string[]}
 */
function inferSelectSourceColumns({ select, cteColumns, tables }) {
  if (select.from.type === 'subquery') {
    return inferStatementColumns({ stmt: select.from.query, cteColumns, tables })
  }

  if (!select.joins.length) {
    return lookupTableColumns(select.from.table, cteColumns, tables)
  }

  // Collect all sources, then prefix each table's columns
  /** @type {string[]} */
  const result = []
  const fromAlias = select.from.alias ?? select.from.table
  for (const col of lookupTableColumns(select.from.table, cteColumns, tables)) {
    result.push(`${fromAlias}.${col}`)
  }
  for (const join of select.joins) {
    const joinAlias = join.alias ?? join.table
    for (const col of lookupTableColumns(join.table, cteColumns, tables)) {
      result.push(`${joinAlias}.${col}`)
    }
  }
  return result
}

/**
 * @param {string} table
 * @param {Map<string, string[]>} [cteColumns]
 * @param {Record<string, AsyncDataSource>} [tables]
 * @returns {string[]}
 */
function lookupTableColumns(table, cteColumns, tables) {
  return cteColumns?.get(table.toLowerCase()) ?? tables?.[table]?.columns ?? []
}
