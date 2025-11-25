import { defaultAggregateAlias, evaluateAggregate } from './aggregates.js'
import { evaluateExpr } from './expression.js'
import { evaluateHavingExpr } from './having.js'
import { parseSql } from '../parse/parse.js'
import { createMemorySource, createRowAccessor } from '../backend/memory.js'

/**
 * @import { DataSource, ExecuteSqlOptions, ExprNode, OrderByItem, RowSource, SelectStatement, SqlPrimitive } from '../types.js'
 */

/**
 * Executes a SQL SELECT query against named data sources
 *
 * @param {ExecuteSqlOptions} options - the execution options
 * @returns {Record<string, any>[]} the result rows matching the query
 */
export function executeSql({ tables, query }) {
  const select = parseSql(query)

  // Check for unsupported JOIN operations
  if (select.joins.length) {
    throw new Error('JOIN is not supported')
  }

  // Get the table name from the FROM clause
  if (typeof select.from !== 'string') {
    throw new Error('Subquery in FROM clause is not supported')
  }
  if (!select.from) {
    throw new Error('FROM clause is required')
  }

  const table = tables[select.from]
  if (table === undefined) {
    throw new Error(`Table "${select.from}" not found`)
  }

  // Convert raw data to DataSource if needed
  const dataSource = Array.isArray(table) ? createMemorySource(table) : table
  return evaluateSelectAst(select, dataSource)
}

/**
 * Generates a default alias for a derived column expression
 *
 * @param {ExprNode} expr - the expression node
 * @returns {string} the generated alias
 */
function defaultDerivedAlias(expr) {
  if (expr.type === 'identifier') {
    return expr.name
  }
  if (expr.type === 'function') {
    const base = expr.name.toLowerCase()
    // Try to extract column names from identifier arguments
    const columnNames = expr.args
      .filter(arg => arg.type === 'identifier')
      .map(arg => arg.name)
    if (columnNames.length > 0) {
      return base + '_' + columnNames.join('_')
    }
    return base
  }
  if (expr.type === 'cast') return 'cast_expr'
  if (expr.type === 'unary' && expr.argument.type === 'identifier') {
    return expr.op === '-' ? 'neg_' + expr.argument.name : 'expr'
  }
  return 'expr'
}

/**
 * Creates a stable string key for a row to enable deduplication
 *
 * @param {Record<string, any>} row
 * @returns {string} a stable string representation of the row
 */
function stableRowKey(row) {
  const keys = Object.keys(row).sort()
  /** @type {string[]} */
  const parts = []
  for (const k of keys) {
    const v = row[k]
    parts.push(k + ':' + JSON.stringify(v))
  }
  return parts.join('|')
}

/**
 * Compares two SQL values for sorting
 *
 * @param {SqlPrimitive} a
 * @param {SqlPrimitive} b
 * @returns {number} negative if a < b, positive if a > b, 0 if equal
 */
function compareValues(a, b) {
  if (a === b) return 0
  if (a == null) return -1
  if (b == null) return 1

  if (typeof a === 'number' && typeof b === 'number') {
    if (a < b) return -1
    if (a > b) return 1
    return 0
  }

  const as = String(a)
  const bs = String(b)
  if (as < bs) return -1
  if (as > bs) return 1
  return 0
}

/**
 * Applies DISTINCT filtering to remove duplicate rows
 *
 * @param {Record<string, any>[]} rows - The input rows
 * @param {boolean} distinct - Whether to apply deduplication
 * @returns {Record<string, any>[]} The deduplicated rows
 */
function applyDistinct(rows, distinct) {
  if (!distinct) return rows
  /** @type {Set<string>} */
  const seen = new Set()
  /** @type {Record<string, any>[]} */
  const result = []
  for (const row of rows) {
    const key = stableRowKey(row)
    if (seen.has(key)) continue
    seen.add(key)
    result.push(row)
  }
  return result
}

/**
 * Applies ORDER BY sorting to rows
 *
 * @param {Record<string, any>[]} rows - the input rows
 * @param {OrderByItem[]} orderBy - the sort specifications
 * @returns {Record<string, any>[]} the sorted rows
 */
function applyOrderBy(rows, orderBy) {
  if (!orderBy?.length) return rows

  const sorted = rows.slice()
  sorted.sort((a, b) => {
    for (const term of orderBy) {
      const dir = term.direction
      const av = evaluateExpr(term.expr, createRowAccessor(a))
      const bv = evaluateExpr(term.expr, createRowAccessor(b))

      // Handle NULLS FIRST / NULLS LAST
      const aIsNull = av == null
      const bIsNull = bv == null

      if (aIsNull || bIsNull) {
        if (aIsNull && bIsNull) continue // both null, try next sort term

        // Determine null ordering
        const nullsFirst = term.nulls === 'LAST' ? false : true // default is NULLS FIRST

        if (aIsNull) {
          return nullsFirst ? -1 : 1
        } else {
          return nullsFirst ? 1 : -1
        }
      }

      const cmp = compareValues(av, bv)
      if (cmp !== 0) {
        return dir === 'DESC' ? -cmp : cmp
      }
    }
    return 0
  })

  return sorted
}

/**
 * Evaluates a parsed SELECT AST against data rows
 *
 * @param {SelectStatement} select - the parsed SQL AST
 * @param {DataSource} dataSource - the data source
 * @returns {Record<string, any>[]} the filtered, projected, and sorted result rows
 */
function evaluateSelectAst(select, dataSource) {
  // SQL priority: from, where, group by, having, select, order by, offset, limit

  // WHERE clause filtering
  /** @type {RowSource[]} */
  const working = []
  const length = dataSource.getNumRows()
  for (let i = 0; i < length; i++) {
    const row = dataSource.getRow(i)
    if (!select.where || evaluateExpr(select.where, row)) {
      working.push(row)
    }
  }

  const hasAggregate = select.columns.some(col => col.kind === 'aggregate')
  const useGrouping = hasAggregate || select.groupBy?.length > 0

  /** @type {Record<string, any>[]} */
  const projected = []

  if (useGrouping) {
    // Grouping due to GROUP BY or aggregate functions
    /** @type {RowSource[][]} */
    const groups = []

    if (select.groupBy?.length) {
      /** @type {Map<string, RowSource[]>} */
      const map = new Map()
      for (const row of working) {
        /** @type {string[]} */
        const keyParts = []
        for (const expr of select.groupBy) {
          const v = evaluateExpr(expr, row)
          keyParts.push(JSON.stringify(v))
        }
        const key = keyParts.join('|')
        let group = map.get(key)
        if (!group) {
          group = []
          map.set(key, group)
          groups.push(group)
        }
        group.push(row)
      }
    } else {
      groups.push(working)
    }

    const hasStar = select.columns.some(col => col.kind === 'star')
    if (hasStar && hasAggregate) {
      throw new Error('SELECT * with aggregate functions is not supported in this implementation')
    }

    for (const group of groups) {
      /** @type {Record<string, any>} */
      const resultRow = {}
      for (const col of select.columns) {
        if (col.kind === 'star') {
          const firstRow = group[0]
          if (firstRow) {
            const keys = firstRow.getKeys()
            for (const key of keys) {
              resultRow[key] = firstRow.getCell(key)
            }
          }
          continue
        }

        if (col.kind === 'derived') {
          const alias = col.alias ?? defaultDerivedAlias(col.expr)
          const value = group.length > 0 ? evaluateExpr(col.expr, group[0]) : undefined
          resultRow[alias] = value
          continue
        }

        if (col.kind === 'aggregate') {
          const alias = col.alias ?? defaultAggregateAlias(col)
          const value = evaluateAggregate(col, group)
          resultRow[alias] = value
          continue
        }
      }

      // Apply HAVING filter before adding to projected results
      if (select.having) {
        // For HAVING, we need to evaluate aggregates in the context of the group
        // Create a special row context that includes both the group data and aggregate values
        if (!evaluateHavingExpr(select.having, resultRow, group)) {
          continue
        }
      }

      projected.push(resultRow)
    }
  } else {
    // No grouping, simple projection
    for (const row of working) {
      /** @type {Record<string, any>} */
      const outRow = {}
      for (const col of select.columns) {
        if (col.kind === 'star') {
          const keys = row.getKeys()
          for (const key of keys) {
            outRow[key] = row.getCell(key)
          }
        } else if (col.kind === 'derived') {
          const alias = col.alias ?? defaultDerivedAlias(col.expr)
          const value = evaluateExpr(col.expr, row)
          outRow[alias] = value
        } else if (col.kind === 'aggregate') {
          throw new Error(
            'Aggregate functions require GROUP BY or will act on the whole dataset; add GROUP BY or remove aggregates'
          )
        }
      }
      projected.push(outRow)
    }
  }

  let result = projected

  result = applyDistinct(result, select.distinct)
  result = applyOrderBy(result, select.orderBy)

  if (typeof select.offset === 'number' && select.offset > 0) {
    result = result.slice(select.offset)
  }
  if (typeof select.limit === 'number') {
    result = result.slice(0, select.limit)
  }

  return result
}
