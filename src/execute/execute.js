/**
 * @import { ExecuteSqlOptions, FunctionColumn, FunctionNode, OrderByItem, Row, SelectStatement, SqlPrimitive } from '../types.js'
 */

import { defaultAggregateAlias, evaluateAggregate } from './aggregates.js'
import { evaluateExpr } from './expression.js'
import { createHavingContext, evaluateHavingExpr } from './having.js'
import { parseSql } from '../parse/parse.js'

/**
 * Executes a SQL SELECT query against an array of data rows
 *
 * @param {ExecuteSqlOptions} options - the execution options
 * @returns {Row[]} the result rows matching the query
 */
export function executeSql({ source, sql }) {
  const select = parseSql(sql)
  return evaluateSelectAst(select, source)
}

/**
 * Generates a default alias name for a string function
 *
 * @param {FunctionColumn} col - the function column definition
 * @returns {string} the generated alias (e.g., "upper_name", "concat_a_b")
 */
function defaultFunctionAlias(col) {
  const base = col.func.toLowerCase()
  // Try to extract column names from identifier arguments
  const columnNames = col.args
    .filter(arg => arg.type === 'identifier')
    .map(arg => arg.name)
  if (columnNames.length > 0) {
    return base + '_' + columnNames.join('_')
  }
  return base
}

/**
 * Creates a stable string key for a row to enable deduplication
 *
 * @param {Row} row
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
 * @param {Row[]} rows - The input rows
 * @param {boolean} distinct - Whether to apply deduplication
 * @returns {Row[]} The deduplicated rows
 */
function applyDistinct(rows, distinct) {
  if (!distinct) return rows
  /** @type {Set<string>} */
  const seen = new Set()
  /** @type {Row[]} */
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
 * @param {Row[]} rows - the input rows
 * @param {OrderByItem[]} orderBy - the sort specifications
 * @returns {Row[]} the sorted rows
 */
function applyOrderBy(rows, orderBy) {
  if (!orderBy?.length) return rows

  const sorted = rows.slice()
  sorted.sort((a, b) => {
    for (const term of orderBy) {
      const dir = term.direction
      const av = evaluateExpr(term.expr, a)
      const bv = evaluateExpr(term.expr, b)
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
 * @param {Row[]} rows - the data rows
 * @returns {Row[]} the filtered, projected, and sorted result rows
 */
function evaluateSelectAst(select, rows) {
  // Check for unsupported JOIN operations
  if (select.joins.length) {
    throw new Error('JOIN is not supported')
  }

  // WHERE clause filtering
  let working = rows
  if (select.where) {
    /** @type {Row[]} */
    const filtered = []
    for (const row of rows) {
      if (evaluateExpr(select.where, row)) {
        filtered.push(row)
      }
    }
    working = filtered
  }

  const hasAggregate = select.columns.some(col => col.kind === 'aggregate')
  const useGrouping = hasAggregate || select.groupBy?.length > 0

  /** @type {Row[]} */
  const projected = []

  if (useGrouping) {
    // Grouping due to GROUP BY or aggregate functions
    /** @type {Row[][]} */
    const groups = []

    if (select.groupBy?.length) {
      /** @type {Map<string, Row[]>} */
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
      /** @type {Row} */
      const resultRow = {}
      for (const col of select.columns) {
        if (col.kind === 'star') {
          const firstRow = group[0] || {}
          const keys = Object.keys(firstRow)
          for (const key of keys) {
            resultRow[key] = firstRow[key]
          }
          continue
        }

        if (col.kind === 'column') {
          const name = col.column
          const alias = col.alias ?? name
          // Evaluate on first row of group (all rows have same value for GROUP BY columns)
          resultRow[alias] = group.length > 0 ? group[0][name] : undefined
          continue
        }

        if (col.kind === 'function') {
          // Evaluate function on the first row of the group
          /** @type {FunctionNode} */
          const funcNode = { type: 'function', name: col.func, args: col.args }
          const alias = col.alias ?? defaultFunctionAlias(col)
          const value = group.length > 0 ? evaluateExpr(funcNode, group[0]) : undefined
          resultRow[alias] = value
          continue
        }

        if (col.kind === 'aggregate') {
          const alias = col.alias ?? defaultAggregateAlias(col)
          const value = evaluateAggregate(col, group)
          resultRow[alias] = value
          continue
        }

        if (col.kind === 'operation') {
          const alias = col.alias ?? 'expr'
          const value = group.length > 0 ? evaluateExpr(col.expr, group[0]) : undefined
          resultRow[alias] = value
          continue
        }
      }

      // Apply HAVING filter before adding to projected results
      if (select.having) {
        // For HAVING, we need to evaluate aggregates in the context of the group
        // Create a special row context that includes both the group data and aggregate values
        const havingContext = createHavingContext(resultRow, group)
        if (!evaluateHavingExpr(select.having, havingContext, group)) {
          continue
        }
      }

      projected.push(resultRow)
    }
  } else {
    // No grouping, simple projection
    for (const row of working) {
      /** @type {Row} */
      const outRow = {}
      for (const col of select.columns) {
        if (col.kind === 'star') {
          const keys = Object.keys(row)
          for (const key of keys) {
            outRow[key] = row[key]
          }
        } else if (col.kind === 'column') {
          const name = col.column
          const alias = col.alias ?? name
          outRow[alias] = row[name]
        } else if (col.kind === 'function') {
          /** @type {FunctionNode} */
          const funcNode = { type: 'function', name: col.func, args: col.args }
          const value = evaluateExpr(funcNode, row)
          const alias = col.alias ?? defaultFunctionAlias(col)
          outRow[alias] = value
        } else if (col.kind === 'operation') {
          const alias = col.alias ?? 'expr'
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
