import { queryPlan } from './plan.js'
import { parseSql } from '../parse/parse.js'

/**
 * @import { AsyncDataSource, SelectStatement, UserDefinedFunction } from '../types.js'
 * @import { QueryPlan, ScanNode } from './types.d.ts'
 */

/**
 * Estimates the worst-case cost of a query.
 * Cost is computed from column weights and row counts provided by data sources.
 * Returns undefined if any data source lacks statistics.
 *
 * @param {object} options
 * @param {string | SelectStatement} options.query - SQL query string or parsed AST
 * @param {Record<string, AsyncDataSource>} options.tables - data sources with optional statistics
 * @param {Record<string, UserDefinedFunction>} [options.functions] - user-defined functions
 * @returns {number | undefined} estimated worst-case cost, or undefined if not estimable
 */
export function estimateCost({ query, tables, functions }) {
  const select = typeof query === 'string' ? parseSql({ query, functions }) : query
  const plan = queryPlan(select)
  return planCost({ plan, tables })
}

/**
 * Recursively computes worst-case cost for a query plan node.
 *
 * @param {object} options
 * @param {QueryPlan} options.plan
 * @param {Record<string, AsyncDataSource>} options.tables
 * @returns {number | undefined}
 */
function planCost({ plan, tables }) {
  if (plan.type === 'Scan') return scanCost(plan, tables[plan.table])
  // Joins and aggregates
  if ('left' in plan) {
    const left = planCost({ plan: plan.left, tables })
    const right = planCost({ plan: plan.right, tables })
    if (left === undefined || right === undefined) return undefined
    return left + right
  }
  // Single child nodes (e.g. filter, project)
  return planCost({ plan: plan.child, tables })
}

/**
 * Computes worst-case scan cost for a single table.
 * Returns undefined if the data source lacks statistics.
 *
 * @param {ScanNode} plan
 * @param {AsyncDataSource} [source]
 * @returns {number | undefined}
 */
function scanCost({ table, hints }, source) {
  if (!source?.statistics) return undefined
  const { rowCount, columnWeights } = source.statistics
  if (!columnWeights) return undefined

  let weightSum = 0
  if (hints.columns === undefined) {
    // All columns accessed
    for (const w of Object.values(columnWeights)) {
      weightSum += w
    }
  } else {
    for (const col of hints.columns) {
      // Strip table qualifier if it matches this table's alias
      const name = col.startsWith(table + '.') ? col.slice(table.length + 1) : col
      weightSum += columnWeights[name] ?? 0
    }
  }

  // Cap rows when limit is pushed down to the scan.
  // Ignore limit when there is a WHERE clause, because worst-case
  // the filter rejects rows, forcing a full scan to find enough matches.
  const limit = !hints.where ? hints.limit : undefined
  const offset = hints.offset ?? 0
  const effectiveRows = limit !== undefined
    ? Math.min(rowCount, limit + offset)
    : rowCount

  return effectiveRows * weightSum
}
