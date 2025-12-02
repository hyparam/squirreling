import { evaluateExpr } from './expression.js'
import { defaultDerivedAlias } from './utils.js'

/**
 * Evaluates an aggregate function over a set of rows
 *
 * @import { AggregateColumn, AsyncDataSource, ExprNode, AsyncRow } from '../types.js'
 * @param {Object} options
 * @param {AggregateColumn} options.col - aggregate column definition
 * @param {AsyncRow[]} options.rows - rows to aggregate
 * @param {Record<string, AsyncDataSource>} options.tables
 * @returns {Promise<number | null>} aggregated result
 */
export async function evaluateAggregate({ col, rows, tables }) {
  const { arg, func } = col

  if (func === 'COUNT') {
    if (arg.kind === 'star') return rows.length
    let count = 0
    for (const row of rows) {
      const v = await evaluateExpr({ node: arg.expr, row, tables })
      if (v !== null && v !== undefined) {
        count += 1
      }
    }
    return count
  }

  if (func === 'SUM' || func === 'AVG' || func === 'MIN' || func === 'MAX') {
    if (arg.kind === 'star') {
      throw new Error(func + '(*) is not supported, use a column name')
    }
    let sum = 0
    let count = 0
    /** @type {number | null} */
    let min = null
    /** @type {number | null} */
    let max = null

    for (const row of rows) {
      const raw = await evaluateExpr({ node: arg.expr, row, tables })
      if (raw == null) continue
      const num = Number(raw)
      if (!Number.isFinite(num)) continue

      if (count === 0) {
        min = num
        max = num
      } else {
        if (min == null || num < min) min = num
        if (max == null || num > max) max = num
      }
      sum += num
      count += 1
    }

    if (func === 'SUM') return sum
    if (func === 'AVG') return count === 0 ? null : sum / count
    if (func === 'MIN') return min
    if (func === 'MAX') return max
  }

  throw new Error('Unsupported aggregate function ' + func)
}

/**
 * Generates a default alias name for an aggregate function
 * (e.g., "count_all", "sum_amount")
 *
 * @param {AggregateColumn} col
 * @returns {string}
 */
export function defaultAggregateAlias(col) {
  const base = col.func.toLowerCase()
  if (col.arg.kind === 'star') return base + '_all'
  return base + '_' + defaultDerivedAlias(col.arg.expr)
}
