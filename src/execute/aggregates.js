
/**
 * Evaluates an aggregate function over a set of rows
 *
 * @import { AggregateColumn, Row } from '../types.js'
 * @param {AggregateColumn} col - aggregate column definition
 * @param {Row[]} rows - rows to aggregate
 * @returns {number | null} aggregated result
 */
export function evaluateAggregate(col, rows) {
  const { arg, func } = col

  if (func === 'COUNT') {
    if (arg.kind === 'star') return rows.length
    const field = arg.column
    let count = 0
    for (let i = 0; i < rows.length; i += 1) {
      const v = rows[i][field]
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
    const field = arg.column
    let sum = 0
    let count = 0
    /** @type {number | null} */
    let min = null
    /** @type {number | null} */
    let max = null

    for (let i = 0; i < rows.length; i += 1) {
      const raw = rows[i][field]
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
 * @param {AggregateColumn} col - The aggregate column definition
 * @returns {string} The generated alias (e.g., "count_all", "sum_amount")
 */
export function defaultAggregateAlias(col) {
  const base = col.func.toLowerCase()
  if (col.arg.kind === 'star') return base + '_all'
  return base + '_' + col.arg.column
}
