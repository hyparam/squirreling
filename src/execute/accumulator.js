import { keyify } from './utils.js'

/**
 * @import { SqlPrimitive } from '../types.js'
 */

/**
 * Incremental state for one streamable aggregate. Shared by the streaming
 * aggregate executor and the scanColumn fast path so COUNT/COUNTIF/SUM/AVG/
 * MIN/MAX fold semantics live in one place.
 *
 * @typedef {{
 *   count: number,
 *   sum: number,
 *   min: SqlPrimitive,
 *   max: SqlPrimitive,
 *   seen: Set<unknown> | null,
 * }} Accumulator
 */

/**
 * @param {string} funcName
 * @param {boolean} [distinct]
 * @returns {Accumulator}
 */
export function newAccumulator(funcName, distinct) {
  return {
    count: 0,
    sum: 0,
    min: null,
    max: null,
    seen: funcName === 'COUNT' && distinct ? new Set() : null,
  }
}

/**
 * Folds one value into an accumulator, matching the buffered semantics in
 * evaluate.js: COUNT counts non-null, COUNTIF counts truthy, MIN/MAX compare
 * raw values, SUM/AVG only accumulate finite numbers.
 *
 * @param {string} funcName
 * @param {Accumulator} acc
 * @param {SqlPrimitive} value
 */
export function updateAccumulator(funcName, acc, value) {
  switch (funcName) {
  case 'COUNT':
    if (value == null) break
    if (acc.seen) acc.seen.add(keyify(value))
    else acc.count++
    break
  case 'COUNTIF':
    if (value) acc.count++
    break
  case 'MIN':
    if (value != null && (acc.min === null || value < acc.min)) acc.min = value
    break
  case 'MAX':
    if (value != null && (acc.max === null || value > acc.max)) acc.max = value
    break
  default: { // SUM, AVG
    if (value == null) break
    const num = Number(value)
    if (Number.isFinite(num)) {
      acc.sum += num
      acc.count++
    }
  }
  }
}

/**
 * Reduces an accumulator to its final aggregate value.
 *
 * @param {string} funcName
 * @param {Accumulator} acc
 * @returns {SqlPrimitive}
 */
export function finalizeAccumulator(funcName, acc) {
  switch (funcName) {
  case 'COUNT': return acc.seen ? acc.seen.size : acc.count
  case 'COUNTIF': return acc.count
  case 'SUM': return acc.count === 0 ? null : acc.sum
  case 'AVG': return acc.count === 0 ? null : acc.sum / acc.count
  case 'MIN': return acc.min
  case 'MAX': return acc.max
  default: return null
  }
}
