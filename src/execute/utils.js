/**
 * @import { AsyncCells, AsyncRow, OrderByItem, SqlPrimitive } from '../types.js'
 */

/**
 * Compares two values for a single ORDER BY term, handling nulls and direction
 *
 * @param {SqlPrimitive} a
 * @param {SqlPrimitive} b
 * @param {OrderByItem} term
 * @returns {number}
 */
export function compareForTerm(a, b, term) {
  const aIsNull = a == null
  const bIsNull = b == null

  if (aIsNull || bIsNull) {
    if (aIsNull && bIsNull) return 0
    const nullsFirst = term.nulls !== 'LAST'
    if (aIsNull) return nullsFirst ? -1 : 1
    return nullsFirst ? 1 : -1
  }

  // Compare non-null values
  if (a == b) return 0

  const primitives = ['number', 'bigint', 'boolean', 'string']
  let cmp
  if (primitives.includes(typeof a) && primitives.includes(typeof b)) {
    cmp = a < b ? -1 : a > b ? 1 : 0
  } else {
    const aa = String(a)
    const bb = String(b)
    cmp = aa < bb ? -1 : aa > bb ? 1 : 0
  }

  return term.direction === 'DESC' ? -cmp : cmp
}

/**
 * Collects and materialize all results from an async row generator into an array
 *
 * @param {AsyncGenerator<AsyncRow>} asyncRows
 * @returns {Promise<Record<string, SqlPrimitive>[]>} array of all yielded values
 */
export async function collect(asyncRows) {
  /** @type {Record<string, SqlPrimitive>[]} */
  const results = []
  for await (const asyncRow of asyncRows) {
    /** @type {Record<string, SqlPrimitive>} */
    const item = {}
    for (const key of asyncRow.columns) {
      item[key] = await asyncRow.cells[key]()
    }
    results.push(item)
  }
  return results
}

/**
 * @param {SqlPrimitive} value
 * @returns {string}
 */
export function stringify(value) {
  if (value == null) return 'NULL'
  return JSON.stringify(value, (_, val) => {
    if (typeof val === 'bigint') {
      return val <= Number.MAX_SAFE_INTEGER ? Number(val) : val.toString()
    }
    return val
  })
}

/**
 * Creates a stable string key for a row to enable deduplication
 *
 * @param {AsyncCells} cells
 * @returns {Promise<string>}
 */
export async function stableRowKey(cells) {
  const keys = Object.keys(cells).sort()
  /** @type {string[]} */
  const parts = []
  for (const k of keys) {
    const v = await cells[k]()
    parts.push(k + ':' + stringify(v))
  }
  return parts.join('|')
}
