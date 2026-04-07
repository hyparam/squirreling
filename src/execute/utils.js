/**
 * @import { AsyncRow, OrderByItem, QueryResults, SqlPrimitive } from '../types.js'
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
 * Collects and materialize all results from query results into an array
 *
 * @param {QueryResults} results
 * @returns {Promise<Record<string, SqlPrimitive>[]>} array of all yielded values
 */
export async function collect(results) {
  // Collect all rows first, then materialize cells concurrently
  // This enables dataloader-style batching of cell accessors
  /** @type {AsyncRow[]} */
  const rows = []
  for await (const asyncRow of results.rows()) {
    rows.push(asyncRow)
  }
  return Promise.all(rows.map(async asyncRow => {
    const values = await Promise.all(asyncRow.columns.map(k => asyncRow.cells[k]()))
    /** @type {Record<string, SqlPrimitive>} */
    const item = {}
    for (let i = 0; i < asyncRow.columns.length; i++) {
      item[asyncRow.columns[i]] = values[i]
    }
    return item
  }))
}

/**
 * Adds two optional bounds, returning undefined if either is unknown.
 *
 * @param {number | undefined} a
 * @param {number | undefined} b
 * @returns {number | undefined}
 */
export function addBounds(a, b) {
  return a !== undefined && b !== undefined ? a + b : undefined
}

/**
 * Returns the minimum of two optional bounds, or whichever is defined.
 *
 * @param {number | undefined} a
 * @param {number | undefined} b
 * @returns {number | undefined}
 */
export function minBounds(a, b) {
  if (a !== undefined && b !== undefined) return Math.min(a, b)
  return a ?? b
}

/**
 * Returns the maximum of two optional bounds, returning undefined if either is unknown.
 *
 * @param {number | undefined} a
 * @param {number | undefined} b
 * @returns {number | undefined}
 */
export function maxBounds(a, b) {
  if (a !== undefined && b !== undefined) return Math.max(a, b)
  return a ?? b
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
 * Returns a value suitable for use as a Set/Map key.
 * Primitives are returned as-is (fast path), objects are stringified.
 *
 * @param {SqlPrimitive[]} values
 * @returns {string | number | bigint | boolean}
 */
export function keyify(...values) {
  if (values.length === 1 && typeof values[0] !== 'object') return values[0]
  // Strings must be stringified to avoid collisions when joined
  return values.map(v => typeof v === 'object' ? stringify(v) : v).join('|')
}

/**
 * Creates a stable string key for a row to enable deduplication
 *
 * @param {AsyncRow} row
 * @returns {Promise<string | number | bigint | boolean>}
 */
export function stableRowKey(row) {
  return Promise.all(row.columns.map(k => row.cells[k]()))
    .then(values => keyify(...values))
}
