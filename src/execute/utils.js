/**
 * @import { AsyncCell, AsyncRow, OrderByItem, QueryResults, SqlPrimitive } from '../types.js'
 */

const primitiveTypes = new Set(['number', 'bigint', 'boolean', 'string'])

/**
 * Reads an AsyncCell, returning the value (bare) or the Promise (thunk).
 * Caller can `await` the result either way; bare values skip both the
 * closure call and the Promise allocation.
 *
 * @param {AsyncCell} cell
 * @returns {SqlPrimitive | Promise<SqlPrimitive>}
 */
export function readCell(cell) {
  return typeof cell === 'function' ? cell() : cell
}

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

  let cmp
  if (primitiveTypes.has(typeof a) && primitiveTypes.has(typeof b)) {
    cmp = a < b ? -1 : 1
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
    /** @type {Record<string, SqlPrimitive>} */
    const item = {}
    if (asyncRow.columns.length === 0) return item
    // Peek the first cell to pick a fast path. Most data sources produce
    // rows of uniform cell shape — all bare values (memorySource) or all
    // thunks (parquet-style async sources) — so the branch is predictable
    // and we avoid both pending-array overhead (bare path) and Promise.all's
    // implicit Promise.resolve coercion of bare values (thunk path).
    const firstCell = asyncRow.cells[asyncRow.columns[0]]
    if (typeof firstCell !== 'function') {
      for (const k of asyncRow.columns) {
        const c = asyncRow.cells[k]
        item[k] = typeof c === 'function' ? await c() : c
      }
      return item
    }
    const values = await Promise.all(asyncRow.columns.map(k => {
      const c = asyncRow.cells[k]
      return typeof c === 'function' ? c() : c
    }))
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
 * Returns true for plain object SqlPrimitive values, excluding null, arrays, and Dates.
 *
 * @param {SqlPrimitive} value
 * @returns {value is Record<string, SqlPrimitive>}
 */
export function isPlainObject(value) {
  return value != null
    && typeof value === 'object'
    && !Array.isArray(value)
    && !(value instanceof Date)
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
  return Promise.all(row.columns.map(k => readCell(row.cells[k])))
    .then(values => keyify(...values))
}
