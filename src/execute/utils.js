import { collectBatches } from '../backend/batchAdapters.js'
import { toDate } from '../expression/date.js'

/**
 * @import { AsyncRow, OrderByItem, QueryResults, SqlPrimitive } from '../types.js'
 */

const primitiveTypes = new Set(['number', 'bigint', 'boolean', 'string'])

/** @type {WeakMap<QueryResults, AbortSignal>} */
const querySignals = new WeakMap()

/**
 * Associates an execution signal with results without expanding the public
 * result shape solely for the collection adapter.
 *
 * @param {QueryResults} results
 * @param {AbortSignal} [signal]
 * @returns {QueryResults}
 */
export function bindQuerySignal(results, signal) {
  if (!signal || querySignals.get(results) === signal) return results

  const { batches, rows } = results
  results.rows = async function* boundRows() {
    signal.throwIfAborted()
    yield* rows.call(results)
    signal.throwIfAborted()
  }
  if (batches) {
    results.batches = async function* boundBatches() {
      signal.throwIfAborted()
      yield* batches.call(results)
      signal.throwIfAborted()
    }
  }
  querySignals.set(results, signal)
  return results
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
  if (a instanceof Date && b instanceof Date) {
    const at = a.getTime()
    const bt = b.getTime()
    cmp = at < bt ? -1 : at > bt ? 1 : 0
  } else if (primitiveTypes.has(typeof a) && primitiveTypes.has(typeof b)) {
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
  if (results.batches) {
    return await collectBatches(results.batches(), results.columns, querySignals.get(results))
  }

  // Materialize pre-resolved rows as they arrive, so a collected row does not
  // keep its source row (and columns read only to filter it) alive until the
  // scan ends. Lazy rows are gathered first, then materialized concurrently,
  // which enables dataloader-style batching of cell accessors.
  /** @type {Record<string, SqlPrimitive>[]} */
  const rows = []
  /** @type {number[]} */
  const lazyIndex = []
  /** @type {AsyncRow[]} */
  const lazyRows = []
  for await (const asyncRow of results.rows()) {
    /** @type {Record<string, SqlPrimitive>} */
    const item = {}
    if (asyncRow.resolved) {
      for (const col of asyncRow.columns) {
        item[col] = asyncRow.resolved[col]
      }
    } else {
      lazyIndex.push(rows.length)
      lazyRows.push(asyncRow)
    }
    rows.push(item)
  }

  await Promise.all(lazyRows.map(async (asyncRow, n) => {
    const values = await Promise.all(asyncRow.columns.map(k => asyncRow.cells[k]()))
    const item = rows[lazyIndex[n]]
    for (let i = 0; i < asyncRow.columns.length; i++) {
      item[asyncRow.columns[i]] = values[i]
    }
  }))
  return rows
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
 * SQL equality for primitives. Two Date instances for the same instant compare
 * equal (JS `==` would compare by identity), and a string compared with a Date
 * is read as a timestamp.
 *
 * @param {SqlPrimitive} a
 * @param {SqlPrimitive} b
 * @returns {boolean}
 */
export function sqlEquals(a, b) {
  if (a instanceof Date && typeof b === 'string') b = toDate(b)
  else if (b instanceof Date && typeof a === 'string') a = toDate(a)
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime()
  if (a instanceof Date || b instanceof Date) return false
  return a == b
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
 * Non-string primitives are returned as-is; strings are length-prefixed and
 * objects are stringified so their serialized text cannot collide with strings.
 *
 * @param {SqlPrimitive[]} values
 * @returns {string | number | bigint | boolean}
 */
export function keyify(...values) {
  if (values.length === 1 && typeof values[0] !== 'object' && typeof values[0] !== 'string') return values[0]
  // Length-prefix strings so a tuple cannot be confused with another that
  // splits differently, or with the same text as a number or NULL. Tag
  // bigints so composite keys preserve the same type distinction as scalars.
  return values.map(v => typeof v === 'string' ? `${v.length}:${v}` : typeof v === 'object' ? stringify(v) : typeof v === 'bigint' ? `${v}n` : v).join('|')
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
