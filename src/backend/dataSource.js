/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, SqlPrimitive } from '../types.js'
 */

/**
 * Creates an async row accessor that wraps a plain JavaScript object
 *
 * @param {Record<string, SqlPrimitive>} obj - the plain object
 * @param {string[]} columns - list of column names (keys in the object)
 * @returns {AsyncRow} a row accessor interface
 */
export function asyncRow(obj, columns) {
  /** @type {AsyncCells} */
  const cells = {}
  for (const key of columns) {
    cells[key] = () => Promise.resolve(obj[key])
  }
  return { columns, cells, resolved: obj }
}

/**
 * Creates an async memory-backed data source from an array of plain objects
 *
 * @param {Object} options
 * @param {Record<string, SqlPrimitive>[]} options.data - array of plain objects
 * @param {string[]} [options.columns] - optional list of column names (if not provided, inferred from first row)
 * @returns {AsyncDataSource} an async data source interface
 */
export function memorySource({ data, columns }) {
  if (!columns) {
    // Columns not provided, infer from data
    if (!data.length) {
      throw new Error('Unknown columns: data is empty and no columns provided')
    }
    const firstColumns = Object.keys(data[0])
    // Check first 1000 rows for consistent columns
    const firstColSet = new Set(firstColumns)
    for (let i = 1; i < data.length && i < 1000; i++) {
      const rowColumns = Object.keys(data[i])
      const missing = firstColumns.find(col => !rowColumns.includes(col))
      if (missing) {
        throw new Error(`Inconsistent data, column "${missing}" not found in row ${i}`)
      }
      const extra = rowColumns.find(col => !firstColSet.has(col))
      if (extra) {
        throw new Error(`Inconsistent data, unexpected column "${extra}" found in row ${i}`)
      }
    }
    columns = firstColumns
  }
  return {
    numRows: data.length,
    columns,
    scan({ columns: scanColumns, where, limit, offset, signal }) {
      // Only apply offset and limit if no where clause
      const start = !where ? offset ?? 0 : 0
      const end = !where && limit !== undefined ? start + limit : data.length
      const rowColumns = scanColumns ?? columns
      return {
        async *rows() {
          for (let i = start; i < end && i < data.length; i++) {
            if (signal?.aborted) break
            yield asyncRow(data[i], rowColumns)
          }
        },
        appliedWhere: false,
        appliedLimitOffset: !where,
      }
    },
  }
}

/**
 * Wraps a data source, memoizing accessed cells. The row cache is a WeakMap
 * keyed on row identity so entries are collectible once the row is unreachable,
 * keeping a streaming scan O(1) instead of O(rows).
 * @param {AsyncDataSource} source
 * @returns {AsyncDataSource}
 */
export function cachedDataSource(source) {
  /** @type {WeakMap<object, Map<string, Promise<SqlPrimitive>>>} */
  const cache = new WeakMap()
  return {
    ...source,
    scan(options) {
      // Does re-run the scan, but cache avoids re-computing expensive async cells
      // TODO: check cache first to avoid re-scanning when possible
      const { rows, appliedWhere, appliedLimitOffset } = source.scan(options)

      // Applied where clause changes which rows are returned so can't be cached
      if (appliedWhere && options.where) {
        return { rows, appliedWhere, appliedLimitOffset }
      }

      return {
        async *rows() {
          for await (const row of rows()) {
            if (options.signal?.aborted) break
            const anchor = row.resolved ?? row
            let rowCache = cache.get(anchor)
            if (!rowCache) {
              rowCache = new Map()
              cache.set(anchor, rowCache)
            }
            /** @type {AsyncCells} */
            const cells = {}
            for (const key of row.columns) {
              const cell = row.cells[key]
              cells[key] = () => {
                let value = rowCache.get(key)
                if (!value) {
                  value = cell()
                  rowCache.set(key, value)
                }
                return value
              }
            }
            // Preserve resolved so downstream fast paths still apply.
            yield { columns: row.columns, cells, resolved: row.resolved }
          }
        },
        appliedWhere,
        appliedLimitOffset,
      }
    },
    // scanColumn passes through from ...source unwrapped: column values are
    // already materialized, so caching them only retained memory unboundedly.
  }
}
