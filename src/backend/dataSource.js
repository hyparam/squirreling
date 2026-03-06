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
  return { columns, cells }
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
    columns = Object.keys(data[0])
    // Check first 1000 rows for consistent columns
    for (let i = 1; i < data.length && i < 1000; i++) {
      const rowColumns = Object.keys(data[i])
      const missing = columns.find(col => !rowColumns.includes(col))
      if (missing) {
        throw new Error(`Inconsistent data, column "${missing}" not found in row ${i}`)
      }
      const extra = rowColumns.find(col => !columns.includes(col))
      if (extra) {
        throw new Error(`Inconsistent data, unexpected column "${extra}" found in row ${i}`)
      }
    }
  }
  return {
    numRows: data.length,
    columns,
    scan({ columns: scanColumns, where, limit, offset, signal }) {
      // Only apply offset and limit if no where clause
      const start = !where ? offset ?? 0 : 0
      const end = !where && limit !== undefined ? start + limit : data.length
      return {
        rows: (async function* () {
          for (let i = start; i < end && i < data.length; i++) {
            if (signal?.aborted) break
            yield asyncRow(data[i], scanColumns ?? columns)
          }
        })(),
        appliedWhere: false,
        appliedLimitOffset: !where,
      }
    },
  }
}

/**
 * Wraps a data source that caches all accessed rows in memory
 * @param {AsyncDataSource} source
 * @returns {AsyncDataSource}
 */
export function cachedDataSource(source) {
  /** @type {Map<string, Promise<SqlPrimitive>>} */
  const cache = new Map()
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

      // Adjust index when source applied offset so cache keys match original rows
      const indexOffset = appliedLimitOffset && options.offset ? options.offset : 0

      return {
        rows: (async function* () {
          let index = 0
          for await (const row of rows) {
            if (options.signal?.aborted) break
            const rowIndex = index + indexOffset
            /** @type {AsyncCells} */
            const cells = {}
            for (const key of row.columns) {
              const cell = row.cells[key]
              // Wrap the cell to cache accesses
              cells[key] = () => {
                const cacheKey = `${rowIndex}:${key}`
                let value = cache.get(cacheKey)
                if (!value) {
                  value = cell()
                  cache.set(cacheKey, value)
                }
                return value
              }
            }
            yield { columns: row.columns, cells }
            index++
          }
        })(),
        appliedWhere,
        appliedLimitOffset,
      }
    },
  }
}
