/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, SqlPrimitive } from '../types.js'
 */

/**
 * Creates an async row accessor that wraps a plain JavaScript object
 *
 * @param {Record<string, SqlPrimitive>} obj - the plain object
 * @returns {AsyncRow} a row accessor interface
 */
export function asyncRow(obj) {
  /** @type {AsyncCells} */
  const cells = {}
  for (const [key, value] of Object.entries(obj)) {
    cells[key] = () => Promise.resolve(value)
  }
  return { columns: Object.keys(obj), cells }
}

/**
 * Creates an async memory-backed data source from an array of plain objects
 *
 * @param {Record<string, SqlPrimitive>[]} data - array of plain objects
 * @returns {AsyncDataSource} an async data source interface
 */
export function memorySource(data) {
  return {
    numRows: data.length,
    scan({ where, limit, offset, signal }) {
      // Only apply offset and limit if no where clause
      const start = !where ? offset ?? 0 : 0
      const end = !where && limit !== undefined ? start + limit : data.length
      return {
        rows: (async function* () {
          for (let i = start; i < end && i < data.length; i++) {
            if (signal?.aborted) break
            yield asyncRow(data[i])
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
