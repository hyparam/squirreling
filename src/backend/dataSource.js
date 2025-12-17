/**
 * @import { AsyncCell, AsyncCells, AsyncDataSource, AsyncRow, ScanOptions, SqlPrimitive } from '../types.js'
 */

/**
 * Wraps an async generator of plain objects into an AsyncDataSource
 *
 * @param {AsyncGenerator<AsyncRow>} gen
 * @returns {AsyncDataSource}
 */
export function generatorSource(gen) {
  return {
    async *scan({ signal }) {
      for await (const row of gen) {
        if (signal?.aborted) break
        yield row
      }
    },
  }
}

/**
 * Creates an async row accessor that wraps a plain JavaScript object
 *
 * @param {Record<string, SqlPrimitive>} obj - the plain object
 * @returns {AsyncRow} a row accessor interface
 */
function asyncRow(obj) {
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
    async *scan({ signal }) {
      for (const item of data) {
        if (signal?.aborted) break
        yield asyncRow(item)
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
    /**
     * @param {ScanOptions} options
     * @yields {AsyncRow}
     */
    async *scan(options) {
      const { signal } = options
      let index = 0
      for await (const row of source.scan(options)) {
        if (signal?.aborted) break
        const rowIndex = index
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
    },
  }
}
