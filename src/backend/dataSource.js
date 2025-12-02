/**
 * @import { AsyncDataSource, AsyncRow, SqlPrimitive } from '../types.js'
 */


/**
 * Wraps an async generator of plain objects into an AsyncDataSource
 *
 * @param {AsyncGenerator<AsyncRow>} gen
 * @returns {AsyncDataSource}
 */
export function generatorSource(gen) {
  return {
    async *getRows() {
      yield* gen
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
  /** @type {AsyncRow} */
  const row = {}
  for (const [key, value] of Object.entries(obj)) {
    row[key] = () => Promise.resolve(value)
  }
  return row
}

/**
 * Creates an async memory-backed data source from an array of plain objects
 *
 * @param {Record<string, SqlPrimitive>[]} data - array of plain objects
 * @returns {AsyncDataSource} an async data source interface
 */
export function memorySource(data) {
  return {
    async *getRows() {
      for (const item of data) {
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
     * @yields {AsyncRow}
     */
    async *getRows() {
      let index = 0
      for await (const row of source.getRows()) {
        const rowIndex = index
        /** @type {AsyncRow} */
        const out = {}
        for (const [key, cell] of Object.entries(row)) {
          // Wrap the cell to cache accesses
          out[key] = () => {
            const cacheKey = `${rowIndex}:${key}`
            let value = cache.get(cacheKey)
            if (!value) {
              value = cell()
              cache.set(cacheKey, value)
            }
            return value
          }
        }
        yield out
        index++
      }
    },
  }
}
