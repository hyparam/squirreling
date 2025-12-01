/**
 * @import { AsyncDataSource, AsyncRow, SqlPrimitive } from '../types.js'
 */


/**
 * Wraps an async generator of plain objects into an AsyncDataSource
 *
 * @param {AsyncGenerator<Record<string, any>>} gen
 * @returns {AsyncDataSource}
 */
export function generatorSource(gen) {
  return {
    async *getRows() {
      for await (const row of gen) {
        yield asyncRow(row)
      }
    },
  }
}

/**
 * Creates an async row accessor that wraps a plain JavaScript object
 *
 * @param {Record<string, any>} obj - the plain object
 * @returns {AsyncRow} a row accessor interface
 */
export function asyncRow(obj) {
  return {
    getCell(name) {
      return obj[name]
    },
    getKeys() {
      return Object.keys(obj)
    },
  }
}

/**
 * Creates an async memory-backed data source from an array of plain objects
 *
 * @param {Record<string, any>[]} data - array of plain objects
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
  /** @type {Map<string, SqlPrimitive>} */
  const cache = new Map()
  return {
    /**
     * @returns {AsyncGenerator<AsyncRow>}
     */
    async *getRows() {
      let index = 0
      for await (const row of source.getRows()) {
        const rowIndex = index
        index++
        yield {
          /**
           * @param {string} name
           * @returns {SqlPrimitive}
           */
          getCell(name) {
            const cacheKey = `${rowIndex}:${name}`
            if (!cache.has(cacheKey)) {
              cache.set(cacheKey, row.getCell(name))
            }
            return cache.get(cacheKey)
          },
          getKeys() {
            return row.getKeys()
          },
        }
      }
    },
  }
}
