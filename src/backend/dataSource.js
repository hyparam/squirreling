/**
 * @import { AsyncDataSource, AsyncRow } from '../types.js'
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
