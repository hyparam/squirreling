/**
 * @import { AsyncDataSource, RowSource } from '../types.js'
 */

/**
 * Creates a row accessor that wraps a plain JavaScript object
 *
 * @param {Record<string, any>} obj - the plain object
 * @returns {RowSource} a row accessor interface
 */
export function createRowAccessor(obj) {
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
export function createAsyncMemorySource(data) {
  return {
    async *getRows() {
      for (const item of data) {
        yield createRowAccessor(item)
      }
    },
  }
}
