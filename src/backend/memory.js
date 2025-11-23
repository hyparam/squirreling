/**
 * @import { DataSource, RowSource } from '../types.js'
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
 * Creates a memory-backed data source from an array of plain objects
 *
 * @param {Record<string, any>[]} data - array of plain objects
 * @returns {DataSource} a data source interface
 */
export function createMemorySource(data) {
  return {
    getNumRows() {
      return data.length
    },
    getRow(index) {
      return createRowAccessor(data[index])
    },
  }
}
