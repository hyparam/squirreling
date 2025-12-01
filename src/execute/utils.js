/**
 * Collects and materialize all results from an async row generator into an array
 *
 * @import {AsyncRow, SqlPrimitive} from '../types.js'
 * @param {AsyncGenerator<AsyncRow>} asyncRows
 * @returns {Promise<Record<string, SqlPrimitive>[]>} array of all yielded values
 */
export async function collect(asyncRows) {
  /** @type {Record<string, SqlPrimitive>[]} */
  const results = []
  for await (const asyncRow of asyncRows) {
    /** @type {Record<string, SqlPrimitive>} */
    const item = {}
    for (const [key, cell] of Object.entries(asyncRow)) {
      item[key] = await cell()
    }
    results.push(item)
  }
  return results
}
