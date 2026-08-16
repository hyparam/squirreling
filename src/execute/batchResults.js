/**
 * @import { InternalBatchResults } from '../internalTypes.js'
 * @import { QueryResults } from '../types.js'
 */

/** @type {WeakMap<QueryResults, InternalBatchResults>} */
const internalBatches = new WeakMap()

/**
 * Associates a public row result with its private batch execution path.
 *
 * @param {QueryResults} results
 * @param {InternalBatchResults} batchResults
 * @returns {QueryResults}
 */
export function registerBatchResults(results, batchResults) {
  internalBatches.set(results, batchResults)
  return results
}

/**
 * Returns the private batch execution path for a result, when available.
 *
 * @param {QueryResults} results
 * @returns {InternalBatchResults | undefined}
 */
export function batchResultsFor(results) {
  return internalBatches.get(results)
}
