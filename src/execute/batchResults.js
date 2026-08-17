import { batchesToRows } from '../backend/batchAdapters.js'

/**
 * @import { AsyncBatch, InternalBatchResults } from '../internalTypes.js'
 * @import { QueryResults } from '../types.js'
 */

/** @type {WeakMap<QueryResults, InternalBatchResults>} */
const internalBatches = new WeakMap()

/**
 * Creates a public row result backed by private batches.
 *
 * @param {object} options
 * @param {string[]} options.columns
 * @param {number} [options.numRows]
 * @param {number} [options.maxRows]
 * @param {() => AsyncIterable<AsyncBatch>} options.batches
 * @param {AbortSignal} [options.signal]
 * @returns {QueryResults}
 */
export function batchResult({ batches, signal, ...metadata }) {
  const results = {
    ...metadata,
    rows() {
      return batchesToRows(batches(), signal)
    },
  }
  internalBatches.set(results, { columns: metadata.columns, batches, signal })
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
