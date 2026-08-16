import { batchesToRows } from '../backend/batchAdapters.js'
import { bindQuerySignal } from './utils.js'

/**
 * @import { AsyncBatch, QueryResults } from '../types.js'
 */

/**
 * Creates query results backed by batches.
 *
 * @param {object} options
 * @param {string[]} options.columns
 * @param {number} [options.numRows]
 * @param {number} [options.maxRows]
 * @param {() => AsyncIterable<AsyncBatch>} options.batches
 * @param {AbortSignal} [options.signal]
 * @returns {QueryResults}
 */
export function batchResult({ batches: readBatches, signal, ...metadata }) {
  const results = {
    ...metadata,
    batches: readBatches,
    rows() {
      return batchesToRows(readBatches(), metadata.columns, signal)
    },
  }
  return bindQuerySignal(results, signal)
}
