import { yieldToEventLoop } from './yield.js'

/**
 * @import { AsyncRow } from '../types.js'
 */

// Chunk sizing for foldEvaluatedRows. Dispatch starts small so one chunk of
// unexpectedly fat values cannot overshoot far, then adapts to the observed
// result sizes: small values grow the chunk toward MAX_CHUNK_ROWS so async
// cells still overlap, while large values (e.g. long string group keys)
// shrink it so in-flight results stay near CHUNK_BYTE_BUDGET instead of
// scaling with row count.
const INITIAL_CHUNK_ROWS = 64
const MAX_CHUNK_ROWS = 4000
const CHUNK_BYTE_BUDGET = 16 * 1024 * 1024

/**
 * Approximate retained bytes of an evaluated value. Strings dominate the
 * workloads where size matters; everything else counts as a small constant.
 *
 * @param {unknown} value
 * @returns {number}
 */
function valueBytes(value) {
  if (typeof value === 'string') return value.length * 2
  if (Array.isArray(value)) {
    let bytes = 0
    for (const item of value) bytes += valueBytes(item)
    return bytes
  }
  return 16
}

/**
 * Evaluates a value for every row and folds each result in row order, holding
 * at most one adaptively sized chunk of evaluated values. Rows in a chunk are
 * dispatched together so async cells overlap, and the chunk boundary yields
 * to the event loop so aborts can fire. Chunks are bounded by bytes, not row
 * count: a fixed 4000-row chunk of ~90KB string keys would hold hundreds of
 * megabytes of results at once.
 *
 * @template T
 * @param {Object} options
 * @param {AsyncRow[]} options.rows
 * @param {(row: AsyncRow, index: number) => Promise<T>} options.evaluate
 * @param {(value: T, index: number) => void} options.fold
 * @param {AbortSignal} [options.signal]
 * @returns {Promise<void>}
 */
export async function foldEvaluatedRows({ rows, evaluate, fold, signal }) {
  let chunkSize = INITIAL_CHUNK_ROWS
  /** @type {Promise<T>[]} */
  const pending = []
  for (let start = 0; start < rows.length;) {
    if (start > 0) {
      await yieldToEventLoop()
      signal?.throwIfAborted()
    }
    const end = Math.min(start + chunkSize, rows.length)
    pending.length = end - start
    for (let i = start; i < end; i++) {
      pending[i - start] = evaluate(rows[i], i)
    }
    const values = await Promise.all(pending)
    let bytes = 0
    for (let j = 0; j < values.length; j++) {
      bytes += valueBytes(values[j])
      fold(values[j], start + j)
    }
    start = end
    const bytesPerRow = Math.max(1, bytes / values.length)
    chunkSize = Math.min(MAX_CHUNK_ROWS, Math.max(1, Math.floor(CHUNK_BYTE_BUDGET / bytesPerRow)))
  }
}
