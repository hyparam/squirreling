/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, BatchScanOptions, ColumnBatch, SqlPrimitive } from '../types.js'
 */

const DEFAULT_BATCH_SIZE = 1024

/**
 * Adapts a stream of AsyncRow into a stream of ColumnBatch by buffering rows
 * and materializing each requested column into a typed-array-friendly array.
 *
 * @param {AsyncIterable<AsyncRow>} rows
 * @param {string[]} columns - column names to materialize from each row
 * @param {Object} [options]
 * @param {number} [options.batchSize] - target rows per batch (default 1024)
 * @param {number} [options.rowStart] - sequential index of the first row (default 0)
 * @param {AbortSignal} [options.signal]
 * @returns {AsyncIterable<ColumnBatch>}
 */
export async function* adaptRowsToBatches(rows, columns, options) {
  const batchSize = options?.batchSize ?? DEFAULT_BATCH_SIZE
  const signal = options?.signal
  let rowStart = options?.rowStart ?? 0
  /** @type {Record<string, SqlPrimitive[]>} */
  let buffers = makeBuffers(columns)
  let count = 0
  for await (const row of rows) {
    if (signal?.aborted) break
    for (const col of columns) {
      if (row.resolved && col in row.resolved) {
        buffers[col][count] = row.resolved[col]
      } else {
        buffers[col][count] = await row.cells[col]()
      }
    }
    count++
    if (count >= batchSize) {
      yield { rowStart, rowCount: count, columns: buffers }
      rowStart += count
      count = 0
      buffers = makeBuffers(columns)
    }
  }
  if (count > 0) {
    yield { rowStart, rowCount: count, columns: trimBuffers(buffers, columns, count) }
  }
}

/**
 * Adapts a stream of ColumnBatch into a stream of AsyncRow. Each yielded row
 * has resolved values prefilled so consumers can skip the AsyncCell await.
 *
 * @param {AsyncIterable<ColumnBatch>} batches
 * @returns {AsyncIterable<AsyncRow>}
 */
export async function* adaptBatchesToRows(batches) {
  for await (const batch of batches) {
    const cols = Object.keys(batch.columns)
    for (let i = 0; i < batch.rowCount; i++) {
      /** @type {Record<string, SqlPrimitive>} */
      const resolved = {}
      /** @type {AsyncCells} */
      const cells = {}
      for (const col of cols) {
        const value = batch.columns[col][i]
        resolved[col] = value
        cells[col] = () => Promise.resolve(value)
      }
      yield { columns: cols, cells, resolved }
    }
  }
}

/**
 * Returns batches from a data source, using its native scanBatches when
 * available and otherwise falling back to scan() + adaptRowsToBatches.
 * This is the helper future batch-mode operators should call.
 *
 * @param {AsyncDataSource} source
 * @param {BatchScanOptions} [options]
 * @returns {AsyncIterable<ColumnBatch>}
 */
export function scanBatches(source, options) {
  if (source.scanBatches) {
    return source.scanBatches(options ?? {})
  }
  const cols = options?.columns ?? source.columns
  const result = source.scan({
    columns: options?.columns,
    signal: options?.signal,
  })
  return adaptRowsToBatches(result.rows(), cols, {
    batchSize: options?.batchSize,
    signal: options?.signal,
  })
}

/**
 * @param {string[]} columns
 * @returns {Record<string, SqlPrimitive[]>}
 */
function makeBuffers(columns) {
  /** @type {Record<string, SqlPrimitive[]>} */
  const buffers = {}
  for (const col of columns) buffers[col] = []
  return buffers
}

/**
 * @param {Record<string, SqlPrimitive[]>} buffers
 * @param {string[]} columns
 * @param {number} count
 * @returns {Record<string, SqlPrimitive[]>}
 */
function trimBuffers(buffers, columns, count) {
  /** @type {Record<string, SqlPrimitive[]>} */
  const trimmed = {}
  for (const col of columns) trimmed[col] = buffers[col].slice(0, count)
  return trimmed
}
