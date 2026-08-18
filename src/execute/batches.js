import { isPromiseLike, readBatchColumn, resolveColumnResults, selectBatch, selectedRowCount, selectionOrdinals, valueAt } from '../backend/batch.js'
import { keyify } from './utils.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { BatchProjection, CompiledBatchExpression } from '../internalTypes.js'
 * @import { AsyncBatch, BatchColumn, ColumnVector, ReadColumn, RowSelection, SqlPrimitive } from '../types.js'
 */

const INITIAL_FILTER_WINDOW_ROWS = 256
const MAX_FILTER_WINDOW_ROWS = 16_384

/**
 * Applies LIMIT/OFFSET by composing batch selections and stops consuming the
 * source as soon as the requested range is complete.
 *
 * @param {AsyncIterable<AsyncBatch>} batches
 * @param {number} [limit]
 * @param {number} [offset]
 * @param {AbortSignal} [signal]
 * @yields {AsyncBatch}
 */
export async function* limitBatches(batches, limit = Infinity, offset = 0, signal) {
  if (limit <= 0) return
  let skipped = 0
  let yielded = 0
  for await (const batch of batches) {
    signal?.throwIfAborted()
    const rowCount = selectedRowCount(batch.selection)
    if (skipped + rowCount <= offset) {
      skipped += rowCount
      continue
    }

    const start = Math.max(0, offset - skipped)
    const remaining = limit - yielded
    const end = Math.min(rowCount, start + remaining)
    if (end > start) {
      if (start === 0 && end === rowCount) {
        yield batch
      } else {
        yield selectBatch(batch, {
          type: 'range',
          start,
          end,
          length: rowCount,
        })
      }
      yielded += end - start
    }
    skipped += rowCount
    if (yielded >= limit) return
  }
  signal?.throwIfAborted()
}

/**
 * Evaluates a predicate in adaptive windows when a downstream limit can stop
 * early, then composes selections without reading or copying payload columns.
 *
 * @param {AsyncIterable<AsyncBatch>} batches
 * @param {CompiledBatchExpression} expression
 * @param {AbortSignal} [signal]
 * @param {number} [targetRows] - Matches needed by a downstream limit.
 * @yields {AsyncBatch}
 */
export async function* filterBatches(batches, expression, signal, targetRows) {
  let rowOffset = 0
  let matchedRows = 0
  let windowRows = targetRows === undefined
    ? Infinity
    : Math.min(MAX_FILTER_WINDOW_ROWS, Math.max(INITIAL_FILTER_WINDOW_ROWS, targetRows))
  for await (const batch of batches) {
    signal?.throwIfAborted()
    const rowCount = selectedRowCount(batch.selection)
    let start = 0
    while (start < rowCount) {
      const remainingRows = rowCount - start
      const requestedRows = targetRows !== undefined && matchedRows < targetRows
        ? windowRows
        : remainingRows
      const end = Math.min(rowCount, start + requestedRows)
      const windowBatch = start === 0 && end === rowCount
        ? batch
        : selectBatch(batch, {
          type: 'range',
          start,
          end,
          length: rowCount,
        })
      const windowRowCount = end - start
      const result = expression.evaluate({
        batch: windowBatch,
        selection: windowBatch.selection,
        signal,
        rowOffset: rowOffset + start,
      })
      const predicate = isPromiseLike(result) ? await result : result
      const { selection, selectedCount } = predicateSelection(predicate, windowRowCount)
      start = end
      matchedRows += selectedCount

      if (targetRows !== undefined && matchedRows < targetRows) {
        if (selectedCount === 0) {
          windowRows = Math.min(MAX_FILTER_WINDOW_ROWS, windowRows * 2)
        } else {
          const remainingMatches = targetRows - matchedRows
          const estimatedRows = Math.ceil(remainingMatches * windowRowCount / selectedCount * 1.25)
          windowRows = Math.min(MAX_FILTER_WINDOW_ROWS, Math.max(INITIAL_FILTER_WINDOW_ROWS, estimatedRows))
        }
      }

      if (selectedCount === 0) continue
      if (selectedCount === windowRowCount) {
        yield windowBatch
      } else {
        yield selectBatch(windowBatch, selection)
      }
    }
    rowOffset += rowCount
  }
  signal?.throwIfAborted()
}

/**
 * Removes duplicate rows while retaining zero-copy selections over the input
 * batches. DISTINCT requires every output column as both a key and payload, so
 * resolving all batch columns here does not introduce eager payload work.
 *
 * @param {AsyncIterable<AsyncBatch>} batches
 * @param {AbortSignal} [signal]
 * @yields {AsyncBatch}
 */
export async function* distinctBatches(batches, signal) {
  const seen = new Set()
  for await (const batch of batches) {
    signal?.throwIfAborted()
    const results = batch.columns.map(function readColumn(_column, columnIndex) {
      return readBatchColumn({ batch, columnIndex, signal })
    })
    const resolved = resolveColumnResults(results)
    const vectors = isPromiseLike(resolved) ? await resolved : resolved
    const rowCount = selectedRowCount(batch.selection)
    const indices = new Uint32Array(rowCount)
    /** @type {SqlPrimitive[]} */
    const row = new Array(vectors.length)
    let selectedCount = 0

    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      if (signal && rowIndex > 0 && rowIndex % 4000 === 0) {
        await yieldToEventLoop()
        signal.throwIfAborted()
      }
      for (let columnIndex = 0; columnIndex < vectors.length; columnIndex++) {
        row[columnIndex] = valueAt(vectors[columnIndex], rowIndex)
      }
      const key = keyify(...row)
      if (seen.has(key)) continue
      seen.add(key)
      indices[selectedCount++] = rowIndex
    }

    if (selectedCount === 0) continue
    if (selectedCount === rowCount) {
      yield batch
    } else {
      yield selectBatch(batch, {
        type: 'indices',
        indices: indices.subarray(0, selectedCount),
        length: rowCount,
      })
    }
  }
  signal?.throwIfAborted()
}

/**
 * Projects batches into direct, constant, or lazily computed columns. A
 * computed column retains the input batch that owns its dependency indices,
 * so output columns remain aligned without hidden dependency columns.
 *
 * @param {AsyncIterable<AsyncBatch>} batches
 * @param {readonly BatchProjection[]} projections
 * @yields {AsyncBatch}
 */
export async function* projectExpressionBatches(batches, projections) {
  let rowOffset = 0
  for await (const batch of batches) {
    const currentRowOffset = rowOffset
    rowOffset += selectedRowCount(batch.selection)
    /** @type {ColumnVector | undefined} */
    let rowOrdinals
    yield {
      selection: batch.selection,
      columns: projections.map(function projectColumn(projection) {
        /** @type {BatchColumn} */
        let column
        if (projection.type === 'column') {
          column = projectedBatchColumn(batch, projection.columnIndex)
        } else if (projection.type === 'constant') {
          column = {
            type: 'constant',
            value: projection.value,
            length: batch.selection.length,
          }
        } else {
          rowOrdinals ??= selectionOrdinals(batch.selection)
          column = {
            read: projection.expression.evaluate,
            input: batch,
            rowOffset: currentRowOffset,
            rowOrdinals,
          }
        }
        return column
      }),
    }
  }
}

/**
 * Preserves the source batch's deferred-read cache through projection.
 *
 * @param {AsyncBatch} batch
 * @param {number} columnIndex
 * @returns {BatchColumn}
 */
function projectedBatchColumn(batch, columnIndex) {
  const column = batch.columns[columnIndex]
  if (!('read' in column)) return column
  /** @type {ReadColumn} */
  function readProjectedColumn({ selection, signal }) {
    return readBatchColumn({ batch, columnIndex, selection, signal })
  }
  return { read: readProjectedColumn }
}

/**
 * @param {ColumnVector} predicate
 * @param {number} rowCount
 * @returns {{ selection: Extract<RowSelection, { type: 'indices' }>, selectedCount: number }}
 */
function predicateSelection(predicate, rowCount) {
  if (predicate.length !== rowCount) {
    throw new Error(`Predicate returned ${predicate.length} rows, expected ${rowCount}`)
  }
  const indices = new Uint32Array(rowCount)
  let selectedCount = 0
  for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
    if (!valueAt(predicate, rowIndex)) continue
    indices[selectedCount++] = rowIndex
  }
  return {
    selection: { type: 'indices', indices: indices.subarray(0, selectedCount), length: rowCount },
    selectedCount,
  }
}
