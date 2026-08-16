import { selectBatch, selectedRowCount, valueAt } from '../backend/batch.js'

/**
 * @import { AsyncBatch, ColumnVector, CompiledBatchExpression, RelationSchema, RowSelection } from '../internalTypes.js'
 */

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
 * Evaluates a predicate once per batch and composes the resulting selection
 * without reading or copying payload columns.
 *
 * @param {AsyncIterable<AsyncBatch>} batches
 * @param {CompiledBatchExpression} expression
 * @param {AbortSignal} [signal]
 * @yields {AsyncBatch}
 */
export async function* filterBatches(batches, expression, signal) {
  for await (const batch of batches) {
    signal?.throwIfAborted()
    const result = expression.evaluate({ batch, selection: batch.selection, signal })
    const predicate = result instanceof Promise ? await result : result
    const rowCount = selectedRowCount(batch.selection)
    const { selection, selectedCount } = predicateSelection(predicate, rowCount)
    if (selectedCount === 0) continue
    if (selectedCount === rowCount) {
      yield batch
    } else {
      yield selectBatch(batch, selection)
    }
  }
  signal?.throwIfAborted()
}

/**
 * Projects batches by positional column reference without reading or copying
 * their vectors.
 *
 * @param {AsyncIterable<AsyncBatch>} batches
 * @param {RelationSchema} schema
 * @param {number[]} columnIndices
 * @yields {AsyncBatch}
 */
export async function* projectBatches(batches, schema, columnIndices) {
  for await (const batch of batches) {
    yield {
      schema,
      selection: batch.selection,
      columns: columnIndices.map(function selectColumn(columnIndex) {
        return batch.columns[columnIndex]
      }),
    }
  }
}

/**
 * @param {ColumnVector} predicate
 * @param {number} rowCount
 * @returns {{ selection: Extract<RowSelection, { type: 'bitmap' }>, selectedCount: number }}
 */
function predicateSelection(predicate, rowCount) {
  if (predicate.length !== rowCount) {
    throw new Error(`Predicate returned ${predicate.length} rows, expected ${rowCount}`)
  }
  const values = new Uint8Array(rowCount)
  let selectedCount = 0
  for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
    if (!valueAt(predicate, rowIndex)) continue
    values[rowIndex] = 1
    selectedCount++
  }
  return {
    selection: { type: 'bitmap', values, length: rowCount },
    selectedCount,
  }
}
