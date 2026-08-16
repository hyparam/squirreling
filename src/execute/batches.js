import { selectBatch, selectedRowCount } from '../backend/batch.js'

/**
 * @import { AsyncBatch, RelationSchema } from '../internalTypes.js'
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
