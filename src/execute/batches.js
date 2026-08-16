import { readBatchColumn, selectBatch, selectedRowCount, valueAt } from '../backend/batch.js'
import { keyify } from './utils.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { AsyncBatch, BatchColumn, BatchProjection, ColumnResult, ColumnVector, CompiledBatchExpression, RelationSchema, RowSelection } from '../internalTypes.js'
 * @import { SqlPrimitive } from '../types.js'
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
    const resolved = resolveVectors(results)
    const vectors = resolved instanceof Promise ? await resolved : resolved
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
 * so output columns remain schema-aligned without hidden dependency columns.
 *
 * @param {AsyncIterable<AsyncBatch>} batches
 * @param {RelationSchema} schema
 * @param {readonly BatchProjection[]} projections
 * @yields {AsyncBatch}
 */
export async function* projectExpressionBatches(batches, schema, projections) {
  for await (const batch of batches) {
    yield {
      schema,
      selection: batch.selection,
      columns: projections.map(function projectColumn(projection) {
        /** @type {BatchColumn} */
        let column
        if (projection.type === 'column') {
          column = batch.columns[projection.columnIndex]
        } else if (projection.type === 'constant') {
          column = {
            type: 'loaded',
            vector: {
              type: 'constant',
              value: projection.value,
              length: batch.selection.length,
            },
          }
        } else {
          column = {
            type: 'computed',
            input: batch,
            dependencies: projection.expression.dependencies,
            evaluate: projection.expression.evaluate,
          }
        }
        return column
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

/**
 * @param {ColumnResult[]} results
 * @returns {ColumnVector[] | Promise<ColumnVector[]>}
 */
function resolveVectors(results) {
  if (results.some(function isPromise(result) { return result instanceof Promise })) {
    return Promise.all(results)
  }
  /** @type {ColumnVector[]} */
  const vectors = []
  for (const result of results) {
    if (result instanceof Promise) throw new Error('Unexpected asynchronous column result')
    vectors.push(result)
  }
  return vectors
}
