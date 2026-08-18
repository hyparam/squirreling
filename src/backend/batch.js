/**
 * @import { AsyncBatch, ColumnResult, ColumnVector, ReadBatchColumnOptions, RowSelection, SqlPrimitive } from '../types.js'
 */

/** @type {WeakMap<AsyncBatch, Map<number, Map<RowSelection, { resolved?: ColumnVector, pending: Map<AbortSignal | undefined, Promise<ColumnVector>> }>>>} */
const batchCache = new WeakMap()

/**
 * Returns the number of selected rows.
 *
 * @param {RowSelection} selection
 * @returns {number}
 */
export function selectedRowCount(selection) {
  if (selection.type === 'all') return selection.length
  if (selection.type === 'range') return selection.end - selection.start
  return selection.indices.length
}

/**
 * Recognizes promises and thenables without relying on realm identity.
 *
 * @template T
 * @param {T | PromiseLike<T>} value
 * @returns {value is PromiseLike<T>}
 */
export function isPromiseLike(value) {
  if (value === null || typeof value !== 'object' && typeof value !== 'function') return false
  return typeof Reflect.get(value, 'then') === 'function'
}

/**
 * Avoids a promise boundary when every column is already loaded synchronously.
 *
 * @param {ColumnResult[]} results
 * @returns {ColumnVector[] | Promise<ColumnVector[]>}
 */
export function resolveColumnResults(results) {
  if (results.some(isPromiseLike)) return Promise.all(results)
  /** @type {ColumnVector[]} */
  const vectors = []
  for (const result of results) {
    if (isPromiseLike(result)) throw new Error('Unexpected asynchronous column result')
    vectors.push(result)
  }
  return vectors
}

/**
 * Creates base-aligned ordinal values for the rows in a selection.
 *
 * @param {RowSelection} selection
 * @returns {ColumnVector}
 */
export function selectionOrdinals(selection) {
  const values = new Uint32Array(selection.length)
  const length = selectedRowCount(selection)
  for (let ordinal = 0; ordinal < length; ordinal++) {
    values[selectionIndexAt(selection, ordinal)] = ordinal
  }
  return { type: 'typed', values, length: selection.length }
}

/**
 * Composes a selection over an already-selected domain with the selection
 * that produced that domain.
 *
 * @param {RowSelection} outer - selection over the original base domain
 * @param {RowSelection} inner - selection over the rows selected by `outer`
 * @returns {RowSelection}
 */
export function composeSelections(outer, inner) {
  const outerCount = selectedRowCount(outer)
  if (inner.length !== outerCount) {
    throw new Error(`Cannot compose selection of length ${inner.length} over ${outerCount} rows`)
  }
  if (inner.type === 'all') return outer
  if (outer.type === 'all') return { ...inner, length: outer.length }

  if (outer.type === 'range' && inner.type === 'range') {
    return {
      type: 'range',
      start: outer.start + inner.start,
      end: outer.start + inner.end,
      length: outer.length,
    }
  }

  const indices = new Uint32Array(selectedRowCount(inner))
  for (let i = 0; i < indices.length; i++) {
    indices[i] = selectionIndexAt(outer, selectionIndexAt(inner, i))
  }
  return { type: 'indices', indices, length: outer.length }
}

/**
 * Reads one logical value from a vector.
 *
 * @param {ColumnVector} vector
 * @param {number} index
 * @returns {SqlPrimitive}
 */
export function valueAt(vector, index) {
  if (!Number.isInteger(index) || index < 0 || index >= vector.length) {
    throw new RangeError(`Column index ${index} is outside vector length ${vector.length}`)
  }
  if (vector.type === 'values') return vector.values[index]
  if (vector.type === 'typed') {
    if (vector.validity && vector.validity[index] === 0) return null
    return vector.values[index]
  }
  if (vector.type === 'constant') return vector.value
  return valueAt(vector.source, selectionIndexAt(vector.selection, index))
}

/**
 * Creates a zero-copy view of a vector through a row selection.
 *
 * @param {ColumnVector} vector
 * @param {RowSelection} selection
 * @returns {ColumnVector}
 */
export function selectVector(vector, selection) {
  if (selection.length !== vector.length) {
    throw new Error(`Cannot select ${selection.length} rows from vector length ${vector.length}`)
  }
  if (selection.type === 'all') return vector
  if (vector.type === 'selected') {
    const composed = composeSelections(vector.selection, selection)
    return {
      type: 'selected',
      source: vector.source,
      selection: composed,
      length: selectedRowCount(composed),
    }
  }
  return {
    type: 'selected',
    source: vector,
    selection,
    length: selectedRowCount(selection),
  }
}

/**
 * Resolves one batch column for an effective selection.
 *
 * @param {ReadBatchColumnOptions} options
 * @returns {ColumnResult}
 */
export function readBatchColumn({ batch, columnIndex, selection = batch.selection, signal }) {
  const column = batch.columns[columnIndex]
  if (!column) throw new RangeError(`Column index ${columnIndex} is outside batch columns`)
  if (selection.length !== batch.selection.length) {
    throw new Error(`Selection length ${selection.length} does not match batch length ${batch.selection.length}`)
  }
  if (!('read' in column)) {
    return selectVector(column, selection)
  }

  let batchResults = batchCache.get(batch)
  if (!batchResults) {
    batchResults = new Map()
    batchCache.set(batch, batchResults)
  }
  let columnResults = batchResults.get(columnIndex)
  if (!columnResults) {
    columnResults = new Map()
    batchResults.set(columnIndex, columnResults)
  }
  let cache = columnResults.get(selection)
  if (!cache) {
    cache = { pending: new Map() }
    columnResults.set(selection, cache)
  }
  const pending = cache.pending.get(signal)
  if (pending) return pending
  if (cache.resolved) return cache.resolved

  const result = column.read({
    batch: column.input ?? batch,
    selection,
    signal,
    rowOffset: column.rowOffset,
    rowOrdinals: column.rowOrdinals
      ? selectVector(column.rowOrdinals, selection)
      : undefined,
  })
  const validated = validateColumnResult(result, selectedRowCount(selection))
  if (isPromiseLike(validated)) {
    const settled = Promise.resolve(validated).then(function cacheResolved(vector) {
      cache.pending.delete(signal)
      cache.resolved ??= vector
      return vector
    }, function evictRejected(error) {
      cache.pending.delete(signal)
      throw error
    })
    cache.pending.set(signal, settled)
    return settled
  }
  cache.resolved = validated
  return validated
}

/**
 * Selects rows from a batch without reading or copying its columns.
 *
 * @param {AsyncBatch} batch
 * @param {RowSelection} selection - selection over the batch's current rows
 * @returns {AsyncBatch}
 */
export function selectBatch(batch, selection) {
  const composed = composeSelections(batch.selection, selection)
  return {
    selection: composed,
    columns: batch.columns,
  }
}

/**
 * Returns a base-domain row index for a logical selected-row index.
 *
 * @param {RowSelection} selection
 * @param {number} index
 * @returns {number}
 */
function selectionIndexAt(selection, index) {
  const count = selectedRowCount(selection)
  if (!Number.isInteger(index) || index < 0 || index >= count) {
    throw new RangeError(`Selection index ${index} is outside selected length ${count}`)
  }
  if (selection.type === 'all') return index
  if (selection.type === 'range') return selection.start + index
  return selection.indices[index]
}

/**
 * Validates the resolved vector length without forcing synchronous column
 * implementations through a promise.
 *
 * @param {ColumnResult} result
 * @param {number} expectedLength
 * @returns {ColumnResult}
 */
function validateColumnResult(result, expectedLength) {
  if (isPromiseLike(result)) {
    return Promise.resolve(result).then(function validateResolvedVector(vector) {
      return validateVectorLength(vector, expectedLength)
    })
  }
  return validateVectorLength(result, expectedLength)
}

/**
 * @param {ColumnVector} vector
 * @param {number} expectedLength
 * @returns {ColumnVector}
 */
function validateVectorLength(vector, expectedLength) {
  if (vector.length !== expectedLength) {
    throw new Error(`Column returned ${vector.length} rows, expected ${expectedLength}`)
  }
  return vector
}
