/**
 * @import { AsyncBatch, ColumnResult, ColumnVector, ReadBatchColumnOptions, RowSelection } from '../internalTypes.js'
 * @import { SqlPrimitive } from '../types.js'
 */

/** @type {WeakMap<AsyncBatch, Map<number, Map<RowSelection, { resolved?: ColumnVector, pending: Map<AbortSignal | undefined, Promise<ColumnVector>> }>>>} */
const batchCache = new WeakMap()

/** @type {WeakMap<RowSelection, Uint32Array | number[]>} */
const selectionCache = new WeakMap()

/**
 * Returns the number of selected rows.
 *
 * @param {RowSelection} selection
 * @returns {number}
 */
export function selectedRowCount(selection) {
  if (selection.type === 'all') return selection.length
  if (selection.type === 'range') return selection.end - selection.start
  if (selection.type === 'ranges') {
    const ends = endsForRanges(selection)
    return ends.length === 0 ? 0 : ends[ends.length - 1]
  }
  if (selection.type === 'indices') return selection.indices.length
  return indicesForBitmap(selection).length
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

  if (outer.type === 'range' && inner.type === 'ranges') {
    return {
      type: 'ranges',
      ranges: inner.ranges.map(function shiftRange(range) {
        return { start: outer.start + range.start, end: outer.start + range.end }
      }),
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
  if (!column) throw new RangeError(`Column index ${columnIndex} is outside batch schema`)
  if (selection.length !== batch.selection.length) {
    throw new Error(`Selection length ${selection.length} does not match batch length ${batch.selection.length}`)
  }
  if (column.type === 'loaded') return selectVector(column.vector, selection)

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

  const result = column.type === 'source'
    ? column.read({ selection, signal })
    : column.evaluate({
      batch: column.input,
      selection,
      signal,
      rowOrdinals: selectVector(column.rowOrdinals, selection),
    })
  const validated = validateColumnResult(result, selectedRowCount(selection))
  if (validated instanceof Promise) {
    const settled = validated.then(function cacheResolved(vector) {
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
    schema: batch.schema,
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
  if (selection.type === 'indices') return selection.indices[index]
  if (selection.type === 'ranges') {
    const ends = endsForRanges(selection)
    let low = 0
    let high = ends.length
    while (low < high) {
      const middle = low + Math.floor((high - low) / 2)
      if (index < ends[middle]) high = middle
      else low = middle + 1
    }
    const previousEnd = low === 0 ? 0 : ends[low - 1]
    const range = selection.ranges[low]
    return range.start + index - previousEnd
  } else return indicesForBitmap(selection)[index]
}

/**
 * Returns the cached cumulative selected-row endpoint for each range.
 *
 * @param {Extract<RowSelection, { type: 'ranges' }>} selection
 * @returns {number[]}
 */
function endsForRanges(selection) {
  const cached = selectionCache.get(selection)
  if (Array.isArray(cached)) return cached
  const ends = []
  let count = 0
  for (const range of selection.ranges) {
    count += range.end - range.start
    ends.push(count)
  }
  selectionCache.set(selection, ends)
  return ends
}

/**
 * Builds the positional index for a bitmap once, avoiding a full bitmap scan
 * for every value read through a selected vector.
 *
 * @param {Extract<RowSelection, { type: 'bitmap' }>} selection
 * @returns {Uint32Array}
 */
function indicesForBitmap(selection) {
  const cached = selectionCache.get(selection)
  if (cached instanceof Uint32Array) return cached
  if (selection.values.length !== selection.length) {
    throw new Error(`Bitmap length ${selection.values.length} does not match selection length ${selection.length}`)
  }
  let count = 0
  for (const value of selection.values) {
    if (value) count++
  }
  const indices = new Uint32Array(count)
  let selectedIndex = 0
  for (let index = 0; index < selection.values.length; index++) {
    if (selection.values[index]) indices[selectedIndex++] = index
  }
  selectionCache.set(selection, indices)
  return indices
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
  if (result instanceof Promise) {
    return result.then(function validateResolvedVector(vector) {
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
