import { isPromiseLike, readBatchColumn, resolveColumnResults, selectVector, selectedRowCount, selectionIndexAt, valueAt } from '../backend/batch.js'
import { compareForTerm } from './utils.js'

/**
 * @import { CompiledBatchExpression } from '../internalTypes.js'
 * @import { AsyncBatch, ColumnVector, OrderByItem, SqlPrimitive } from '../types.js'
 */

const INITIAL_OUTPUT_ROWS = 64
const MAX_OUTPUT_ROWS = 4096

/**
 * Sorts batches by evaluating only the ORDER BY keys. Each input row is kept
 * as a (source batch, base row) address; non-key columns stay deferred and are
 * gathered per output chunk, so a consumer that stops early never reads the
 * payload of rows it did not take.
 *
 * Output chunks start small and grow, keeping the first rows cheap.
 *
 * @param {object} options
 * @param {AsyncIterable<AsyncBatch>} options.batches
 * @param {CompiledBatchExpression[]} options.keys - one compiled key per term
 * @param {OrderByItem[]} options.orderBy
 * @param {number} options.columnCount
 * @param {number} [options.topK]
 * @param {AbortSignal} [options.signal]
 * @yields {AsyncBatch}
 */
export async function* sortBatches({ batches, keys, orderBy, columnCount, topK, signal }) {
  /** @type {AsyncBatch[]} */
  let sources = []
  /** @type {number[]} */
  let sourceIds = []
  /** @type {number[]} */
  let baseRows = []
  /** @type {SqlPrimitive[][]} */
  let keyValues = orderBy.map(function emptyKeys() {
    /** @type {SqlPrimitive[]} */
    const values = []
    return values
  })
  const bufferLimit = topK === undefined ? Infinity : Math.max(topK * 2, 1024)
  let rowOffset = 0

  for await (const batch of batches) {
    signal?.throwIfAborted()
    const count = selectedRowCount(batch.selection)
    if (count === 0) continue
    // A throwaway batch identity keeps key dependency vectors out of the
    // source batch's read cache, so retaining the source pins no key copies.
    const keyBatch = { selection: batch.selection, columns: batch.columns }
    const vectors = await resolveColumnResults(keys.map(function evaluateKey(key) {
      return key.evaluate({ batch: keyBatch, selection: keyBatch.selection, signal, rowOffset })
    }))
    const sourceId = sources.length
    sources.push(batch)
    for (let row = 0; row < count; row++) {
      sourceIds.push(sourceId)
      baseRows.push(selectionIndexAt(batch.selection, row))
      for (let term = 0; term < keyValues.length; term++) {
        keyValues[term].push(valueAt(vectors[term], row))
      }
    }
    rowOffset += count

    if (topK !== undefined && sourceIds.length >= bufferLimit) {
      const kept = sortedOrder(keyValues, orderBy).slice(0, topK)
      /** @type {Map<number, number>} */
      const remapped = new Map()
      /** @type {AsyncBatch[]} */
      const keptSources = []
      sourceIds = kept.map(function remapSource(index) {
        const oldId = sourceIds[index]
        let newId = remapped.get(oldId)
        if (newId === undefined) {
          newId = keptSources.length
          remapped.set(oldId, newId)
          keptSources.push(sources[oldId])
        }
        return newId
      })
      sources = keptSources
      const oldBaseRows = baseRows
      baseRows = kept.map(index => oldBaseRows[index])
      keyValues = keyValues.map(values => kept.map(index => values[index]))
    }
  }
  signal?.throwIfAborted()

  let order = sortedOrder(keyValues, orderBy)
  if (topK !== undefined && order.length > topK) order = order.slice(0, topK)
  // Key values are no longer needed once the order is fixed.
  keyValues.length = 0

  // Each source's needed rows, ascending, for reads that serve many chunks.
  /** @type {SourceState[]} */
  const states = sources.map(function sourceState(batch) {
    return { batch, rows: new Uint32Array(0), remaining: new Map(), cache: new Map() }
  })
  /** @type {number[]} */
  const counts = new Array(states.length).fill(0)
  for (const index of order) counts[sourceIds[index]]++
  for (let id = 0; id < states.length; id++) states[id].rows = new Uint32Array(counts[id])
  counts.fill(0)
  for (const index of order) {
    const id = sourceIds[index]
    states[id].rows[counts[id]++] = baseRows[index]
  }
  for (const state of states) state.rows.sort()
  sources = []

  let start = 0
  let size = INITIAL_OUTPUT_ROWS
  while (start < order.length) {
    signal?.throwIfAborted()
    const end = Math.min(order.length, start + size)
    yield gatheredBatch({ states, sourceIds, baseRows, order, start, end, columnCount })
    start = end
    size = Math.min(size * 2, MAX_OUTPUT_ROWS)
  }
}

/**
 * Returns row indices in sorted order. Ties keep input order.
 *
 * @param {SqlPrimitive[][]} keyValues
 * @param {OrderByItem[]} orderBy
 * @returns {number[]}
 */
function sortedOrder(keyValues, orderBy) {
  const length = keyValues[0]?.length ?? 0
  /** @type {number[]} */
  const order = new Array(length)
  for (let i = 0; i < length; i++) order[i] = i
  order.sort(function compareRows(a, b) {
    for (let term = 0; term < orderBy.length; term++) {
      const values = keyValues[term]
      const cmp = compareForTerm(values[a], values[b], orderBy[term])
      if (cmp !== 0) return cmp
    }
    return a - b
  })
  return order
}

/**
 * A source batch's rows in the sorted output. `rows` holds every needed base
 * row ascending. A column read that would span most of those rows anyway
 * reads all of them once into `cache`, held until `remaining` reaches zero.
 *
 * @typedef {object} SourceState
 * @property {AsyncBatch} batch
 * @property {Uint32Array} rows
 * @property {Map<number, number>} remaining
 * @property {Map<number, ColumnVector | Promise<ColumnVector>>} cache
 */

/**
 * @typedef {object} SourceRead
 * @property {SourceState} state
 * @property {Uint32Array} indices - base rows for this chunk, ascending
 * @property {number[]} positions - output position of each index
 */

/**
 * Builds one output batch whose columns gather their values from the source
 * batches only when read.
 *
 * @param {object} options
 * @param {SourceState[]} options.states
 * @param {number[]} options.sourceIds
 * @param {number[]} options.baseRows
 * @param {number[]} options.order
 * @param {number} options.start
 * @param {number} options.end
 * @param {number} options.columnCount
 * @returns {AsyncBatch}
 */
function gatheredBatch({ states, sourceIds, baseRows, order, start, end, columnCount }) {
  const length = end - start
  /** @type {Map<number, { rows: number[], positions: number[] }>} */
  const groups = new Map()
  for (let position = start; position < end; position++) {
    const index = order[position]
    const sourceId = sourceIds[index]
    let group = groups.get(sourceId)
    if (!group) {
      group = { rows: [], positions: [] }
      groups.set(sourceId, group)
    }
    group.rows.push(baseRows[index])
    group.positions.push(position - start)
  }
  // Sources read ascending row indices; keep each output position aligned.
  /** @type {SourceRead[]} */
  const reads = []
  for (const [sourceId, group] of groups) {
    const pairs = group.rows.map((row, i) => [row, group.positions[i]])
    pairs.sort((a, b) => a[0] - b[0])
    const indices = new Uint32Array(pairs.length)
    const positions = new Array(pairs.length)
    for (let i = 0; i < pairs.length; i++) {
      indices[i] = pairs[i][0]
      positions[i] = pairs[i][1]
    }
    reads.push({ state: states[sourceId], indices, positions })
  }

  return {
    selection: { type: 'all', length },
    columns: Array.from({ length: columnCount }, function gatheredColumn(_, columnIndex) {
      /** @type {ColumnVector | Promise<ColumnVector> | undefined} */
      let gathered
      return {
        read({ selection, signal }) {
          gathered ??= gatherColumn(reads, columnIndex, length, signal)
          if (isPromiseLike(gathered)) {
            return gathered.then(vector => selectVector(vector, selection))
          }
          return selectVector(gathered, selection)
        },
      }
    }),
  }
}

/**
 * @param {SourceRead[]} reads
 * @param {number} columnIndex
 * @param {number} length
 * @param {AbortSignal} [signal]
 * @returns {ColumnVector | Promise<ColumnVector>}
 */
function gatherColumn(reads, columnIndex, length, signal) {
  const results = reads.map(function readSource(read) {
    return readSourceRows(read, columnIndex, signal)
  })
  /**
   * @param {ColumnVector[]} vectors
   * @returns {ColumnVector}
   */
  function place(vectors) {
    /** @type {SqlPrimitive[]} */
    const values = new Array(length)
    for (let r = 0; r < reads.length; r++) {
      const { positions } = reads[r]
      const vector = vectors[r]
      for (let i = 0; i < positions.length; i++) {
        values[positions[i]] = valueAt(vector, i)
      }
    }
    return { type: 'values', values, length }
  }
  const resolved = resolveColumnResults(results)
  return isPromiseLike(resolved) ? resolved.then(place) : place(resolved)
}

/**
 * Reads one chunk's rows of a source column. Sources decode the span between
 * the first and last requested row. When this chunk already takes a sixteenth
 * of the rows the source owes and spans at least half of them, read them all
 * once and serve later chunks from that read instead of decoding the span
 * again. A chunk taking a few scattered rows still reads only those, so a
 * consumer that stops early never pays for the rest.
 *
 * @param {SourceRead} read
 * @param {number} columnIndex
 * @param {AbortSignal} [signal]
 * @returns {ColumnVector | Promise<ColumnVector>}
 */
function readSourceRows({ state, indices }, columnIndex, signal) {
  const { rows, cache, remaining } = state
  let cached = cache.get(columnIndex)
  if (!cached && indices.length < rows.length && indices.length * 16 >= rows.length) {
    const chunkSpan = indices[indices.length - 1] - indices[0] + 1
    const sourceSpan = rows[rows.length - 1] - rows[0] + 1
    if (chunkSpan * 2 >= sourceSpan) {
      cached = readRows(state.batch, rows, columnIndex, signal)
      cache.set(columnIndex, cached)
    }
  }
  const left = (remaining.get(columnIndex) ?? rows.length) - indices.length
  if (left > 0) {
    remaining.set(columnIndex, left)
  } else {
    remaining.delete(columnIndex)
    cache.delete(columnIndex)
  }
  if (!cached) return readRows(state.batch, indices, columnIndex, signal)
  if (isPromiseLike(cached)) {
    return cached.then(vector => pickRows(vector, rows, indices))
  }
  return pickRows(cached, rows, indices)
}

/**
 * @param {AsyncBatch} batch
 * @param {Uint32Array} indices - ascending base rows
 * @param {number} columnIndex
 * @param {AbortSignal} [signal]
 * @returns {ColumnVector | Promise<ColumnVector>}
 */
function readRows(batch, indices, columnIndex, signal) {
  return readBatchColumn({
    batch: {
      selection: { type: 'indices', indices, length: batch.selection.length },
      columns: batch.columns,
    },
    columnIndex,
    signal,
  })
}

/**
 * Picks ascending `indices` out of a vector read for ascending `rows`.
 *
 * @param {ColumnVector} vector
 * @param {Uint32Array} rows
 * @param {Uint32Array} indices
 * @returns {ColumnVector}
 */
function pickRows(vector, rows, indices) {
  /** @type {SqlPrimitive[]} */
  const values = new Array(indices.length)
  let row = 0
  for (let i = 0; i < indices.length; i++) {
    while (rows[row] !== indices[i]) row++
    values[i] = valueAt(vector, row)
  }
  return { type: 'values', values, length: indices.length }
}
