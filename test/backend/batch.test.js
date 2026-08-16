import { describe, expect, it, vi } from 'vitest'
import {
  composeSelections,
  readBatchColumn,
  selectBatch,
  selectVector,
  selectedRowCount,
  valueAt,
} from '../../src/backend/batch.js'

/**
 * @import { AsyncBatch, ColumnVector, ReadColumn, RelationSchema } from '../../src/internalTypes.js'
 */

/** @type {RelationSchema} */
const schema = {
  fields: [
    { id: 0, name: 'value', dataType: { type: 'number' }, nullable: false },
  ],
}

describe('row selections', () => {
  it('counts every concrete selection representation', () => {
    expect(selectedRowCount({ type: 'all', length: 5 })).toBe(5)
    expect(selectedRowCount({ type: 'range', start: 1, end: 4, length: 5 })).toBe(3)
    expect(selectedRowCount({
      type: 'ranges',
      ranges: [{ start: 0, end: 2 }, { start: 4, end: 5 }],
      length: 5,
    })).toBe(3)
    expect(selectedRowCount({
      type: 'indices',
      indices: new Uint32Array([1, 4]),
      length: 5,
    })).toBe(2)
    expect(selectedRowCount({
      type: 'bitmap',
      values: new Uint8Array([0, 1, 0, 1, 1]),
      length: 5,
    })).toBe(3)
  })

  it('preserves ranges while composing contiguous selections', () => {
    expect(composeSelections(
      { type: 'range', start: 10, end: 20, length: 30 },
      { type: 'range', start: 2, end: 5, length: 10 }
    )).toEqual({ type: 'range', start: 12, end: 15, length: 30 })
  })

  it('composes sparse selections into base-domain indices', () => {
    expect(composeSelections(
      { type: 'indices', indices: new Uint32Array([2, 4, 8, 9]), length: 10 },
      { type: 'indices', indices: new Uint32Array([1, 3]), length: 4 }
    )).toEqual({
      type: 'indices',
      indices: new Uint32Array([4, 9]),
      length: 10,
    })
  })

  it('rejects selections over the wrong domain', () => {
    expect(() => composeSelections(
      { type: 'range', start: 2, end: 5, length: 10 },
      { type: 'all', length: 10 }
    )).toThrow('Cannot compose selection of length 10 over 3 rows')
  })
})

describe('column vectors', () => {
  it('reads typed nulls through a zero-copy selected view', () => {
    /** @type {ColumnVector} */
    const source = {
      type: 'typed',
      values: new Int32Array([10, 20, 30, 40]),
      validity: new Uint8Array([1, 0, 1, 1]),
      length: 4,
    }
    const vector = selectVector(source, {
      type: 'indices',
      indices: new Uint32Array([3, 1]),
      length: 4,
    })

    expect(vector.type).toBe('selected')
    expect(vector.type === 'selected' && vector.source).toBe(source)
    expect(valueAt(vector, 0)).toBe(40)
    expect(valueAt(vector, 1)).toBeNull()
  })

  it('composes nested selected views', () => {
    /** @type {ColumnVector} */
    const source = { type: 'values', values: ['a', 'b', 'c', 'd'], length: 4 }
    const first = selectVector(source, {
      type: 'indices',
      indices: new Uint32Array([3, 1, 2]),
      length: 4,
    })
    const second = selectVector(first, {
      type: 'range',
      start: 1,
      end: 3,
      length: 3,
    })

    expect(valueAt(second, 0)).toBe('b')
    expect(valueAt(second, 1)).toBe('c')
  })
})

describe('async batches', () => {
  it('keeps source reads lazy and memoizes a selected result', async () => {
    /** @type {ReadColumn} */
    function readColumn({ selection }) {
      return Promise.resolve({
        type: 'constant',
        value: 7,
        length: selectedRowCount(selection),
      })
    }
    const read = vi.fn(readColumn)
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 5 },
      columns: [{ type: 'source', read }],
    }

    expect(read).not.toHaveBeenCalled()
    const selected = selectBatch(batch, {
      type: 'range',
      start: 1,
      end: 3,
      length: 5,
    })
    const first = readBatchColumn({ batch: selected, columnIndex: 0 })
    const second = readBatchColumn({ batch: selected, columnIndex: 0 })

    expect(first).toBe(second)
    expect(await first).toEqual({ type: 'constant', value: 7, length: 2 })
    expect(read).toHaveBeenCalledTimes(1)
    expect(read).toHaveBeenCalledWith({
      selection: { type: 'range', start: 1, end: 3, length: 5 },
      signal: undefined,
    })
  })

  it('does not turn a synchronous loaded read into a promise', () => {
    /** @type {ColumnVector} */
    const vector = { type: 'values', values: [1, 2, 3], length: 3 }
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 3 },
      columns: [{ type: 'loaded', vector }],
    }

    expect(readBatchColumn({ batch, columnIndex: 0 })).toBe(vector)
  })

  it('validates deferred vector alignment', async () => {
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 3 },
      columns: [{
        type: 'source',
        read() {
          return Promise.resolve({ type: 'values', values: [1], length: 1 })
        },
      }],
    }

    await expect(readBatchColumn({ batch, columnIndex: 0 })).rejects.toThrow(
      'Column returned 1 rows, expected 3'
    )
  })

  it('does not retain a rejected signal-bound column read', async () => {
    const first = new AbortController()
    const second = new AbortController()
    /** @type {ReadColumn} */
    function readColumn({ selection, signal }) {
      if (signal === second.signal) {
        return Promise.resolve({
          type: 'constant',
          value: 2,
          length: selectedRowCount(selection),
        })
      }
      return new Promise(function waitForAbort(_resolve, reject) {
        signal?.addEventListener('abort', function rejectAbort() { reject(signal.reason) }, { once: true })
      })
    }
    const read = vi.fn(readColumn)
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 1 },
      columns: [{ type: 'source', read }],
    }

    const rejected = readBatchColumn({ batch, columnIndex: 0, signal: first.signal })
    const rejection = expect(rejected).rejects.toThrow('first read aborted')
    first.abort(new Error('first read aborted'))
    await rejection

    await expect(readBatchColumn({
      batch,
      columnIndex: 0,
      signal: second.signal,
    })).resolves.toEqual({ type: 'constant', value: 2, length: 1 })
    expect(read).toHaveBeenCalledTimes(2)
  })
})
