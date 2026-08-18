import { describe, expect, it, vi } from 'vitest'
import { selectedRowCount } from '../../src/backend/batch.js'
import { collect } from '../../src/execute/utils.js'
import { batchesToRows, rowsToBatches } from '../../src/backend/batchAdapters.js'
import { batchResult } from '../../src/execute/batchResults.js'

/**
 * @import { AsyncBatch, AsyncRow, ColumnVector, ReadColumn } from '../../src/types.js'
 */

const schema = ['id', 'name']

describe('batch adapters', () => {
  it('materializes legacy rows into bounded aligned batches', async () => {
    /** @type {AsyncRow[]} */
    const rows = [
      resolvedRow({ id: 1, name: 'a' }),
      resolvedRow({ id: 2, name: 'b' }),
      resolvedRow({ id: 3, name: 'c' }),
    ]

    const batches = []
    for await (const batch of rowsToBatches(asyncValues(rows), schema, { batchRows: 2 })) {
      batches.push(batch)
    }

    expect(batches.map(function summarize(batch) {
      return {
        rowCount: selectedRowCount(batch.selection),
        ids: 'type' in batch.columns[0] && batch.columns[0].type === 'values'
          ? batch.columns[0].values
          : [],
      }
    })).toEqual([
      { rowCount: 2, ids: [1, 2] },
      { rowCount: 1, ids: [3] },
    ])
  })

  it('keeps deferred columns lazy through the row adapter', async () => {
    /** @type {ReadColumn} */
    function readColumn() {
      /** @type {ColumnVector} */
      const vector = { type: 'values', values: [1, 2], length: 2 }
      return Promise.resolve(vector)
    }
    const read = vi.fn(readColumn)
    /** @type {AsyncBatch} */
    const batch = {
      selection: { type: 'all', length: 2 },
      columns: [{ read }],
    }

    const iterator = batchesToRows(asyncValues([batch]), [schema[0]])[Symbol.asyncIterator]()
    const first = await iterator.next()
    expect(read).not.toHaveBeenCalled()
    if (first.done || !first.value) throw new Error('expected a row')
    expect(await first.value.cells.id()).toBe(1)
    expect(read).toHaveBeenCalledTimes(1)

    const second = await iterator.next()
    if (second.done || !second.value) throw new Error('expected a row')
    expect(await second.value.cells.id()).toBe(2)
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('adapts loaded columns through the batch selection', async () => {
    /** @type {AsyncBatch} */
    const batch = {
      selection: {
        type: 'indices',
        indices: new Uint32Array([2, 0]),
        length: 3,
      },
      columns: [
        {
          type: 'values',
          values: [10, 20, 30],
          length: 3,
        },
        {
          type: 'values',
          values: ['a', 'b', 'c'],
          length: 3,
        },
      ],
    }

    const rows = []
    for await (const row of batchesToRows(asyncValues([batch]), schema)) {
      rows.push({
        id: await row.cells.id(),
        name: await row.cells.name(),
      })
    }

    expect(rows).toEqual([
      { id: 30, name: 'c' },
      { id: 10, name: 'a' },
    ])
  })

  it('collects native batches without consuming the row adapter', async () => {
    /** @type {AsyncBatch} */
    const batch = {
      selection: {
        type: 'indices',
        indices: new Uint32Array([2, 0]),
        length: 3,
      },
      columns: [
        {
          type: 'values',
          values: [10, 20, 30],
          length: 3,
        },
        {
          type: 'values',
          values: ['a', 'b', 'c'],
          length: 3,
        },
      ],
    }
    const results = batchResult({
      columns: ['id', 'name'],
      async *batches() { yield batch },
    })
    results.rows = vi.fn(function rows() {
      throw new Error('row adapter should not be consumed')
    })

    await expect(collect(results)).resolves.toEqual([
      { id: 30, name: 'c' },
      { id: 10, name: 'a' },
    ])
    expect(results.rows).not.toHaveBeenCalled()
  })

  it('rejects partial rows when a cooperative batch source stops on abort', async () => {
    const controller = new AbortController()
    const reason = new Error('batch collection aborted')
    /** @type {AsyncBatch} */
    const batch = {
      selection: { type: 'all', length: 1 },
      columns: [{
        type: 'constant',
        value: 1,
        length: 1,
      }],
    }
    const results = batchResult({
      columns: ['id'],
      signal: controller.signal,
      async *batches() {
        yield batch
        controller.abort(reason)
      },
    })

    await expect(collect(results)).rejects.toThrow('batch collection aborted')
  })
})

/**
 * @param {Record<string, import('../../src/types.js').SqlPrimitive>} values
 * @returns {AsyncRow}
 */
function resolvedRow(values) {
  return {
    columns: Object.keys(values),
    cells: Object.fromEntries(Object.entries(values).map(function cell([name, value]) {
      return [name, function readCell() { return Promise.resolve(value) }]
    })),
    resolved: values,
  }
}

/**
 * @template T
 * @param {T[]} values
 * @yields {T}
 */
async function* asyncValues(values) {
  yield* values
}
