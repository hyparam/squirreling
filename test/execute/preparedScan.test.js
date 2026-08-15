import { describe, expect, it, vi } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * @import { AsyncBatch, AsyncDataSource, PrepareScan, ReadColumn, RelationSchema, RowSelection } from '../../src/types.js'
 */

/** @type {RelationSchema} */
const schema = {
  fields: [
    { id: 10, name: 'keep', dataType: { type: 'boolean' }, nullable: false },
    { id: 20, name: 'payload', dataType: { type: 'string' }, nullable: false },
  ],
}

describe('prepared scans', () => {
  it('passes the final filtered range to a deferred output column', async () => {
    /** @type {ReadColumn} */
    function readPayload({ selection }) {
      expect(selection).toEqual({
        type: 'indices',
        indices: new Uint32Array([1]),
        length: 3,
      })
      return { type: 'values', values: ['second'], length: 1 }
    }
    const payload = vi.fn(readPayload)
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      baseRowCount: 3,
      rowCount: 3,
      selection: { type: 'all', length: 3 },
      columns: [
        { type: 'loaded', vector: { type: 'values', values: [false, true, true], length: 3 } },
        { type: 'source', length: 3, read: payload },
      ],
    }
    /** @type {PrepareScan} */
    function prepareScan(request) {
      return {
        schema,
        residual: {
          filter: request.filter,
          limit: request.limit,
          offset: request.offset,
        },
        properties: {
          exactRows: 3,
          maxRows: 3,
          columns: [
            { field: 10, selectionGranularity: 'arbitrary' },
            { field: 20, selectionGranularity: 'arbitrary' },
          ],
          restartable: true,
        },
        async *batches() {
          yield batch
        },
      }
    }
    const prepare = vi.fn(prepareScan)
    const source = preparedSource(prepare)

    const results = executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data WHERE keep LIMIT 1',
    })

    expect(prepare).toHaveBeenCalledTimes(1)
    const [request] = prepare.mock.calls[0]
    expect(request.columns).toEqual([
      { field: 10, phase: 0, purpose: 'filter', mode: 'required' },
      { field: 20, phase: 1, purpose: 'output', mode: 'deferred' },
    ])
    expect(payload).not.toHaveBeenCalled()
    expect(await collect(results)).toEqual([{ payload: 'second' }])
    expect(payload).toHaveBeenCalledTimes(1)
  })

  it('uses the row compatibility boundary for an unsupported residual', async () => {
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      baseRowCount: 2,
      rowCount: 2,
      selection: { type: 'all', length: 2 },
      columns: [
        { type: 'loaded', vector: { type: 'values', values: [false, true], length: 2 } },
        { type: 'loaded', vector: { type: 'values', values: ['first', 'second'], length: 2 } },
      ],
    }
    const source = preparedSource(function prepareScan(request) {
      return {
        schema,
        residual: { filter: request.filter },
        properties: { maxRows: 2, columns: [] },
        async *batches() { yield batch },
      }
    })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data WHERE COALESCE(keep, FALSE)',
    })

    expect(results.batches).toBeUndefined()
    expect(await collect(results)).toEqual([{ payload: 'second' }])
  })
})

/**
 * @param {PrepareScan} prepareScan
 * @returns {AsyncDataSource}
 */
function preparedSource(prepareScan) {
  return {
    columns: ['keep', 'payload'],
    schema,
    prepareScan,
    scan() {
      throw new Error('legacy row scan should not be called')
    },
  }
}
