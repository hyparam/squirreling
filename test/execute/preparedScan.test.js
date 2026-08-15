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
    const controller = new AbortController()
    /** @type {ReadColumn} */
    function readPayload({ selection, signal }) {
      expect(signal).toBe(controller.signal)
      expect(selection).toEqual({
        type: 'indices',
        indices: new Uint32Array([1]),
        length: 3,
      })
      return { type: 'values', values: ['second'], length: 1 }
    }
    const payload = vi.fn(readPayload)
    /** @type {AsyncBatch} */
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 3 },
      columns: [
        { type: 'loaded', vector: { type: 'values', values: [false, true, true], length: 3 } },
        { type: 'source', read: payload },
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
      signal: controller.signal,
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
        properties: { maxRows: 2 },
        async *batches() { yield batch },
      }
    })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data WHERE CASE WHEN keep THEN TRUE ELSE FALSE END',
    })

    expect(results.batches).toBeUndefined()
    expect(await collect(results)).toEqual([{ payload: 'second' }])
  })

  it('preserves short-circuiting before deferred column reads', async () => {
    const payload = vi.fn(function readPayload() {
      throw new Error('payload should not be read')
    })
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 2 },
      columns: [
        { type: 'loaded', vector: { type: 'values', values: [true, false], length: 2 } },
        { type: 'source', read: payload },
      ],
    }
    const source = preparedSource(function prepareScan(request) {
      return {
        schema,
        residual: { filter: request.filter },
        properties: { maxRows: 2 },
        async *batches() { yield batch },
      }
    })

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT keep FROM data WHERE FALSE AND LENGTH(payload) > 0',
    }))).resolves.toEqual([])
    expect(payload).not.toHaveBeenCalled()
  })

  it('counts prepared zero-column batches without a legacy scan', async () => {
    /** @type {RelationSchema} */
    const emptySchema = { fields: [] }
    /** @type {PrepareScan} */
    function prepareCount(request) {
      expect(request).toEqual({ columns: [] })
      return {
        schema: emptySchema,
        residual: {},
        properties: {},
        async *batches() {
          yield { schema: emptySchema, selection: { type: 'all', length: 2 }, columns: [] }
          yield { schema: emptySchema, selection: { type: 'all', length: 1 }, columns: [] }
        },
      }
    }
    const prepareScan = vi.fn(prepareCount)
    const source = {
      columns: ['keep'],
      schema: { fields: [schema.fields[0]] },
      prepareScan,
    }

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT COUNT(*) AS count FROM data',
    }))).resolves.toEqual([{ count: 3 }])
    expect(prepareScan).toHaveBeenCalledTimes(1)
  })

  it('reports batch expression errors with stream-global row numbers', async () => {
    /** @type {AsyncBatch[]} */
    const batches = [
      {
        schema,
        selection: { type: 'all', length: 2 },
        columns: [
          { type: 'loaded', vector: { type: 'values', values: [true, true], length: 2 } },
          { type: 'loaded', vector: { type: 'values', values: ['a', 'bb'], length: 2 } },
        ],
      },
      {
        schema,
        selection: { type: 'all', length: 1 },
        columns: [
          { type: 'loaded', vector: { type: 'values', values: [true], length: 1 } },
          { type: 'loaded', vector: { type: 'values', values: [3], length: 1 } },
        ],
      },
    ]
    const source = preparedSource(function prepareScan() {
      return {
        schema,
        residual: {},
        properties: { exactRows: 3 },
        async *batches() { yield* batches },
      }
    })

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT LENGTH(payload) AS length FROM data',
    }))).rejects.toThrow(
      'LENGTH(string): expected string or array, got number. Use CAST to convert to a string first. (row 3)'
    )
  })

  it('rejects an applied range while a residual filter remains', () => {
    const source = preparedSource(function prepareScan(request) {
      return {
        schema,
        residual: { filter: request.filter },
        properties: { maxRows: 1 },
        async *batches() {},
      }
    })

    expect(() => executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data WHERE keep LIMIT 1',
    })).toThrow('Data source "data" applied limit/offset without applying where')
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
