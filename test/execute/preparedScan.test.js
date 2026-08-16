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
    const batch = {
      selection: { type: 'all', length: 3 },
      columns: [
        { type: 'values', values: [false, true, true], length: 3 },
        { read: payload },
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
      { field: 20, phase: 1, purpose: 'output', mode: 'deferred' },
      { field: 10, phase: 0, purpose: 'filter', mode: 'required' },
    ])
    expect(payload).not.toHaveBeenCalled()
    expect(await collect(results)).toEqual([{ payload: 'second' }])
    expect(payload).toHaveBeenCalledTimes(1)
  })

  it('keeps CASE residuals in native batches', async () => {
    /** @type {AsyncBatch} */
    const batch = {
      selection: { type: 'all', length: 2 },
      columns: [
        { type: 'values', values: [false, true], length: 2 },
        { type: 'values', values: ['first', 'second'], length: 2 },
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

    expect(results.batches).toBeTypeOf('function')
    expect(await collect(results)).toEqual([{ payload: 'second' }])
  })

  it('uses the row compatibility boundary for an unsupported residual', async () => {
    /** @type {AsyncBatch} */
    const batch = {
      selection: { type: 'all', length: 2 },
      columns: [
        { type: 'values', values: [false, true], length: 2 },
        { type: 'values', values: ['first', 'second'], length: 2 },
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
      query: 'SELECT payload FROM data WHERE REGEXP_LIKE(payload, \'^second$\')',
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
      selection: { type: 'all', length: 2 },
      columns: [
        { type: 'values', values: [true, false], length: 2 },
        { read: payload },
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
          yield { selection: { type: 'all', length: 2 }, columns: [] }
          yield { selection: { type: 'all', length: 1 }, columns: [] }
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
        selection: { type: 'all', length: 2 },
        columns: [
          { type: 'values', values: [true, true], length: 2 },
          { type: 'values', values: ['a', 'bb'], length: 2 },
        ],
      },
      {
        selection: { type: 'all', length: 1 },
        columns: [
          { type: 'values', values: [true], length: 1 },
          { type: 'values', values: [3], length: 1 },
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

  it('preserves logical schema order while assigning filter phases', async () => {
    /** @type {RelationSchema} */
    const orderedSchema = {
      fields: [
        { id: 1, name: 'first', dataType: { type: 'string' }, nullable: false },
        { id: 2, name: 'keep', dataType: { type: 'boolean' }, nullable: false },
        { id: 3, name: 'last', dataType: { type: 'string' }, nullable: false },
      ],
    }
    /** @type {PrepareScan} */
    function prepareScan(request) {
      const fields = request.columns.map(function requestedField(demand) {
        const field = orderedSchema.fields.find(function fieldById(candidate) {
          return candidate.id === demand.field
        })
        if (!field) throw new Error(`unknown field: ${demand.field}`)
        return field
      })
      /** @type {Record<string, import('../../src/types.js').SqlPrimitive[]>} */
      const values = {
        first: ['a'],
        keep: [true],
        last: ['z'],
      }
      return {
        schema: { fields },
        residual: { filter: request.filter },
        properties: { exactRows: 1 },
        async *batches() {
          yield {
            selection: { type: 'all', length: 1 },
            columns: fields.map(function loadedField(field) {
              return { type: 'values', values: values[field.name], length: 1 }
            }),
          }
        },
      }
    }
    const prepare = vi.fn(prepareScan)
    const source = {
      schema: orderedSchema,
      prepareScan: prepare,
    }

    const results = executeSql({
      tables: { data: source },
      query: 'SELECT * FROM data WHERE keep',
    })

    expect(results.columns).toEqual(['first', 'keep', 'last'])
    expect(prepare.mock.calls[0][0].columns).toEqual([
      { field: 1, phase: 1, purpose: 'output', mode: 'deferred' },
      { field: 2, phase: 0, purpose: 'filter', mode: 'required' },
      { field: 3, phase: 1, purpose: 'output', mode: 'deferred' },
    ])
    await expect(collect(results)).resolves.toEqual([{ first: 'a', keep: true, last: 'z' }])
  })

  it('requests a struct base field used by a residual filter', async () => {
    /** @type {RelationSchema} */
    const structSchema = {
      fields: [
        {
          id: 1,
          name: 'obj',
          dataType: {
            type: 'struct',
            fields: [{ id: 2, name: 'x', dataType: { type: 'number' }, nullable: false }],
          },
          nullable: false,
        },
        { id: 3, name: 'payload', dataType: { type: 'string' }, nullable: false },
      ],
    }
    /** @type {PrepareScan} */
    function prepareScan(request) {
      const fields = request.columns.map(function requestedField(demand) {
        const field = structSchema.fields.find(function fieldById(candidate) {
          return candidate.id === demand.field
        })
        if (!field) throw new Error(`unknown field: ${demand.field}`)
        return field
      })
      return {
        schema: { fields },
        residual: { filter: request.filter },
        properties: { exactRows: 2 },
        async *batches() {
          yield {
            selection: { type: 'all', length: 2 },
            columns: fields.map(function loadedField(field) {
              const values = field.name === 'obj' ? [{ x: 1 }, { x: 2 }] : ['yes', 'no']
              return { type: 'values', values, length: 2 }
            }),
          }
        },
      }
    }
    const prepare = vi.fn(prepareScan)
    const source = {
      columns: ['obj', 'payload'],
      schema: structSchema,
      prepareScan: prepare,
    }

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data WHERE obj.x = 1',
    }))).resolves.toEqual([{ payload: 'yes' }])
    expect(prepare.mock.calls[0][0].columns).toEqual([
      { field: 3, phase: 1, purpose: 'output', mode: 'deferred' },
      { field: 1, phase: 0, purpose: 'filter', mode: 'required' },
    ])
  })

  it('surfaces cooperative aborts from direct batch iteration', async () => {
    const { results } = abortingPreparedResults()
    if (!results.batches) throw new Error('expected native batches')
    async function consumeBatches() {
      const batches = []
      for await (const batch of results.batches()) batches.push(batch)
      return batches
    }

    await expect(consumeBatches()).rejects.toThrow('prepared scan aborted')
  })

  it('surfaces cooperative aborts from direct row iteration', async () => {
    const { results } = abortingPreparedResults()
    async function consumeRows() {
      const rows = []
      for await (const row of results.rows()) rows.push(row)
      return rows
    }

    await expect(consumeRows()).rejects.toThrow('prepared scan aborted')
  })

  it('surfaces cooperative aborts while collecting batches', async () => {
    const { results } = abortingPreparedResults()
    await expect(collect(results)).rejects.toThrow('prepared scan aborted')
  })
})

/**
 * @returns {{ results: import('../../src/types.js').QueryResults }}
 */
function abortingPreparedResults() {
  const controller = new AbortController()
  const source = preparedSource(function prepareScan() {
    return {
      schema,
      residual: {},
      properties: { exactRows: 1 },
      async *batches({ signal } = {}) {
        yield {
          selection: { type: 'all', length: 1 },
          columns: [
            { type: 'constant', value: true, length: 1 },
            { type: 'constant', value: 'value', length: 1 },
          ],
        }
        controller.abort(new Error('prepared scan aborted'))
        if (!signal?.aborted) throw new Error('expected prepared signal to be aborted')
      },
    }
  })
  return {
    results: executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data',
      signal: controller.signal,
    }),
  }
}

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
