import { describe, expect, it, vi } from 'vitest'
import { selectedRowCount } from '../../src/backend/batch.js'
import { cachedDataSource, collect, executeSql } from '../../src/index.js'

/**
 * @import { AsyncBatch, AsyncDataSource, PreparedScan, PrepareScan, ReadColumn, RelationSchema, RowSelection, ScanRequest, SqlPrimitive } from '../../src/types.js'
 */

/** @type {RelationSchema} */
const schema = {
  fields: [
    { id: 10, name: 'keep', dataType: { type: 'boolean' }, nullable: false },
    { id: 20, name: 'payload', dataType: { type: 'string' }, nullable: false },
  ],
}

describe('prepared scans', () => {
  it('preserves alias scope in correlated residual filters', async () => {
    /** @type {RelationSchema} */
    const outerSchema = {
      fields: [
        { id: 1, name: 'id', dataType: { type: 'number' }, nullable: false },
        { id: 2, name: 'payload', dataType: { type: 'string' }, nullable: false },
      ],
    }
    /** @type {RelationSchema} */
    const innerSchema = {
      fields: [
        { id: 3, name: 'id', dataType: { type: 'number' }, nullable: false },
      ],
    }

    await expect(collect(executeSql({
      tables: {
        a: preparedValuesSource(outerSchema, { id: [1, 2], payload: ['yes', 'no'] }),
        b: preparedValuesSource(innerSchema, { id: [1] }),
      },
      query: `SELECT aa.payload
        FROM a aa
        WHERE EXISTS (SELECT 1 FROM b bb WHERE bb.id = aa.id)`,
    }))).resolves.toEqual([{ payload: 'yes' }])
  })

  it('preserves outer scope in filters above prepared derived tables', async () => {
    /** @type {RelationSchema} */
    const ordersSchema = {
      fields: [
        { id: 1, name: 'user_id', dataType: { type: 'number' }, nullable: false },
      ],
    }

    await expect(collect(executeSql({
      tables: {
        users: [{ id: 1 }, { id: 2 }],
        orders: preparedValuesSource(ordersSchema, { user_id: [1] }),
      },
      query: `SELECT u.id FROM users u
        WHERE EXISTS (
          SELECT 1 FROM (SELECT user_id AS id FROM orders) q
          WHERE q.id = u.id
        )
        ORDER BY u.id`,
    }))).resolves.toEqual([{ id: 1 }])
  })

  it('preserves outer scope in prepared projections', async () => {
    /** @type {RelationSchema} */
    const innerSchema = {
      fields: [
        { id: 1, name: 'u', dataType: { type: 'unknown' }, nullable: false },
      ],
    }

    await expect(collect(executeSql({
      tables: {
        users: [{ id: 1 }, { id: 2 }],
        items: preparedValuesSource(innerSchema, { u: [{ id: 99 }] }),
      },
      query: `SELECT u.id, (SELECT u.id FROM items) AS projected
        FROM users u ORDER BY u.id`,
    }))).resolves.toEqual([
      { id: 1, projected: 1 },
      { id: 2, projected: 2 },
    ])
  })

  it('preserves table alias precedence in prepared projections', async () => {
    /** @type {RelationSchema} */
    const aliasSchema = {
      fields: [
        { id: 1, name: 'd', dataType: { type: 'unknown' }, nullable: false },
        { id: 2, name: 'keep', dataType: { type: 'boolean' }, nullable: false },
      ],
    }

    await expect(collect(executeSql({
      tables: {
        data: preparedValuesSource(aliasSchema, {
          d: [{ keep: false }],
          keep: [true],
        }),
      },
      query: 'SELECT d AS value, d.keep AS projected FROM data d',
    }))).resolves.toEqual([{ value: { keep: false }, projected: true }])
  })

  it('resolves a table alias before an object field with the same name', async () => {
    /** @type {RelationSchema} */
    const aliasSchema = {
      fields: [
        { id: 1, name: 'd', dataType: { type: 'unknown' }, nullable: false },
        { id: 2, name: 'keep', dataType: { type: 'boolean' }, nullable: false },
      ],
    }
    const source = preparedValuesSource(aliasSchema, {
      d: [{ keep: false }],
      keep: [true],
    })

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT d FROM data d WHERE d.keep',
    }))).resolves.toEqual([{ d: { keep: false } }])
  })

  it('preserves table alias precedence inside a CTE', async () => {
    /** @type {RelationSchema} */
    const aliasSchema = {
      fields: [
        {
          id: 1,
          name: 't',
          dataType: {
            type: 'struct',
            fields: [{ id: 2, name: 'keep', dataType: { type: 'boolean' }, nullable: false }],
          },
          nullable: false,
        },
        { id: 3, name: 'keep', dataType: { type: 'boolean' }, nullable: false },
        { id: 4, name: 'payload', dataType: { type: 'string' }, nullable: false },
      ],
    }

    await expect(collect(executeSql({
      tables: {
        data: preparedValuesSource(aliasSchema, {
          t: [{ keep: false }],
          keep: [true],
          payload: ['yes'],
        }),
      },
      query: `WITH q AS (SELECT payload FROM data t WHERE t.keep)
        SELECT * FROM q`,
    }))).resolves.toEqual([{ payload: 'yes' }])

    await expect(collect(executeSql({
      tables: {
        data: preparedValuesSource(aliasSchema, {
          t: [{ keep: false }],
          keep: [true],
          payload: ['yes'],
        }),
      },
      query: `WITH q AS (
          SELECT payload FROM data t WHERE t.keep
          UNION ALL
          SELECT payload FROM data t WHERE t.keep
        ) SELECT * FROM q`,
    }))).resolves.toEqual([{ payload: 'yes' }, { payload: 'yes' }])
  })

  it('shares a deferred read between direct and computed projections', async () => {
    /** @type {RelationSchema} */
    const payloadSchema = {
      fields: [
        { id: 1, name: 'payload', dataType: { type: 'string' }, nullable: false },
      ],
    }
    /** @type {ReadColumn} */
    function readPayload() {
      return { type: 'values', values: ['value'], length: 1 }
    }
    const payload = vi.fn(readPayload)
    /** @type {AsyncDataSource} */
    const source = {
      schema: payloadSchema,
      prepareScan() {
        return {
          schema: payloadSchema,
          residual: {},
          properties: { exactRows: 1 },
          async *batches() {
            yield {
              selection: { type: 'all', length: 1 },
              columns: [{ read: payload }],
            }
          },
        }
      },
    }

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT payload, LENGTH(payload) AS length FROM data',
    }))).resolves.toEqual([{ payload: 'value', length: 5 }])
    expect(payload).toHaveBeenCalledTimes(1)

    payload.mockClear()
    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT payload AS first, payload AS second FROM data',
    }))).resolves.toEqual([{ first: 'value', second: 'value' }])
    expect(payload).toHaveBeenCalledTimes(1)
  })

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

  it('bounds residual predicate reads to the limited range', async () => {
    const length = 1_000
    /** @type {ReadColumn} */
    function readKeep({ selection }) {
      return { type: 'constant', value: true, length: selectedRowCount(selection) }
    }
    const keep = vi.fn(readKeep)
    /** @type {AsyncBatch} */
    const batch = {
      selection: { type: 'all', length },
      columns: [
        { read: keep },
        { type: 'constant', value: 'match', length },
      ],
    }
    const source = preparedSource(function prepareScan(request) {
      return {
        schema,
        residual: {
          filter: request.filter,
          limit: request.limit,
          offset: request.offset,
        },
        properties: { exactRows: length, maxRows: length },
        async *batches() { yield batch },
      }
    })

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data WHERE keep LIMIT 1 OFFSET 300',
    }))).resolves.toEqual([{ payload: 'match' }])
    expect(keep).toHaveBeenCalledTimes(1)
    expect(keep.mock.calls[0][0].selection).toEqual({
      type: 'range',
      start: 0,
      end: 301,
      length,
    })
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

  it('keeps REGEXP_LIKE residuals in native batches', async () => {
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

    expect(results.batches).toBeTypeOf('function')
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

  it('applies a source-applied range to fallback row metadata', async () => {
    /** @type {RelationSchema} */
    const rangeSchema = {
      fields: [
        { id: 1, name: 'payload', dataType: { type: 'string' }, nullable: false },
      ],
    }
    const source = {
      numRows: 100,
      schema: rangeSchema,
      /** @type {PrepareScan} */
      prepareScan(request) {
        expect(request.limit).toBe(1)
        expect(request.offset).toBe(5)
        return {
          schema: rangeSchema,
          residual: {},
          properties: {},
          async *batches() {
            yield {
              selection: { type: 'all', length: 1 },
              columns: [{ type: 'constant', value: 'sixth', length: 1 }],
            }
          },
        }
      },
    }

    const results = executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data LIMIT 1 OFFSET 5',
    })

    expect(results.numRows).toBe(1)
    expect(results.maxRows).toBe(1)
    await expect(collect(results)).resolves.toEqual([{ payload: 'sixth' }])
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

  it('requests a struct base field used by a projection', async () => {
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
      ],
    }
    const prepareScan = vi.fn(preparedValuesSource(structSchema, {
      obj: [{ x: 1 }, { x: 2 }],
    }).prepareScan)
    const source = { schema: structSchema, prepareScan }

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT obj.x FROM data',
    }))).resolves.toEqual([{ x: 1 }, { x: 2 }])
    expect(prepareScan.mock.calls[0][0].columns).toEqual([
      { field: 1, phase: 1, purpose: 'output', mode: 'deferred' },
    ])
  })

  it('requests an object base field used only by ordering', async () => {
    /** @type {RelationSchema} */
    const objectSchema = {
      fields: [
        { id: 1, name: 'obj', dataType: { type: 'unknown' }, nullable: false },
        { id: 2, name: 'payload', dataType: { type: 'string' }, nullable: false },
      ],
    }
    const prepareScan = vi.fn(preparedValuesSource(objectSchema, {
      obj: [{ x: 2 }, { x: 1 }],
      payload: ['second', 'first'],
    }).prepareScan)
    const source = { schema: objectSchema, prepareScan }

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data ORDER BY obj.x',
    }))).resolves.toEqual([{ payload: 'first' }, { payload: 'second' }])
    expect(prepareScan.mock.calls[0][0].columns).toEqual([
      { field: 2, phase: 1, purpose: 'output', mode: 'deferred' },
      { field: 1, phase: 1, purpose: 'output', mode: 'deferred' },
    ])
  })

  it('requests an unknown object base field used by a residual filter', async () => {
    /** @type {RelationSchema} */
    const objectSchema = {
      fields: [
        { id: 1, name: 'obj', dataType: { type: 'unknown' }, nullable: false },
        { id: 2, name: 'payload', dataType: { type: 'string' }, nullable: false },
      ],
    }
    const prepareScan = vi.fn(preparedValuesSource(objectSchema, {
      obj: [{ x: 1 }, { x: 2 }],
      payload: ['yes', 'no'],
    }).prepareScan)
    const source = { schema: objectSchema, prepareScan }

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data WHERE obj.x = 1',
    }))).resolves.toEqual([{ payload: 'yes' }])
    expect(prepareScan.mock.calls[0][0].columns).toEqual([
      { field: 2, phase: 1, purpose: 'output', mode: 'deferred' },
      { field: 1, phase: 0, purpose: 'filter', mode: 'required' },
    ])
  })

  it('does not request qualified columns owned by a predicate subquery', async () => {
    /** @type {RelationSchema} */
    const outerSchema = {
      fields: [
        { id: 1, name: 'id', dataType: { type: 'number' }, nullable: false },
        { id: 2, name: 'payload', dataType: { type: 'string' }, nullable: false },
      ],
    }
    /** @type {PrepareScan} */
    function prepareScan(request) {
      const fields = request.columns.map(function requestedField(demand) {
        const field = outerSchema.fields.find(function fieldById(candidate) {
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
              const values = field.name === 'id' ? [1, 2] : ['yes', 'no']
              return { type: 'values', values, length: 2 }
            }),
          }
        },
      }
    }
    const prepare = vi.fn(prepareScan)
    const source = {
      columns: ['id', 'payload'],
      schema: outerSchema,
      prepareScan: prepare,
    }

    await expect(collect(executeSql({
      tables: { data: source, other: [{ foreign_id: 1 }] },
      query: 'SELECT payload FROM data WHERE id IN (SELECT o.foreign_id FROM other o)',
    }))).resolves.toEqual([{ payload: 'yes' }])
    expect(prepare.mock.calls[0][0].columns).toEqual([
      { field: 2, phase: 1, purpose: 'output', mode: 'deferred' },
      { field: 1, phase: 0, purpose: 'filter', mode: 'required' },
    ])
  })

  it('does not request a struct column owned by a predicate subquery', async () => {
    /** @type {RelationSchema} */
    const parentSchema = {
      fields: [
        { id: 1, name: 'obj', dataType: { type: 'unknown' }, nullable: false },
        { id: 2, name: 'payload', dataType: { type: 'string' }, nullable: false },
      ],
    }
    /** @type {RelationSchema} */
    const childSchema = {
      fields: [
        { id: 3, name: 'obj', dataType: { type: 'unknown' }, nullable: false },
      ],
    }
    const parent = preparedValuesSource(parentSchema, {
      obj: [{ x: 0 }],
      payload: ['yes'],
    })
    const prepareScan = vi.fn(parent.prepareScan)

    await expect(collect(executeSql({
      tables: {
        parents: { schema: parentSchema, prepareScan },
        children: preparedValuesSource(childSchema, { obj: [{ x: 1 }] }),
      },
      query: `SELECT payload FROM parents
        WHERE EXISTS (SELECT * FROM children c WHERE obj.x = 1)`,
    }))).resolves.toEqual([{ payload: 'yes' }])
    expect(prepareScan.mock.calls[0][0].columns).toEqual([
      { field: 2, phase: 1, purpose: 'output', mode: 'deferred' },
    ])
  })

  it('resolves a filter prefix as a table alias before a struct column', () => {
    /** @type {RelationSchema} */
    const aliasSchema = {
      fields: [
        {
          id: 1,
          name: 'd',
          dataType: {
            type: 'struct',
            fields: [{ id: 2, name: 'x', dataType: { type: 'number' }, nullable: false }],
          },
          nullable: false,
        },
        { id: 3, name: 'keep', dataType: { type: 'boolean' }, nullable: false },
        { id: 4, name: 'payload', dataType: { type: 'string' }, nullable: false },
      ],
    }
    /** @type {PrepareScan} */
    function prepareScan(request) {
      const fields = request.columns.map(function requestedField(demand) {
        const field = aliasSchema.fields.find(function fieldById(candidate) {
          return candidate.id === demand.field
        })
        if (!field) throw new Error(`unknown field: ${demand.field}`)
        return field
      })
      return {
        schema: { fields },
        residual: {},
        properties: { exactRows: 0 },
        async *batches() {},
      }
    }
    const prepare = vi.fn(prepareScan)
    const source = {
      columns: ['d', 'keep', 'payload'],
      schema: aliasSchema,
      prepareScan: prepare,
    }

    executeSql({
      tables: { data: source },
      query: 'SELECT payload FROM data d WHERE d.keep',
    })

    expect(prepare.mock.calls[0][0].columns).toEqual([
      { field: 4, phase: 1, purpose: 'output', mode: 'deferred' },
      { field: 3, phase: 0, purpose: 'filter', mode: 'required' },
    ])
  })

  it('preserves prototype prepareScan methods through the cache wrapper', async () => {
    class PreparedSource {
      constructor() {
        /** @type {RelationSchema} */
        this.schema = {
          fields: [
            { id: 1, name: 'id', dataType: { type: 'number' }, nullable: false },
          ],
        }
      }

      /**
       * @param {ScanRequest} request
       * @returns {PreparedScan}
       */
      prepareScan(request) {
        expect(this).toBe(source)
        expect(request.columns).toEqual([
          { field: 1, phase: 1, purpose: 'output', mode: 'deferred' },
        ])
        return {
          schema: this.schema,
          residual: {},
          properties: { exactRows: 1 },
          async *batches() {
            yield {
              selection: { type: 'all', length: 1 },
              columns: [{ type: 'constant', value: 1, length: 1 }],
            }
          },
        }
      }

      /** @returns {never} */
      scan() {
        throw new Error('legacy row scan should not be called')
      }
    }
    const source = new PreparedSource()

    await expect(collect(executeSql({
      tables: { data: cachedDataSource(source) },
      query: 'SELECT id FROM data',
    }))).resolves.toEqual([{ id: 1 }])
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

/**
 * @param {RelationSchema} sourceSchema
 * @param {Record<string, SqlPrimitive[]>} values
 * @returns {AsyncDataSource}
 */
function preparedValuesSource(sourceSchema, values) {
  const length = Object.values(values)[0]?.length ?? 0
  return {
    numRows: length,
    schema: sourceSchema,
    prepareScan(request) {
      const fields = request.columns.map(function requestedField(demand) {
        const field = sourceSchema.fields.find(function fieldById(candidate) {
          return candidate.id === demand.field
        })
        if (!field) throw new Error(`unknown field: ${demand.field}`)
        return field
      })
      return {
        schema: { fields },
        residual: { filter: request.filter },
        properties: { exactRows: length },
        async *batches() {
          yield {
            selection: { type: 'all', length },
            columns: fields.map(function loadedField(field) {
              return { type: 'values', values: values[field.name], length }
            }),
          }
        },
      }
    },
  }
}
