import { describe, expect, it, vi } from 'vitest'
import { runInNewContext } from 'node:vm'
import { collect, executeSql, planSql } from '../../src/index.js'
import { planStreamingAggregates } from '../../src/execute/streamingAggregate.js'

/**
 * @import { AsyncBatch, AsyncDataSource, PrepareScan, ReadColumn, RelationSchema, ScanColumnResults, SqlPrimitive } from '../../src/types.js'
 */

/** @type {RelationSchema} */
const schema = {
  fields: [
    { id: 1, name: 'provider', dataType: { type: 'string' }, nullable: false },
    { id: 2, name: 'session_id', dataType: { type: 'string' }, nullable: false },
    { id: 3, name: 'attributes', dataType: { type: 'unknown' }, nullable: true },
  ],
}

/** @type {RelationSchema} */
const aliasSchema = {
  fields: [
    { id: 1, name: 'd', dataType: { type: 'unknown' }, nullable: false },
    { id: 2, name: 'keep', dataType: { type: 'boolean' }, nullable: false },
    { id: 3, name: 'e', dataType: { type: 'unknown' }, nullable: false },
  ],
}

const aliasValues = {
  d: [{ keep: false }, { keep: false }],
  keep: [true, true],
  e: [{ keep: false }, { keep: false }],
}

describe('batch aggregate execution', () => {
  it('counts and groups constant batches through prepared scans', async () => {
    /** @type {AsyncDataSource} */
    const source = {
      schema: { fields: [{ id: 1, name: 'day', dataType: { type: 'string' }, nullable: true }] },
      scanColumn() { throw new Error('COUNT must use prepared batches') },
      prepareScan(request) {
        return {
          schema: this.schema,
          properties: {},
          residual: { filter: request.filter },
          async *batches() {
            for (const [value, length] of [['a', 3], ['b', 5], ['a', 2], [null, 4]]) {
              yield {
                selection: { type: 'range', start: 1, end: Number(length), length: Number(length) },
                columns: [{ type: 'constant', value, length: Number(length) }],
              }
            }
          },
        }
      },
    }
    for (const [query, expected] of [
      ['SELECT COUNT(*) AS n FROM t WHERE day = \'a\'', [{ n: 3 }]],
      ['SELECT COUNT(*) AS n FROM t WHERE day = \'missing\'', [{ n: 0 }]],
      ['SELECT COUNT(*) AS n FROM t WHERE day IS NULL', [{ n: 3 }]],
      ['SELECT day, COUNT(*) AS n FROM t GROUP BY day ORDER BY day', [{ day: null, n: 3 }, { day: 'a', n: 3 }, { day: 'b', n: 4 }]],
      ['SELECT day, COUNT(*) AS n FROM t GROUP BY day HAVING COUNT(*) > 3', [{ day: 'b', n: 4 }]],
      ['SELECT day, COUNT(*), COUNT(day) FROM t GROUP BY day ORDER BY day', null],
    ]) {
      const rows = [{ day: 'a' }, { day: 'a' }, { day: 'b' }, { day: 'b' }, { day: 'b' }, { day: 'b' }, { day: 'a' }, { day: null }, { day: null }, { day: null }]
      const sql = String(query)
      expect(await collect(executeSql({ tables: { t: source }, query: sql }))).toEqual(
        expected ?? await collect(executeSql({ tables: { t: rows }, query: sql }))
      )
    }
  })

  it('groups production token expressions without using the row adapter', async () => {
    /** @type {ReadColumn} */
    function readAttributes({ selection }) {
      expect(selection).toEqual({ type: 'all', length: 4 })
      return {
        type: 'values',
        values: [
          { usage: { input_tokens: 10 } },
          { usage: { input_tokens: 7 } },
          { usage: { input_tokens: 20 } },
          { usage: {} },
        ],
        length: 4,
      }
    }
    const attributes = vi.fn(readAttributes)
    const batch = loadedBatch(attributes)
    /** @type {PrepareScan} */
    function prepareScan() {
      return {
        schema,
        residual: {},
        properties: { exactRows: 4, maxRows: 4 },
        async *batches() { yield batch },
      }
    }
    /** @type {AsyncDataSource} */
    const source = {
      columns: schema.fields.map(function fieldName(field) { return field.name }),
      schema,
      prepareScan,
      scan: vi.fn(function scan() { throw new Error('legacy row scan should not be called') }),
    }

    const results = executeSql({
      tables: { messages: source },
      query: `SELECT provider,
        COUNT(*) AS parts,
        COUNT(DISTINCT session_id) AS sessions,
        COALESCE(SUM(CAST(JSON_EXTRACT(attributes, '$.usage.input_tokens') AS BIGINT)), 0) AS tokens
        FROM messages GROUP BY provider ORDER BY provider`,
    })

    expect(await collect(results)).toEqual([
      { provider: 'claude', parts: 2, sessions: 2, tokens: 30 },
      { provider: 'codex', parts: 2, sessions: 1, tokens: 7 },
    ])
    expect(source.scan).not.toHaveBeenCalled()
    expect(attributes).toHaveBeenCalledTimes(1)
  })

  it('aggregates CASE, compound predicates, and NULLIF from native batches', async () => {
    const source = columnSource([1, 2, 3, 4])

    await expect(collect(executeSql({
      tables: { data: source },
      query: `SELECT
        SUM(CASE WHEN id > 1 AND id < 4 THEN id ELSE 0 END) AS middle_total,
        SUM(NULLIF(id, 2)) AS without_two
        FROM data`,
    }))).resolves.toEqual([{ middle_total: 5, without_two: 8 }])
    expect(source.scan).not.toHaveBeenCalled()
  })

  it('keeps the sessions title query streaming across 50000 native rows', async () => {
    const length = 50000
    /** @type {Record<string, SqlPrimitive>[]} */
    const rows = Array.from({ length }, (_, index) => ({
      session_id: 'session-' + index % 100,
      message_id: 'message-' + index,
      part_type: index >= length - 300 ? 'session_metadata' : 'text',
      attributes: index >= length - 300 ? JSON.stringify({ session: {
        title: index < length - 200 ? 'Zulu' : index < length - 100 ? 'Alpha' : 'Zulu',
        title_updated_at: '2026-10-' + (index < length - 200 ? '21' : index < length - 100 ? '22' : '23'),
      } }) : null,
    }))
    /** @type {RelationSchema} */
    const titleSchema = { fields: Object.keys(rows[0]).map(function field(name, index) {
      return { id: index + 1, name, dataType: { type: 'string' }, nullable: true }
    }) }
    const source = preparedSource(titleSchema, Object.fromEntries(titleSchema.fields.map(function column(field) {
      return [field.name, rows.map(function value(row) { return row[field.name] })]
    })))
    const query = `SELECT session_id,
      COUNT(CASE WHEN part_type != 'session_metadata' THEN 1 END) AS parts,
      COUNT(DISTINCT CASE WHEN part_type != 'session_metadata' THEN message_id END) AS messages,
      MAX_BY(CASE WHEN part_type = 'session_metadata' THEN JSON_EXTRACT(attributes, '$.session.title') END,
        CASE WHEN part_type = 'session_metadata' THEN JSON_EXTRACT(attributes, '$.session.title_updated_at') END) AS title,
      MAX(CASE WHEN part_type = 'session_metadata' THEN JSON_EXTRACT(attributes, '$.session.title_updated_at') END) AS updated
      FROM messages GROUP BY session_id`
    const tables = { messages: source }
    const plan = planSql({ query, tables })
    if (plan.type !== 'HashAggregate') throw new Error('expected hash aggregate')
    expect(planStreamingAggregates(plan)?.needsRow).toBe(false)
    const result = await collect(executeSql({ query, tables }))
    expect(result).toHaveLength(100)
    for (const row of result) {
      expect(row).toEqual({ session_id: row.session_id, parts: 497, messages: 497, title: 'Zulu', updated: '2026-10-23' })
    }
  })

  it('preserves paired aggregates, nulls and first ties alongside single-argument aggregates', async () => {
    const source = preparedSource({ fields: [
      { id: 1, name: 'value', dataType: { type: 'string' }, nullable: true },
      { id: 2, name: 'rank', dataType: { type: 'number' }, nullable: true },
    ] }, { value: ['Zulu', 'first', 'tie', null, 'ignored'], rank: [3, 1, 1, 0, null] })
    await expect(collect(executeSql({ tables: { data: source }, query: `SELECT
      COUNT(*) AS n, MIN_BY(value, rank) AS lo, SUM(rank) AS total,
      MAX_BY(value, rank) AS hi, COUNT(rank) AS ranked,
      ARG_MIN(value, rank) AS alias_lo, ARG_MAX(value, rank) AS alias_hi FROM data`,
    }))).resolves.toEqual([{ n: 5, lo: 'first', total: 5, hi: 'Zulu', ranked: 4, alias_lo: 'first', alias_hi: 'Zulu' }])
  })

  it('accepts asynchronous column results from another realm', async () => {
    const source = preparedSource({
      fields: [{ id: 1, name: 'id', dataType: { type: 'number' }, nullable: false }],
    }, {
      id: [1, 2],
    }, true)

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT SUM(id) AS total FROM data',
    }))).resolves.toEqual([{ total: 3 }])
  })

  it('keeps unambiguous qualified aggregate inputs on the native batch path', async () => {
    const id = 8675309
    const source = preparedSource({
      fields: [{ id: 1, name: 'id', dataType: { type: 'number' }, nullable: false }],
    }, {
      id: [id],
    })
    const resolve = vi.spyOn(Promise, 'resolve')

    try {
      await expect(collect(executeSql({
        tables: { data: source },
        query: 'SELECT SUM(d.id) AS total FROM data d',
      }))).resolves.toEqual([{ total: id }])
      expect(resolve.mock.calls.some(function resolvedSourceValue([value]) {
        return value === id
      })).toBe(false)
    } finally {
      resolve.mockRestore()
    }
  })

  it('preserves struct-field precedence over a bare column', async () => {
    const source = columnSource([{ x: 2 }, { x: 3 }])

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT SUM(obj.x) AS total FROM (SELECT id AS obj, 99 AS x FROM data)',
    }))).resolves.toEqual([{ total: 5 }])
  })

  it('preserves table-alias precedence in batch aggregate inputs', async () => {
    const source = preparedSource(aliasSchema, aliasValues)

    await expect(collect(executeSql({
      tables: { data: source },
      query: `SELECT d.keep AS grouped, MAX(d) AS max_d, COUNTIF(d.keep) AS matching
        FROM data d GROUP BY d.keep`,
    }))).resolves.toEqual([{
      grouped: true,
      max_d: { keep: false },
      matching: 2,
    }])
  })

  it('preserves table-alias precedence in compound batch aggregate inputs', async () => {
    const source = preparedSource(aliasSchema, aliasValues)

    await expect(collect(executeSql({
      tables: { data: source },
      query: `SELECT d.keep AS grouped, MAX(d) AS max_d, COUNTIF(d.keep) AS matching
        FROM data d GROUP BY d.keep
        UNION ALL
        SELECT e.keep AS grouped, MAX(e) AS max_d, COUNTIF(e.keep) AS matching
        FROM data e GROUP BY e.keep`,
    }))).resolves.toEqual([
      { grouped: true, max_d: { keep: false }, matching: 2 },
      { grouped: true, max_d: { keep: false }, matching: 2 },
    ])
  })

  it('preserves outer references in aggregate inputs', async () => {
    await expect(collect(executeSql({
      tables: {
        users: [{ id: 1 }, { id: 2 }],
        orders: columnSource([10, 20]),
      },
      query: `SELECT u.id,
        (SELECT SUM(u.id + id) FROM orders) AS total
        FROM users u ORDER BY u.id`,
    }))).resolves.toEqual([
      { id: 1, total: 32 },
      { id: 2, total: 34 },
    ])
  })
})

/**
 * @param {ReadColumn} readAttributes
 * @returns {AsyncBatch}
 */
function loadedBatch(readAttributes) {
  return {
    selection: { type: 'all', length: 4 },
    columns: [
      { type: 'values', values: ['claude', 'codex', 'claude', 'codex'], length: 4 },
      { type: 'values', values: ['a', 'b', 'c', 'b'], length: 4 },
      { read: readAttributes },
    ],
  }
}

/**
 * @param {import('../../src/types.js').SqlPrimitive[]} values
 * @returns {AsyncDataSource}
 */
function columnSource(values) {
  return {
    columns: ['id'],
    scan: vi.fn(function scan() {
      throw new Error('row scan should not be called')
    }),
    scanColumn() {
      /** @type {ScanColumnResults} */
      const results = {
        appliedWhere: false,
        appliedLimitOffset: false,
        async *chunks() { yield values },
      }
      return results
    },
  }
}

/**
 * @param {RelationSchema} sourceSchema
 * @param {Record<string, import('../../src/types.js').SqlPrimitive[]>} values
 * @param {boolean} [foreignPromise]
 * @returns {AsyncDataSource}
 */
function preparedSource(sourceSchema, values, foreignPromise) {
  const length = Object.values(values)[0]?.length ?? 0
  return {
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
        residual: {},
        properties: { exactRows: length },
        async *batches() {
          yield {
            selection: { type: 'all', length },
            columns: fields.map(function loadedField(field) {
              /** @type {import('../../src/types.js').ColumnVector} */
              const vector = { type: 'values', values: values[field.name], length }
              if (!foreignPromise) return vector
              return {
                read() {
                  return runInNewContext('Promise.resolve(vector)', { vector })
                },
              }
            }),
          }
        },
      }
    },
  }
}
