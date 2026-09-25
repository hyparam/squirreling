import { describe, expect, it } from 'vitest'
import { collect, executePlan, executeSql, parseSql, planSql } from '../../src/index.js'

/**
 * @import {AsyncDataSource, RelationSchema, ScanRequest, SqlPrimitive} from '../../src/types.js'
 */

/** @type {Record<string, SqlPrimitive>[]} */
const data = [
  { id: 0, date: 20 },
  { id: 1, date: null },
  { id: 2, date: 40 },
  { id: 3, date: 40 },
  { id: 4, date: 10 },
]

/**
 * A prepared source that deliberately ignores Top-K hints.
 *
 * @returns {{ source: AsyncDataSource, requests: ScanRequest[] }}
 */
function fixture() {
  /** @type {ScanRequest[]} */
  const requests = []
  /** @type {RelationSchema} */
  const schema = { fields: [
    { id: 10, name: 'id', dataType: { type: 'number' }, nullable: false },
    { id: 20, name: 'date', dataType: { type: 'number' }, nullable: true },
  ] }
  /** @type {AsyncDataSource} */
  const source = {
    schema,
    prepareScan(request) {
      requests.push(request)
      const fields = request.columns.map(d => schema.fields.find(f => f.id === d.field))
      return {
        schema: { fields }, properties: { exactRows: data.length },
        residual: { filter: request.filter, limit: request.limit, offset: request.offset },
        async *batches() {
          yield {
            selection: { type: 'all', length: data.length },
            columns: fields.map(f => ({ type: 'values', values: data.map(r => r[f.name]), length: data.length })),
          }
        },
      }
    },
  }
  return { source, requests }
}

describe('Top-K scan hints', () => {
  it('uses resolved aliases and field ids, includes OFFSET, and retains the final sort', async () => {
    const { source, requests } = fixture()
    const query = 'SELECT id, t.date AS d FROM t ORDER BY d DESC NULLS LAST LIMIT 2 OFFSET 1'
    expect(await collect(executeSql({ query, tables: { t: source } })))
      .toEqual([{ id: 3, d: 40 }, { id: 0, d: 20 }])
    expect(requests[0].topK).toEqual({
      orderBy: [{ field: 20, direction: 'DESC', nulls: 'LAST' }], limit: 3,
    })
    expect(requests[0].limit).toBeUndefined()
    expect(requests[0].offset).toBeUndefined()
  })

  it('carries multiple sort terms and the engine default NULLS FIRST', async () => {
    const { source, requests } = fixture()
    await collect(executeSql({ query: 'SELECT id FROM t ORDER BY date DESC, id ASC LIMIT 2', tables: { t: source } }))
    expect(requests[0].topK).toEqual({ orderBy: [
      { field: 20, direction: 'DESC', nulls: 'FIRST' },
      { field: 10, direction: 'ASC', nulls: 'FIRST' },
    ], limit: 2 })
  })

  it('allows ignoring the hint when the scan has a residual filter', async () => {
    const { source, requests } = fixture()
    const rows = await collect(executeSql({
      query: 'SELECT id FROM t WHERE date < 40 ORDER BY date DESC LIMIT 1', tables: { t: source },
    }))
    expect(rows).toEqual([{ id: 0 }])
    expect(requests[0].filter).toBeDefined()
    expect(requests[0].topK).toBeDefined()
  })

  it.each([
    'SELECT id FROM t ORDER BY date + 1 LIMIT 2',
    'SELECT id FROM t ORDER BY date',
    'SELECT DISTINCT date FROM t ORDER BY date LIMIT 2',
    'SELECT date, COUNT(*) AS n FROM t GROUP BY date ORDER BY date LIMIT 2',
    'SELECT id, ROW_NUMBER() OVER () AS n FROM t ORDER BY date LIMIT 2',
    'SELECT t.id FROM t JOIN u ON t.id = u.id ORDER BY t.date LIMIT 2',
    'WITH c AS (SELECT * FROM t LIMIT 3) SELECT id FROM c ORDER BY date LIMIT 2',
  ])('does not send unsafe hints: %s', async query => {
    const { source, requests } = fixture()
    const expected = await collect(executeSql({ query, tables: { t: data, u: data } }))
    expect(await collect(executeSql({ query, tables: { t: source, u: data } }))).toEqual(expected)
    expect(requests.length).toBeGreaterThan(0)
    expect(requests.every(request => request.topK === undefined)).toBe(true)
  })

  it('does not mutate reusable query plans', async () => {
    const { source, requests } = fixture()
    const tables = { t: source }
    const plan = planSql({ query: parseSql({ query: 'SELECT * FROM t ORDER BY date LIMIT 2' }), tables })
    const before = JSON.stringify(plan)
    await collect(executePlan({ plan, context: { tables } }))
    expect(requests[0].topK).toBeDefined()
    expect(JSON.stringify(plan)).toBe(before)
  })
})
