import { describe, expect, it } from 'vitest'
import { collect, executeSql, planSql } from '../../src/index.js'

/**
 * @import {AsyncDataSource, Field, ScanRequest, SqlPrimitive} from '../../src/types.js'
 */

describe('constant sort keys', () => {
  it('removes a string sort fixed by equality', () => {
    const { source } = preparedTable([])
    const plan = planSql({ query: 'SELECT id FROM t WHERE role = \'assistant\' ORDER BY role LIMIT 1', tables: { t: source } })
    expect(plan).toEqual(planSql({ query: 'SELECT id FROM t WHERE role = \'assistant\' LIMIT 1', tables: { t: source } }))
  })

  it('stops reading after the requested number of matching rows', async () => {
    const { source, stats, requests } = preparedTable([
      { id: 1, role: 'user' }, { id: 2, role: 'assistant' }, { id: 3, role: 'assistant' },
    ])
    const query = 'SELECT id FROM t WHERE role = \'assistant\' ORDER BY role LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2 }])
    expect(stats.rowsRead).toBe(2)
    expect(requests[0].topK).toBeUndefined()
    expect(requests[0].limit).toBe(1)
  })

  it('counts matching rows rather than physical rows for OFFSET', async () => {
    const { source, stats } = preparedTable([
      { id: 1, role: 'user' }, { id: 2, role: 'assistant' }, { id: 3, role: 'user' },
      { id: 4, role: 'assistant' }, { id: 5, role: 'assistant' },
    ])
    const query = 'SELECT id FROM t WHERE role = \'assistant\' ORDER BY role LIMIT 1 OFFSET 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 4 }])
    expect(stats.rowsRead).toBe(4)
  })

  it('recognizes equality with the literal on the left', () => {
    const { source } = preparedTable([])
    const plan = planSql({ query: 'SELECT id FROM t WHERE \'assistant\' = role ORDER BY role LIMIT 1', tables: { t: source } })
    expect(plan).toEqual(planSql({ query: 'SELECT id FROM t WHERE \'assistant\' = role LIMIT 1', tables: { t: source } }))
  })

  it('finds a fixed sort key inside a conjunction', () => {
    const { source } = preparedTable([])
    const plan = planSql({ query: 'SELECT id FROM t WHERE id > 0 AND role = \'assistant\' ORDER BY role LIMIT 1', tables: { t: source } })
    expect(plan).toEqual(planSql({ query: 'SELECT id FROM t WHERE id > 0 AND role = \'assistant\' LIMIT 1', tables: { t: source } }))
  })

  it('resolves a projected alias before eliminating its sort', () => {
    const { source } = preparedTable([])
    const plan = planSql({ query: 'SELECT role AS kind FROM t WHERE role = \'assistant\' ORDER BY kind LIMIT 1', tables: { t: source } })
    expect(plan).toEqual(planSql({ query: 'SELECT role AS kind FROM t WHERE role = \'assistant\' LIMIT 1', tables: { t: source } }))
  })

  it('recognizes a qualified reference to the same local column', () => {
    const { source } = preparedTable([])
    const plan = planSql({ query: 'SELECT id FROM t AS a WHERE a.role = \'assistant\' ORDER BY role LIMIT 1', tables: { t: source } })
    expect(plan).toEqual(planSql({ query: 'SELECT id FROM t AS a WHERE a.role = \'assistant\' LIMIT 1', tables: { t: source } }))
  })

  it('removes an integer sort fixed by equality', () => {
    const { source } = preparedTable([])
    const plan = planSql({ query: 'SELECT id FROM t WHERE id = 5 ORDER BY id LIMIT 1', tables: { t: source } })
    expect(plan).toEqual(planSql({ query: 'SELECT id FROM t WHERE id = 5 LIMIT 1', tables: { t: source } }))
  })

  it('eliminates a sort whose key is always null', async () => {
    const { source, stats } = preparedTable([{ id: 1, role: null }, { id: 2, role: null }])
    const query = 'SELECT id FROM t WHERE role IS NULL ORDER BY role LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 1 }])
    expect(stats.rowsRead).toBe(1)
  })

  it('keeps a varying secondary sort key', async () => {
    const { source } = preparedTable([{ id: 1, role: 'assistant' }, { id: 2, role: 'assistant' }])
    const query = 'SELECT id FROM t WHERE role = \'assistant\' ORDER BY role, id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2 }])
  })

  it('does not infer a fixed value from an OR branch', async () => {
    const { source } = preparedTable([{ id: 1, role: 'user' }, { id: 2, role: 'assistant' }])
    const query = 'SELECT id FROM t WHERE role = \'assistant\' OR id = 1 ORDER BY role LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2 }])
  })

  it('keeps sorting when numeric equality matches different strings', async () => {
    const { source } = preparedTable([{ id: 1, role: '01' }, { id: 2, role: '1' }])
    const query = 'SELECT id FROM t WHERE role = 1 ORDER BY role DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2 }])
  })

  it('does not infer constant ordering for an unknown column type', async () => {
    const { source } = preparedTable([{ id: 1, role: '01' }, { id: 2, role: '1' }])
    source.schema.fields[1].dataType = { type: 'unknown' }
    const query = 'SELECT id FROM t WHERE role = 1 ORDER BY role DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2 }])
  })

  it('retains the sort on an aggregate result despite a same-named input filter', async () => {
    const { source } = preparedTable([
      { id: 5, role: 'a' }, { id: 5, role: 'b' }, { id: 5, role: 'b' },
    ])
    const query = 'SELECT role, COUNT(*) AS id FROM t WHERE id = 5 GROUP BY role ORDER BY id DESC LIMIT 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ role: 'b', id: 2 }])
  })

  it('preserves window numbering before an offset', async () => {
    const { source } = preparedTable([{ id: 1, role: 'assistant' }, { id: 2, role: 'assistant' }])
    const query = 'SELECT id, ROW_NUMBER() OVER () AS n FROM t WHERE role = \'assistant\' ORDER BY role LIMIT 1 OFFSET 1'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 2, n: 2 }])
  })

  it('does not push a limit below DISTINCT after eliminating the sort', async () => {
    const { source } = preparedTable([
      { id: 1, role: 'assistant' }, { id: 1, role: 'assistant' }, { id: 2, role: 'assistant' },
    ])
    const query = 'SELECT DISTINCT id FROM t WHERE role = \'assistant\' ORDER BY role LIMIT 2'
    expect(await collect(executeSql({ query, tables: { t: source } }))).toEqual([{ id: 1 }, { id: 2 }])
  })
})

/**
 * A typed source yielding one physical row per batch, leaving filtering to SQL.
 * @param {Record<string, SqlPrimitive>[]} data
 * @returns {{ source: AsyncDataSource & { schema: {fields: Field[]} }, stats: {rowsRead: number}, requests: ScanRequest[] }}
 */
function preparedTable(data) {
  const stats = { rowsRead: 0 }
  /** @type {ScanRequest[]} */
  const requests = []
  /** @type {AsyncDataSource & { schema: {fields: Field[]} }} */
  const source = {
    schema: { fields: [
      { id: 1, name: 'id', dataType: { type: 'number' }, nullable: false },
      { id: 2, name: 'role', dataType: { type: 'string' }, nullable: true },
    ] },
    prepareScan(request) {
      requests.push(request)
      const fields = request.columns.map(column => source.schema.fields.find(field => field.id === column.field))
      if (fields.some(field => !field)) throw new Error('Unknown requested field')
      const selected = fields.filter(field => field !== undefined)
      return {
        schema: { fields: selected }, properties: {},
        residual: { filter: request.filter, limit: request.limit, offset: request.offset },
        async *batches() {
          for (const row of data) {
            stats.rowsRead++
            yield {
              selection: { type: 'all', length: 1 },
              columns: selected.map(field => ({ type: 'values', values: [row[field.name]], length: 1 })),
            }
          }
        },
      }
    },
  }
  return { source, stats, requests }
}
