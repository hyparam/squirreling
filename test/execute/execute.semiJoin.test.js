import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { collect, executePlan, executeSql, planSql } from '../../src/index.js'
import { parseSql } from '../../src/parse/parse.js'

/**
 * @import { AsyncDataSource } from '../../src/types.js'
 */

describe('IN semi-join', () => {
  it('scans once per iteration and keeps reusable plans and ASTs stateless', async () => {
    const data = [{ id: 2 }, { id: null }]
    const source = memorySource({ data })
    let scans = 0
    /** @type {AsyncDataSource} */
    const wanted = {
      columns: ['id'],
      scan(options) {
        scans++
        return source.scan(options)
      },
    }
    const query = parseSql({ query: 'SELECT id FROM items WHERE id IN (SELECT id FROM wanted)' })
    const original = structuredClone(query)
    const tables = { items: memorySource({ data: [{ id: 1 }, { id: 2 }, { id: 2 }, { id: null }, { id: 3 }] }), wanted }
    const plan = planSql({ query, tables })
    if (plan.type !== 'Project') throw new Error('expected project')
    expect(plan.child.type).toBe('SemiJoin')
    const originalPlan = structuredClone(plan)
    const result = executePlan({ plan, context: { tables } })
    expect(await collect(result)).toEqual([{ id: 2 }, { id: 2 }])
    expect(scans).toBe(1)
    data[0].id = 3
    expect(await collect(result)).toEqual([{ id: 3 }])
    expect(scans).toBe(2)
    expect(await collect(executePlan({ plan, context: { tables } }))).toEqual([{ id: 3 }])
    expect(scans).toBe(3)
    expect(query).toEqual(original)
    expect(plan).toEqual(originalPlan)
  })

  it.each([
    { values: [], included: [], excluded: [1, 2, null] },
    { values: [null], included: [], excluded: [] },
    { values: [2, 2], included: [2], excluded: [1] },
    { values: [2, null], included: [2], excluded: [] },
  ])('preserves empty-set, duplicate and NULL semantics for $values', async ({ values, included, excluded }) => {
    const tables = { items: [{ id: 1 }, { id: 2 }, { id: null }], wanted: memorySource({ data: values.map(id => ({ id })), columns: ['id'] }) }
    expect(await collect(executeSql({ tables, query: 'SELECT id FROM items WHERE id IN (SELECT id FROM wanted)' }))).toEqual(included.map(id => ({ id })))
    expect(await collect(executeSql({ tables, query: 'SELECT id FROM items WHERE id NOT IN (SELECT id FROM wanted)' }))).toEqual(excluded.map(id => ({ id })))
  })

  it.each([
    { probe: '2', value: 2, expected: true },
    { probe: 2n, value: '2', expected: true },
    { probe: false, value: 0, expected: true },
    { probe: new Date(1000), value: new Date(1000), expected: true },
    { probe: NaN, value: NaN, expected: false },
  ])('preserves SQL equality for $probe and $value', async ({ probe, value, expected }) => {
    expect(await collect(executeSql({ tables: { items: [{ id: probe }], wanted: [{ id: value }] }, query: 'SELECT id FROM items WHERE id IN (SELECT id FROM wanted)' }))).toEqual(expected ? [{ id: probe }] : [])
  })

  it('resolves CTEs when planning the right input', async () => {
    expect(await collect(executeSql({ tables: { items: [{ id: 1 }, { id: 2 }] }, query: 'WITH wanted AS (SELECT id FROM items WHERE id = 2) SELECT id FROM items WHERE id IN (SELECT id FROM wanted)' }))).toEqual([{ id: 2 }])
  })

  it('applies LIMIT and OFFSET after membership filtering', async () => {
    const tables = { items: [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }], wanted: [{ id: 2 }, { id: 3 }, { id: 4 }] }
    expect(await collect(executeSql({ tables, query: 'SELECT id FROM items WHERE id IN (SELECT id FROM wanted) LIMIT 1 OFFSET 1' }))).toEqual([{ id: 3 }])
    expect(await collect(executeSql({ tables, query: 'SELECT count(*) AS n FROM items WHERE id IN (SELECT id FROM wanted)' }))).toEqual([{ n: 3 }])
  })
})
