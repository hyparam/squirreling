import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { collect, executePlan, executeSql, planSql } from '../../src/index.js'
import { parseSql } from '../../src/parse/parse.js'

/**
 * @import { AsyncDataSource } from '../../src/types.js'
 */

describe('correlated EXISTS joins', () => {
  it.each(['EXISTS', 'NOT EXISTS', 'NOT (EXISTS'])('builds one inner input for %s and leaves reusable ASTs unchanged', async operator => {
    let scans = 0
    const data = [{ id: 2, active: true }, { id: 2, active: true }, { id: null, active: true }, { id: 1, active: false }]
    const source = memorySource({ data })
    /** @type {Record<string, AsyncDataSource>} */
    const tables = {
      c: memorySource({ data: [{ id: 1 }, { id: 2 }, { id: 2 }, { id: null }] }),
      r: { columns: ['id', 'active'], scan(options) { scans++; return source.scan(options) } },
    }
    const query = parseSql({ query: `SELECT c.id FROM c WHERE ${operator} (SELECT 1 FROM r WHERE r.id = c.id AND r.active = true)${operator === 'NOT (EXISTS' ? ')' : ''}` })
    const original = structuredClone(query)
    const plan = planSql({ query, tables })
    expect(plan.type).toBe('Project')
    if (plan.type !== 'Project') throw new Error('expected project')
    expect(plan.child.type).toBe('SemiJoin')
    if (plan.child.type !== 'SemiJoin') throw new Error('expected semi join')
    expect(plan.child.not).toBe(operator !== 'EXISTS')
    const result = executePlan({ plan, context: { tables } })
    const expected = operator === 'EXISTS' ? [{ id: 2 }, { id: 2 }] : [{ id: 1 }, { id: null }]
    expect(await collect(result)).toEqual(expected)
    expect(scans).toBe(1)
    expect(await collect(result)).toEqual(expected)
    expect(scans).toBe(2)
    expect(query).toEqual(original)
  })

  it('matches whole tuples, reversed equalities and coercive SQL equality', async () => {
    const tables = { c: [{ id: 1, k: '2' }, { id: 1, k: 3 }, { id: 2, k: 2 }, { id: null, k: 2 }], r: [{ id: 1, k: 2 }, { id: 2, k: 3 }] }
    expect(await collect(executeSql({ tables, query: 'SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM r WHERE c.id = r.id AND r.k = c.k)' }))).toEqual([{ id: 1 }])
  })

  it.each([{ data: [] }, { data: [{ id: null }] }, { data: [{ id: 2 }, { id: null }] }])('preserves NULL and empty inner semantics for %j', async ({ data }) => {
    const tables = { c: [{ id: 1 }, { id: 2 }, { id: null }], r: memorySource({ data, columns: ['id'] }) }
    const included = data.some(row => row.id === 2) ? [{ id: 2 }] : []
    const excluded = included.length ? [{ id: 1 }, { id: null }] : tables.c
    expect(await collect(executeSql({ tables, query: 'SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM r WHERE r.id = c.id)' }))).toEqual(included)
    expect(await collect(executeSql({ tables, query: 'SELECT c.id FROM c WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.id = c.id)' }))).toEqual(excluded)
  })

  it.each([
    ['r.id > c.id', [{ id: 1 }]],
    ['r.id = c.id + 1', [{ id: 1 }]],
    ['r.id = c.id OR r.id > c.id', [{ id: 1 }, { id: 2 }]],
  ])('keeps unsupported correlation on the fallback: %s', async (condition, expected) => {
    const tables = { c: memorySource({ data: [{ id: 1 }, { id: 2 }] }), r: memorySource({ data: [{ id: 2 }] }) }
    const query = `SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM r WHERE ${condition})`
    const plan = planSql({ tables, query })
    if (plan.type !== 'Project') throw new Error('expected project')
    expect(plan.child.type).toBe('Scan')
    expect(await collect(executeSql({ tables, query }))).toEqual(expected)
  })

  it.each([
    ['SELECT count(*) FROM r WHERE r.id = c.id', [{ id: 1 }, { id: 2 }]],
    ['SELECT 1 FROM r WHERE r.id = c.id LIMIT 0', []],
    ['SELECT 1 FROM r WHERE r.id = c.id LIMIT 1 OFFSET 1', []],
  ])('retains per-row aggregate and limit semantics: %s', async (inner, expected) => {
    const tables = { c: [{ id: 1 }, { id: 2 }], r: [{ id: 2 }] }
    expect(await collect(executeSql({ tables, query: `SELECT c.id FROM c WHERE EXISTS (${inner})` }))).toEqual(expected)
  })

  it('optimizes positive correlated IN and evaluates NOT IN with outer scope', async () => {
    const tables = { c: memorySource({ data: [{ id: 1 }, { id: 2 }, { id: null }] }), r: memorySource({ data: [{ id: 2 }, { id: null }] }) }
    const query = 'SELECT c.id FROM c WHERE c.id IN (SELECT r.id FROM r WHERE r.id = c.id)'
    const plan = planSql({ tables, query })
    if (plan.type !== 'Project') throw new Error('expected project')
    expect(plan.child.type).toBe('SemiJoin')
    expect(await collect(executeSql({ tables, query }))).toEqual([{ id: 2 }])
    expect(await collect(executeSql({ tables, query: query.replace(' IN ', ' NOT IN ') }))).toEqual([{ id: 1 }, { id: null }])
    expect(await collect(executeSql({ tables, query: 'SELECT c.id FROM c WHERE c.id NOT IN (SELECT r.id FROM r)' }))).toEqual([])
  })

  it('preserves residual outer references and alias shadowing', async () => {
    const tables = { c: [{ id: 1 }, { id: 2 }], r: [{ id: 2 }] }
    expect(await collect(executeSql({ tables, query: 'SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM r WHERE r.id = c.id AND c.id > 2)' }))).toEqual([])
    expect(await collect(executeSql({ tables, query: 'SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM r c WHERE c.id = c.id)' }))).toEqual([{ id: 1 }, { id: 2 }])
  })

  it('resolves CTEs and applies outer LIMIT after membership', async () => {
    const tables = { c: [{ id: 1 }, { id: 2 }, { id: 3 }], r: [{ id: 2 }, { id: 3 }] }
    expect(await collect(executeSql({ tables, query: 'WITH wanted AS (SELECT id FROM r) SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM wanted r WHERE r.id = c.id) LIMIT 1 OFFSET 1' }))).toEqual([{ id: 3 }])
  })

  it.each([
    { probe: new Date(1000), value: new Date(1000), matches: true },
    { probe: NaN, value: NaN, matches: false },
    { probe: false, value: 0, matches: true },
    { probe: 2n, value: '2', matches: true },
  ])('preserves equality for $probe and $value', async ({ probe, value, matches }) => {
    const tables = { c: [{ id: probe }], r: [{ id: value }] }
    expect(await collect(executeSql({ tables, query: 'SELECT c.id FROM c WHERE EXISTS (SELECT 1 FROM r WHERE r.id = c.id)' }))).toEqual(matches ? [{ id: probe }] : [])
  })

})
