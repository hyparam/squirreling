import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { collect, executePlan, executeSql, planSql } from '../../src/index.js'
import { trackingSource } from './trackingSource.js'

describe('IN mark join', () => {
  it('scans the subquery once for a grouped membership key', async () => {
    const u = Array.from({ length: 400 }, (_, i) => ({ message_id: i, tokens: 1 }))
    const poll = trackingSource(Array.from({ length: 100 }, (_, i) => ({ message_id: i * 2 })))
    const tables = { u, poll: poll.source }
    const result = await collect(executeSql({
      tables,
      query: 'SELECT message_id IN (SELECT message_id FROM poll) AS is_poll, SUM(tokens) AS tokens FROM u GROUP BY 1 ORDER BY is_poll',
    }))
    expect(result).toEqual([{ is_poll: false, tokens: 300 }, { is_poll: true, tokens: 100 }])
    expect(poll.getScanCount()).toBe(1)
    expect(poll.getRowCount()).toBe(100)
  })

  it('handles the report shape with CTEs', async () => {
    const tables = { messages: [{ id: 1, kind: 'poll' }, { id: 2, kind: 'chat' }, { id: 3, kind: 'poll' }] }
    const result = await collect(executeSql({
      tables,
      query: `WITH poll AS (SELECT id FROM messages WHERE kind = 'poll'), u AS (SELECT id FROM messages)
        SELECT id IN (SELECT id FROM poll) AS is_poll, COUNT(*) AS n FROM u GROUP BY 1 ORDER BY 1`,
    }))
    expect(result).toEqual([{ is_poll: false, n: 1 }, { is_poll: true, n: 2 }])
  })

  it('plans a mark join and scans once per iteration', async () => {
    const wanted = trackingSource([{ id: 2 }])
    const tables = { items: memorySource({ data: [{ id: 1 }, { id: 2 }, { id: 3 }] }), wanted: wanted.source }
    const plan = planSql({ query: 'SELECT id, id IN (SELECT id FROM wanted) AS hit FROM items', tables })
    if (plan.type !== 'Project') throw new Error('expected project')
    expect(plan.child.type).toBe('SemiJoin')
    const original = structuredClone(plan)
    const result = executePlan({ plan, context: { tables } })
    const expected = [{ id: 1, hit: false }, { id: 2, hit: true }, { id: 3, hit: false }]
    expect(await collect(result)).toEqual(expected)
    expect(await collect(result)).toEqual(expected)
    expect(wanted.getScanCount()).toBe(2)
    expect(plan).toEqual(original)
  })

  it.each([
    { values: [], hit: [false, false, false] },
    { values: [null], hit: [null, null, null] },
    { values: [2, 2], hit: [false, true, null] },
    { values: [2, null], hit: [null, true, null] },
  ])('preserves empty-set, duplicate and NULL semantics for $values', async ({ values, hit }) => {
    const tables = { items: [{ id: 1 }, { id: 2 }, { id: null }], wanted: memorySource({ data: values.map(id => ({ id })), columns: ['id'] }) }
    expect(await collect(executeSql({ tables, query: 'SELECT id IN (SELECT id FROM wanted) AS hit FROM items' }))).toEqual(hit.map(hit => ({ hit })))
    expect(await collect(executeSql({ tables, query: 'SELECT id NOT IN (SELECT id FROM wanted) AS miss FROM items' }))).toEqual(hit.map(hit => ({ miss: hit === null ? null : !hit })))
  })

  it('keeps the derived column name', async () => {
    const tables = { items: [{ id: 1 }], wanted: [{ id: 1 }] }
    expect(await collect(executeSql({ tables, query: 'SELECT id IN (SELECT id FROM wanted) FROM items' }))).toEqual([{ expr: true }])
  })

  it('evaluates membership inside aggregate arguments', async () => {
    const wanted = trackingSource([{ id: 1 }, { id: 3 }])
    const tables = { items: [{ id: 1 }, { id: 2 }, { id: 3 }], wanted: wanted.source }
    expect(await collect(executeSql({ tables, query: 'SELECT COUNTIF(id IN (SELECT id FROM wanted)) AS n FROM items' }))).toEqual([{ n: 2 }])
    expect(wanted.getScanCount()).toBe(1)
  })

  it('orders by membership', async () => {
    const tables = { items: [{ id: 1 }, { id: 2 }, { id: 3 }], wanted: [{ id: 2 }] }
    expect(await collect(executeSql({ tables, query: 'SELECT id FROM items ORDER BY id IN (SELECT id FROM wanted) DESC, id' }))).toEqual([{ id: 2 }, { id: 1 }, { id: 3 }])
  })

  it('plans correlated IN as a keyed mark join', async () => {
    const wanted = trackingSource([{ id: 1, g: 'a' }, { id: 2, g: 'a' }])
    const tables = { items: memorySource({ data: [{ id: 1, g: 'a' }, { id: 2, g: 'b' }] }), wanted: wanted.source }
    const query = 'SELECT id, id IN (SELECT w.id FROM wanted w WHERE w.g = items.g) AS hit FROM items'
    const plan = planSql({ query, tables })
    if (plan.type !== 'Project') throw new Error('expected project')
    expect(plan.child.type).toBe('SemiJoin')
    expect(await collect(executeSql({ tables, query }))).toEqual([{ id: 1, hit: true }, { id: 2, hit: false }])
    expect(wanted.getScanCount()).toBe(1)
  })

  it('preserves NULL semantics per correlation key', async () => {
    const tables = {
      items: [{ id: 1, g: 'a' }, { id: 3, g: 'a' }, { id: null, g: 'a' }, { id: 1, g: 'b' }, { id: null, g: 'b' }, { id: 1, g: 'c' }, { id: 1, g: null }],
      wanted: [{ id: 1, g: 'a' }, { id: null, g: 'a' }, { id: 2, g: 'b' }, { id: 1, g: null }],
    }
    const query = 'SELECT id IN (SELECT w.id FROM wanted w WHERE w.g = items.g) AS hit, id NOT IN (SELECT w.id FROM wanted w WHERE w.g = items.g) AS miss FROM items'
    expect(await collect(executeSql({ tables, query }))).toEqual([
      { hit: true, miss: false },
      { hit: null, miss: null },
      { hit: null, miss: null },
      { hit: false, miss: true },
      { hit: null, miss: null },
      { hit: false, miss: true },
      { hit: false, miss: true },
    ])
  })

  it('matches correlation keys with SQL equality', async () => {
    const tables = { items: [{ id: 1, g: 2 }], wanted: [{ id: 1, g: '2' }] }
    expect(await collect(executeSql({ tables, query: 'SELECT id IN (SELECT w.id FROM wanted w WHERE w.g = items.g) AS hit FROM items' }))).toEqual([{ hit: true }])
  })

  it('leaves non-equality correlations on the per-row path', async () => {
    const tables = { items: memorySource({ data: [{ id: 1, g: 1 }, { id: 2, g: 5 }] }), wanted: memorySource({ data: [{ id: 1, g: 2 }, { id: 2, g: 2 }] }) }
    const query = 'SELECT id, id IN (SELECT w.id FROM wanted w WHERE w.g > items.g) AS hit FROM items'
    const plan = planSql({ query, tables })
    if (plan.type !== 'Project') throw new Error('expected project')
    expect(plan.child.type).toBe('Scan')
    expect(await collect(executeSql({ tables, query }))).toEqual([{ id: 1, hit: true }, { id: 2, hit: false }])
  })

  it('scans uncorrelated EXISTS once and stops at the first row', async () => {
    const wanted = trackingSource([{ id: 1 }, { id: 2 }, { id: 3 }])
    const tables = { items: [{ id: 1 }, { id: 2 }], wanted: wanted.source }
    expect(await collect(executeSql({ tables, query: 'SELECT id, EXISTS (SELECT id FROM wanted) AS a, NOT EXISTS (SELECT id FROM wanted) AS b FROM items' }))).toEqual([
      { id: 1, a: true, b: false },
      { id: 2, a: true, b: false },
    ])
    expect(wanted.getScanCount()).toBe(2)
    expect(wanted.getRowCount()).toBe(2)
  })

  it('evaluates uncorrelated EXISTS over an empty set', async () => {
    const tables = { items: [{ id: 1 }], wanted: memorySource({ data: [], columns: ['id'] }) }
    expect(await collect(executeSql({ tables, query: 'SELECT EXISTS (SELECT id FROM wanted) AS a, NOT EXISTS (SELECT id FROM wanted) AS b FROM items' }))).toEqual([{ a: false, b: true }])
  })

  it('plans correlated EXISTS as a keyed mark join', async () => {
    const orders = trackingSource([{ user_id: 1 }, { user_id: 1 }, { user_id: null }])
    const tables = { users: [{ id: 1 }, { id: 2 }, { id: null }], orders: orders.source }
    const result = await collect(executeSql({
      tables,
      query: 'SELECT id, EXISTS (SELECT 1 FROM orders o WHERE o.user_id = users.id) AS has, NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = users.id) AS none FROM users',
    }))
    expect(result).toEqual([
      { id: 1, has: true, none: false },
      { id: 2, has: false, none: true },
      { id: null, has: false, none: true },
    ])
    expect(orders.getScanCount()).toBe(2)
  })

  it('groups by correlated EXISTS', async () => {
    const tables = { users: [{ id: 1 }, { id: 2 }, { id: 3 }], orders: [{ user_id: 1 }, { user_id: 3 }] }
    expect(await collect(executeSql({
      tables,
      query: 'SELECT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = users.id) AS buyer, COUNT(*) AS n FROM users GROUP BY 1 ORDER BY 1',
    }))).toEqual([{ buyer: false, n: 1 }, { buyer: true, n: 2 }])
  })

  it('leaves membership on aggregate results on the per-row path', async () => {
    const tables = { items: [{ g: 'a' }, { g: 'a' }, { g: 'b' }], wanted: [{ n: 2 }] }
    expect(await collect(executeSql({ tables, query: 'SELECT g, COUNT(*) IN (SELECT n FROM wanted) AS hit FROM items GROUP BY g ORDER BY g' }))).toEqual([{ g: 'a', hit: true }, { g: 'b', hit: false }])
  })
})
