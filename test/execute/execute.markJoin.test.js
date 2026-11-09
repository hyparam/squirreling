import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { collect, executePlan, executeSql, planSql } from '../../src/index.js'
import { trackingSource } from './trackingSource.js'

/**
 * @import { AsyncDataSource } from '../../src/types.js'
 */

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

  it('filters correlated NOT IN in WHERE with one scan', async () => {
    const wanted = trackingSource([{ id: 1, g: 'a' }, { id: null, g: 'a' }, { id: 2, g: 'b' }])
    const tables = { items: memorySource({ data: [{ id: 1, g: 'a' }, { id: 3, g: 'a' }, { id: 1, g: 'b' }, { id: 2, g: 'b' }, { id: null, g: 'b' }, { id: 1, g: 'c' }] }), wanted: wanted.source }
    const query = 'SELECT id, g FROM items WHERE id NOT IN (SELECT w.id FROM wanted w WHERE w.g = items.g)'
    const plan = planSql({ query, tables })
    if (plan.type !== 'Project') throw new Error('expected project')
    expect(plan.child.type).toBe('SemiJoin')
    expect(await collect(executeSql({ tables, query }))).toEqual([{ id: 1, g: 'b' }, { id: 1, g: 'c' }])
    expect(wanted.getScanCount()).toBe(1)
  })

  it('filters uncorrelated EXISTS in WHERE with one scan', async () => {
    const wanted = trackingSource([{ id: 1 }, { id: 2 }])
    const tables = { items: [{ id: 1 }, { id: 2 }], wanted: wanted.source }
    expect(await collect(executeSql({ tables, query: 'SELECT id FROM items WHERE EXISTS (SELECT id FROM wanted)' }))).toEqual([{ id: 1 }, { id: 2 }])
    expect(await collect(executeSql({ tables, query: 'SELECT id FROM items WHERE NOT EXISTS (SELECT id FROM wanted)' }))).toEqual([])
    expect(wanted.getScanCount()).toBe(2)
    expect(wanted.getRowCount()).toBe(2)
  })
})

describe('mark membership', () => {
  it.each([{ data: [] }, { data: [{ id: 1 }] }])('evaluates membership on an empty scalar aggregate with right input $data', async ({ data }) => {
    const tables = {
      items: memorySource({ data: [], columns: ['id'] }),
      wanted: memorySource({ data, columns: ['id'] }),
    }
    expect(await collect(executeSql({
      tables,
      query: 'SELECT COUNT(*) AS n, EXISTS (SELECT id FROM wanted) AS hit, 1 IN (SELECT id FROM wanted) AS included, NOT EXISTS (SELECT id FROM wanted) AS missing FROM items',
    }))).toEqual([{ n: 0, hit: !!data.length, included: !!data.length, missing: !data.length }])
  })

  it('builds membership only when requested and shares the build across rows', async () => {
    const source = memorySource({ data: [{ id: 1 }] })
    let scans = 0
    /** @type {AsyncDataSource} */
    const wanted = {
      columns: ['id'],
      scan(options) {
        scans++
        return source.scan(options)
      },
    }
    const tables = { items: [{ id: 1 }, { id: 2 }], wanted }
    expect(await collect(executeSql({
      tables,
      query: 'SELECT CASE WHEN false THEN id IN (SELECT id FROM wanted) ELSE false END AS hit FROM items',
    }))).toEqual([{ hit: false }, { hit: false }])
    expect(scans).toBe(0)
    const result = executeSql({ tables, query: 'SELECT id IN (SELECT id FROM wanted) AS hit FROM items' })
    expect(await collect(result)).toEqual([{ hit: true }, { hit: false }])
    expect(scans).toBe(1)
    expect(await collect(result)).toEqual([{ hit: true }, { hit: false }])
    expect(scans).toBe(2)
  })
})

describe('scalar single join', () => {
  it('reads an uncorrelated scalar subquery once', async () => {
    const wanted = trackingSource([{ id: 3 }, { id: 1 }, { id: 2 }])
    const tables = { items: [{ id: 1 }, { id: 2 }], wanted: wanted.source }
    expect(await collect(executeSql({ tables, query: 'SELECT id, (SELECT id FROM wanted ORDER BY id LIMIT 1) AS v FROM items' }))).toEqual([
      { id: 1, v: 1 },
      { id: 2, v: 1 },
    ])
    expect(wanted.getScanCount()).toBe(1)
  })

  it('returns NULL for an empty scalar subquery', async () => {
    const tables = { items: [{ id: 1 }], wanted: memorySource({ data: [], columns: ['id'] }) }
    expect(await collect(executeSql({ tables, query: 'SELECT id, (SELECT id FROM wanted) AS v FROM items' }))).toEqual([{ id: 1, v: null }])
  })

  it('rejects a scalar subquery with more than one row when read', async () => {
    const tables = { items: [{ id: 1 }], wanted: [{ id: 1 }, { id: 2 }] }
    await expect(collect(executeSql({ tables, query: 'SELECT id, (SELECT id FROM wanted) AS v FROM items' }))).rejects.toThrow('Scalar subquery returned more than one row')
    expect(await collect(executeSql({ tables: { ...tables, items: memorySource({ data: [], columns: ['id'] }) }, query: 'SELECT id, (SELECT id FROM wanted) AS v FROM items' }))).toEqual([])
  })

  it('joins a correlated scalar subquery on equality keys', async () => {
    const names = trackingSource([{ id: 1, name: 'a' }, { id: 2, name: 'b' }, { id: 2, name: 'c' }])
    const tables = { items: [{ id: 1 }, { id: 3 }, { id: null }], names: names.source }
    expect(await collect(executeSql({ tables, query: 'SELECT id, (SELECT n.name FROM names n WHERE n.id = items.id) AS name FROM items' }))).toEqual([
      { id: 1, name: 'a' },
      { id: 3, name: null },
      { id: null, name: null },
    ])
    expect(names.getScanCount()).toBe(1)
    await expect(collect(executeSql({ tables: { ...tables, items: [{ id: 2 }] }, query: 'SELECT (SELECT n.name FROM names n WHERE n.id = items.id) AS name FROM items' }))).rejects.toThrow('Scalar subquery returned more than one row')
  })

  it('evaluates correlated scalar values only for requested keys', async () => {
    const tables = { items: [{ id: 1 }], wanted: [{ id: 1 }, { id: 2 }] }
    const query = 'SELECT (SELECT CASE WHEN w.id=2 THEN (SELECT id FROM wanted) ELSE 5 END FROM wanted w WHERE w.id=items.id) AS v FROM items'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ v: 5 }])
    await expect(collect(executeSql({ tables: { ...tables, items: [{ id: 2 }] }, query }))).rejects.toThrow('Scalar subquery returned more than one row')
  })

  it('keeps a correlated DISTINCT scalar on the per-row path', async () => {
    const tables = { items: memorySource({ data: [{ id: 2 }] }), names: memorySource({ data: [{ id: 2, name: 'b' }, { id: 2, name: 'b' }] }) }
    const query = 'SELECT (SELECT DISTINCT n.name FROM names n WHERE n.id = items.id) AS name FROM items'
    const plan = planSql({ query, tables })
    if (plan.type !== 'Project') throw new Error('expected project')
    expect(plan.child.type).toBe('Scan')
    expect(await collect(executeSql({ tables, query }))).toEqual([{ name: 'b' }])
  })

  it('uses an uncorrelated scalar in a grouped query', async () => {
    const limits = trackingSource([{ n: 2 }])
    const tables = { items: [{ g: 'a' }, { g: 'a' }, { g: 'b' }], limits: limits.source }
    expect(await collect(executeSql({ tables, query: 'SELECT g, COUNT(*) >= (SELECT n FROM limits) AS big FROM items GROUP BY g ORDER BY g' }))).toEqual([
      { g: 'a', big: true },
      { g: 'b', big: false },
    ])
    expect(limits.getScanCount()).toBe(1)
  })
})

describe('mark joins in other clauses', () => {
  it('filters a WHERE disjunction with one scan', async () => {
    const wanted = trackingSource([{ id: 2 }, { id: null }])
    const tables = { items: [{ id: 1 }, { id: 2 }, { id: 3 }, { id: null }], wanted: wanted.source }
    expect(await collect(executeSql({ tables, query: 'SELECT id FROM items WHERE id = 1 OR id IN (SELECT id FROM wanted)' }))).toEqual([{ id: 1 }, { id: 2 }])
    expect(await collect(executeSql({ tables, query: 'SELECT id FROM items WHERE id = 1 OR id NOT IN (SELECT id FROM wanted)' }))).toEqual([{ id: 1 }])
    expect(wanted.getScanCount()).toBe(2)
  })

  it('filters on a scalar subquery in WHERE', async () => {
    const limits = trackingSource([{ n: 2 }])
    const tables = { items: [{ id: 1 }, { id: 2 }, { id: 3 }], limits: limits.source }
    expect(await collect(executeSql({ tables, query: 'SELECT id FROM items WHERE id >= (SELECT n FROM limits) LIMIT 1' }))).toEqual([{ id: 2 }])
    expect(limits.getScanCount()).toBe(1)
  })

  it('keeps marks out of star expansion', async () => {
    const wanted = trackingSource([{ id: 2 }])
    const tables = { items: [{ id: 1, v: 'x' }, { id: 2, v: 'y' }], wanted: wanted.source }
    expect(await collect(executeSql({ tables, query: 'SELECT *, id IN (SELECT id FROM wanted) AS hit FROM items' }))).toEqual([
      { id: 1, v: 'x', hit: false },
      { id: 2, v: 'y', hit: true },
    ])
    expect(await collect(executeSql({ tables, query: 'SELECT * FROM items WHERE id = 1 OR EXISTS (SELECT id FROM wanted WHERE id = 5)' }))).toEqual([{ id: 1, v: 'x' }])
    expect(wanted.getScanCount()).toBe(2)
  })

  it.each([
    { projection: '*', expected: { id: 1, wid: 1, v: 'x' } },
    { projection: 'a.*', expected: { id: 1 } },
    { projection: 'b.*', expected: { wid: 1, v: 'x' } },
  ])('preserves derived-table join columns for $projection with a mark', async ({ projection, expected }) => {
    const tables = { items: [{ id: 1 }], wanted: [{ wid: 1, v: 'x' }] }
    const query = `SELECT ${projection} FROM (SELECT id FROM items) a JOIN wanted b ON a.id=b.wid WHERE EXISTS(SELECT wid FROM wanted) OR a.id=2`
    const result = executeSql({ tables, query })
    expect(result.columns).toEqual(Object.keys(expected))
    expect(await collect(result)).toEqual([expected])
  })

  it('keeps marks out of qualified stars over joins', async () => {
    const tables = { a: [{ id: 1, x: 'p' }, { id: 2, x: 'q' }], b: [{ id: 1, y: 'r' }, { id: 2, y: 's' }], wanted: [{ id: 2 }] }
    expect(await collect(executeSql({ tables, query: 'SELECT a.*, b.y, a.id IN (SELECT id FROM wanted) AS hit FROM a JOIN b ON a.id = b.id ORDER BY a.id' }))).toEqual([
      { id: 1, x: 'p', y: 'r', hit: false },
      { id: 2, x: 'q', y: 's', hit: true },
    ])
  })

  it('evaluates membership alongside window functions', async () => {
    const wanted = trackingSource([{ id: 2 }])
    const tables = { items: [{ id: 1 }, { id: 2 }], wanted: wanted.source }
    expect(await collect(executeSql({ tables, query: 'SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS rn, id IN (SELECT id FROM wanted) AS hit FROM items ORDER BY id' }))).toEqual([
      { id: 1, rn: 2, hit: false },
      { id: 2, rn: 1, hit: true },
    ])
    expect(wanted.getScanCount()).toBe(1)
  })

  it('filters HAVING membership on aggregates above the aggregate', async () => {
    const counts = trackingSource([{ n: 2 }, { n: null }])
    const tables = { items: [{ g: 'a' }, { g: 'a' }, { g: 'b' }, { g: 'c' }, { g: 'c' }], counts: counts.source }
    const query = 'SELECT g, COUNT(*) AS n FROM items GROUP BY g HAVING COUNT(*) IN (SELECT n FROM counts) ORDER BY g'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ g: 'a', n: 2 }, { g: 'c', n: 2 }])
    expect(counts.getScanCount()).toBe(1)
    expect(await collect(executeSql({ tables, query: 'SELECT g FROM items GROUP BY g HAVING g = \'b\' OR MAX(g) IN (SELECT \'c\' AS v FROM counts) ORDER BY g' }))).toEqual([{ g: 'b' }, { g: 'c' }])
    expect(await collect(executeSql({ tables, query: 'SELECT g, COUNT(*) AS n FROM items GROUP BY g HAVING n NOT IN (SELECT n FROM counts WHERE n IS NOT NULL) ORDER BY g' }))).toEqual([{ g: 'b', n: 1 }])
  })

  it('filters HAVING membership on a scalar aggregate', async () => {
    const tables = { items: memorySource({ data: [], columns: ['id'] }), counts: [{ n: 0 }] }
    expect(await collect(executeSql({ tables, query: 'SELECT COUNT(*) AS n FROM items HAVING COUNT(*) IN (SELECT n FROM counts)' }))).toEqual([{ n: 0 }])
    expect(await collect(executeSql({ tables, query: 'SELECT COUNT(*) AS n FROM items HAVING COUNT(*) NOT IN (SELECT n FROM counts)' }))).toEqual([])
  })

  it('leaves correlated HAVING membership on aggregates on the per-row path', async () => {
    const tables = { items: [{ g: 'a' }, { g: 'a' }, { g: 'b' }], counts: [{ g: 'a', n: 2 }, { g: 'b', n: 2 }] }
    expect(await collect(executeSql({ tables, query: 'SELECT g FROM items GROUP BY g HAVING COUNT(*) IN (SELECT c.n FROM counts c WHERE c.g = items.g) ORDER BY g' }))).toEqual([{ g: 'a' }])
  })
})
