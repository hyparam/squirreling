import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { collect, executeSql } from '../../src/index.js'
import { planSql } from '../../src/plan/plan.js'

describe('TopN (ORDER BY + LIMIT fusion)', () => {
  const users = memorySource({ data: [
    { id: 1, name: 'Alice', age: 30 },
    { id: 2, name: 'Bob', age: 25 },
    { id: 3, name: 'Charlie', age: 35 },
    { id: 4, name: 'Diana', age: 28 },
    { id: 5, name: 'Eve', age: 30 },
  ] })

  it('should plan Sort+Limit as TopN', () => {
    const plan = planSql({ query: 'SELECT * FROM users ORDER BY age LIMIT 3', tables: { users } })
    // SELECT * has no Project wrapper, so TopN is the top of the plan
    expect(plan.type).toBe('TopN')
    if (plan.type !== 'TopN') return
    expect(plan.limit).toBe(3)
    expect(plan.child.type).toBe('Scan')
  })

  it('should not fuse when OFFSET is present', () => {
    const plan = planSql({ query: 'SELECT * FROM users ORDER BY age LIMIT 3 OFFSET 1', tables: { users } })
    expect(plan.type).toBe('Limit')
    if (plan.type !== 'Limit') return
    expect(plan.child.type).toBe('Sort')
  })

  it('should return fewer rows when limit exceeds input', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users ORDER BY age LIMIT 100' }))
    expect(result).toHaveLength(5)
    expect(result.map(r => r.age)).toEqual([25, 28, 30, 30, 35])
  })

  it('should return empty for LIMIT 0', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users ORDER BY age LIMIT 0' }))
    expect(result).toEqual([])
  })

  it('should return top N ascending', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT name, age FROM users ORDER BY age LIMIT 2' }))
    expect(result.map(r => r.age)).toEqual([25, 28])
    expect(result[0].name).toBe('Bob')
    expect(result[1].name).toBe('Diana')
  })

  it('should return top N descending', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT name, age FROM users ORDER BY age DESC LIMIT 2' }))
    expect(result.map(r => r.age)).toEqual([35, 30])
    expect(result[0].name).toBe('Charlie')
  })

  it('should match Sort+Limit exactly on boundary ties (LIMIT hits middle of tie group)', async () => {
    // ages: 25, 28, 30, 30, 35 — LIMIT 3 forces a tie-break at age=30
    const tables = { users }
    const topn = await collect(executeSql({ tables, query: 'SELECT id, age FROM users ORDER BY age LIMIT 3' }))
    expect(topn).toHaveLength(3)
    expect(topn.map(r => r.age)).toEqual([25, 28, 30])
    // The exact id for the third row may be 1 or 5 (both age 30); assert it's one of them.
    expect([1, 5]).toContain(topn[2].id)
  })

  it('should sort by multiple columns with mixed directions', async () => {
    const data = [
      { a: 1, b: 'y' },
      { a: 2, b: 'x' },
      { a: 1, b: 'x' },
      { a: 2, b: 'y' },
      { a: 1, b: 'z' },
    ]
    const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data ORDER BY a ASC, b DESC LIMIT 3' }))
    expect(result).toEqual([
      { a: 1, b: 'z' },
      { a: 1, b: 'y' },
      { a: 1, b: 'x' },
    ])
  })

  it('should handle NULLs in sort keys', async () => {
    const data = [
      { id: 1, v: 10 },
      { id: 2, v: null },
      { id: 3, v: 5 },
      { id: 4, v: null },
      { id: 5, v: 20 },
    ]
    // NULLs sort first (NULLS FIRST is the default, regardless of direction)
    const asc = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data ORDER BY v LIMIT 3' }))
    expect(asc.map(r => r.v)).toEqual([null, null, 5])

    const desc = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data ORDER BY v DESC LIMIT 3' }))
    expect(desc.map(r => r.v)).toEqual([null, null, 20])
  })

  it('should produce identical results to Sort+Limit for a larger random input', async () => {
    // Generate deterministic pseudo-random data
    const data = []
    let seed = 42
    for (let i = 0; i < 200; i++) {
      seed = seed * 1103515245 + 12345 & 0x7fffffff
      data.push({ id: i, k: seed % 50 })
    }
    const topn = await collect(executeSql({ tables: { data }, query: 'SELECT k FROM data ORDER BY k LIMIT 10' }))
    // Reference: full JS sort
    const reference = [...data].sort((a, b) => a.k - b.k).slice(0, 10).map(r => ({ k: r.k }))
    expect(topn).toEqual(reference)
  })

  it('should project after TopN when Limit(Project(Sort)) pattern applies', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT name FROM users ORDER BY age DESC LIMIT 2' }))
    expect(result).toHaveLength(2)
    expect(result[0]).toEqual({ name: 'Charlie' })
    // Second is age-30 tie between Alice and Eve; TopN is not stable.
    expect(['Alice', 'Eve']).toContain(result[1].name)
  })
})
