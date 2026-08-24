import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('WHERE clause', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
  ]

  it('should filter with equality', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE name = \'Alice\'' }))
    expect(result).toHaveLength(1)
    expect(result[0].name).toBe('Alice')
  })

  it('should filter with equality using == operator', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE name == \'Alice\'' }))
    expect(result).toHaveLength(1)
    expect(result[0].name).toBe('Alice')
  })

  it('should filter with comparison operators', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age > 30' }))
    expect(result).toHaveLength(1)
    expect(result[0].name).toBe('Charlie')
  })

  it('should filter with AND', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE city = \'NYC\' AND age = 30' }))
    expect(result).toHaveLength(2)
    expect(result.every(u => u.city === 'NYC' && u.age === 30)).toBe(true)
  })

  it('should filter with OR', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age < 26 OR age > 33' }))
    expect(result).toHaveLength(2)
    expect(result.map(u => u.name).sort()).toEqual(['Bob', 'Charlie'])
  })

  it('should handle complex WHERE with parentheses', async () => {
    const result = await collect(executeSql({ tables: { users }, query: `
      SELECT * FROM users
      WHERE (age < 28 OR age > 32) AND city = 'NYC'
    ` }))
    expect(result).toHaveLength(1)
    expect(result[0].name).toBe('Charlie')
  })

  it('should handle OR precedence without parentheses', async () => {
    const result = await collect(executeSql({ tables: { users }, query: `
      SELECT * FROM users
      WHERE city = 'NYC' AND age = 30 OR city = 'LA'
    ` }))
    // Should be: (city = 'NYC' AND age = 30) OR (city = 'LA')
    expect(result).toHaveLength(4)
  })

  it('should filter with NOT', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE NOT active' }))
    expect(result).toHaveLength(1)
    expect(result[0].name).toBe('Charlie')
  })

  it('should handle inequality operators', async () => {
    const result1 = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age != 30' }))
    expect(result1).toHaveLength(3)

    const result2 = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age <> 30' }))
    expect(result2).toHaveLength(3)
  })

  it('should handle <= and >= operators', async () => {
    const result1 = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age <= 28' }))
    expect(result1).toHaveLength(2)

    const result2 = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age >= 30' }))
    expect(result2).toHaveLength(3)
  })

  it('should handle literal values in WHERE', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE active = TRUE' }))
    expect(result).toHaveLength(4)
  })

  it('should handle IS NULL', async () => {
    const users = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: null },
      { id: 3, name: 'Charlie', email: null },
      { id: 4, name: 'Diana', email: 'diana@example.com' },
    ]
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE email IS NULL' }))
    expect(result).toHaveLength(2)
    expect(result.map(r => r.name).sort()).toEqual(['Bob', 'Charlie'])
  })

  it('should handle IS NOT NULL', async () => {
    const users = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: null },
      { id: 3, name: 'Charlie', email: null },
      { id: 4, name: 'Diana', email: 'diana@example.com' },
    ]
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT * FROM users WHERE email IS NOT NULL',
    }))
    expect(result).toHaveLength(2)
    expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Diana'])
  })

  it('should handle IS NULL with explicit null values', async () => {
    const users = [
      { id: 1, name: 'Alice', email: null },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
      { id: 3, name: 'Charlie', email: null },
    ]
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT * FROM users WHERE email IS NULL',
    }))
    expect(result).toHaveLength(2)
    expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
  })

  it('should handle IS NULL/IS NOT NULL with AND/OR', async () => {
    const users = [
      { id: 1, name: 'Alice', email: 'alice@example.com', phone: '123' },
      { id: 2, name: 'Bob', email: null, phone: '456' },
      { id: 3, name: 'Charlie', email: null, phone: null },
      { id: 4, name: 'Diana', email: 'diana@example.com', phone: null },
    ]

    const result1 = await collect(executeSql({
      tables: { users },
      query: 'SELECT * FROM users WHERE email IS NULL AND phone IS NOT NULL',
    }))
    expect(result1).toHaveLength(1)
    expect(result1[0].name).toBe('Bob')

    const result2 = await collect(executeSql({
      tables: { users },
      query: 'SELECT * FROM users WHERE email IS NULL OR phone IS NULL',
    }))
    expect(result2).toHaveLength(3)
    expect(result2.map(r => r.name).sort()).toEqual(['Bob', 'Charlie', 'Diana'])
  })

  it('should NOT match NULL with equality', async () => {
    const data = [
      { id: 1, value: null },
      { id: 2, value: 0 },
      { id: 3, value: false },
    ]
    const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data WHERE value = NULL' }))
    expect(result).toHaveLength(0) // NULL comparisons should return false
  })

  it('should filter with LIKE', async () => {
    const users = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Charlie' },
      { id: 4, name: 'Diana' },
    ]
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE name LIKE \'%li%\'' }))
    expect(result).toHaveLength(2)
    expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
  })

  it('should filter with LIKE using underscore wildcard', async () => {
    const data = [
      { id: 1, code: 'A123' },
      { id: 2, code: 'B456' },
      { id: 3, code: 'A1X3' },
      { id: 4, code: 'A12' },
    ]
    const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data WHERE code LIKE \'A1_3\'' }))
    expect(result).toHaveLength(2)
    expect(result.map(r => r.code).sort()).toEqual(['A123', 'A1X3'])
  })

  it('should filter with LIKE combining % and _ wildcards', async () => {
    const users = [
      { id: 1, email: 'alice@example.com' },
      { id: 2, email: 'bob@test.com' },
      { id: 3, email: 'charlie@example.org' },
      { id: 4, email: 'diana@example.com' },
    ]
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE email LIKE \'_____@example.___\'' }))
    expect(result).toHaveLength(2)
    expect(result.map(r => r.email).sort()).toEqual(['alice@example.com', 'diana@example.com'])
  })

  it('should filter with LIKE across newlines', async () => {
    const data = [
      { id: 1, s: 'line one\nFind me\nline three' },
      { id: 2, s: 'nothing here' },
    ]
    const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data WHERE s LIKE \'%Find me%\'' }))
    expect(result).toHaveLength(1)
    expect(result.map(r => r.id)).toEqual([1])
  })

  it('should filter with NOT LIKE', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE name NOT LIKE \'%li%\'' }))
    expect(result).toHaveLength(3)
    expect(result.map(r => r.name).sort()).toEqual(['Bob', 'Diana', 'Eve'])
  })

  it('should filter with LIKE over an object column by its JSON text', async () => {
    const calls = [
      { id: 1, args: { command: 'cargo build' } },
      { id: 2, args: { command: 'pnpm test' } },
    ]
    const result = await collect(executeSql({ tables: { calls }, query: 'SELECT id FROM calls WHERE args LIKE \'%cargo%\'' }))
    expect(result.map(r => r.id)).toEqual([1])
  })

  it('should not match object columns against [object Object]', async () => {
    const calls = [
      { id: 1, args: { command: 'cargo build' } },
      { id: 2, args: { command: 'pnpm test' } },
    ]
    const result = await collect(executeSql({ tables: { calls }, query: 'SELECT id FROM calls WHERE args LIKE \'%object Object%\'' }))
    expect(result).toHaveLength(0)
  })

  it('should make LIKE on an object column agree with CAST AS VARCHAR', async () => {
    const calls = [
      { id: 1, args: { command: 'cargo build' } },
      { id: 2, args: { command: 'pnpm test' } },
    ]
    const bare = await collect(executeSql({ tables: { calls }, query: 'SELECT id FROM calls WHERE args LIKE \'%"pnpm test"%\'' }))
    const cast = await collect(executeSql({ tables: { calls }, query: 'SELECT id FROM calls WHERE CAST(args AS VARCHAR) LIKE \'%"pnpm test"%\'' }))
    expect(bare.map(r => r.id)).toEqual([2])
    expect(cast.map(r => r.id)).toEqual(bare.map(r => r.id))
  })

  it('should filter with LIKE over an array column by its JSON text', async () => {
    const calls = [
      { id: 1, tags: ['cargo', 'rust'] },
      { id: 2, tags: ['pnpm'] },
    ]
    const result = await collect(executeSql({ tables: { calls }, query: 'SELECT id FROM calls WHERE tags LIKE \'%"rust"%\'' }))
    expect(result.map(r => r.id)).toEqual([1])
  })

  it('should filter with LIKE over a Date column by its JSON text', async () => {
    const events = [
      { id: 1, at: new Date('2026-08-24T15:01:00Z') },
      { id: 2, at: new Date('2026-07-21T15:01:00Z') },
    ]
    const result = await collect(executeSql({ tables: { events }, query: 'SELECT id FROM events WHERE at LIKE \'%2026-08-%\'' }))
    expect(result.map(r => r.id)).toEqual([1])
  })

  it('should filter with IN value list', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE name IN (\'Alice\', \'Charlie\', \'Eve\')' }))
    expect(result).toHaveLength(3)
    expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie', 'Eve'])
  })

  it('should filter with IN value list of numbers', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age IN (25, 28, 30)' }))
    expect(result).toHaveLength(4)
    expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Diana', 'Eve'])
  })

  it('should filter with NOT IN value list', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE name NOT IN (\'Alice\', \'Bob\')' }))
    expect(result).toHaveLength(3)
    expect(result.map(r => r.name).sort()).toEqual(['Charlie', 'Diana', 'Eve'])
  })

  it('should handle IN with empty result', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE name IN (\'Zara\', \'Xander\')' }))
    expect(result).toHaveLength(0)
  })

  it('should filter with IN subquery', async () => {
    const orders = [
      { id: 1, user_id: 1, amount: 100 },
      { id: 2, user_id: 2, amount: 200 },
      { id: 3, user_id: 3, amount: 150 },
      { id: 4, user_id: 1, amount: 50 },
    ]
    const result = await collect(executeSql({
      tables: { orders, users },
      query: 'SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = TRUE)',
    }))
    // Users 1 and 2 are active, so orders 1, 2, 4 match
    expect(result).toHaveLength(3)
  })

  it('should filter with NOT IN subquery', async () => {
    const orders = [
      { id: 1, user_id: 1, amount: 100 },
      { id: 2, user_id: 2, amount: 200 },
      { id: 3, user_id: 3, amount: 150 },
      { id: 4, user_id: 1, amount: 50 },
    ]
    const result = await collect(executeSql({
      tables: { orders, users },
      query: 'SELECT * FROM orders WHERE user_id NOT IN (SELECT id FROM users WHERE active = FALSE)',
    }))
    // Users 3, 4, 5 are inactive, so orders with user_id 1, 2 remain (orders 1, 2, 4)
    expect(result).toHaveLength(3)
  })

  it('should filter with EXISTS subquery (non-correlated)', async () => {
    const orders = [
      { id: 1, user_id: 1, amount: 100 },
      { id: 2, user_id: 2, amount: 200 },
      { id: 3, user_id: 999, amount: 150 },
      { id: 4, user_id: 1, amount: 50 },
    ]
    // Non-correlated EXISTS - returns all rows if subquery has results
    const result = await collect(executeSql({
      tables: { orders, users },
      query: 'SELECT * FROM orders WHERE EXISTS (SELECT * FROM users WHERE active = TRUE)',
    }))
    expect(result).toHaveLength(4) // all orders since there are active users
  })

  it('should filter with NOT EXISTS subquery (non-correlated)', async () => {
    const orders = [
      { id: 1, user_id: 1, amount: 100 },
      { id: 2, user_id: 2, amount: 200 },
      { id: 3, user_id: 999, amount: 150 },
      { id: 4, user_id: 1, amount: 50 },
    ]
    // Non-correlated NOT EXISTS - returns no rows if subquery has results
    const result = await collect(executeSql({
      tables: { orders, users },
      query: 'SELECT * FROM orders WHERE NOT EXISTS (SELECT * FROM users WHERE active = TRUE)',
    }))
    expect(result).toHaveLength(0) // no orders since there are active users
  })

  it('should use loose equality for bigints and numbers', async () => {
    const data = [
      { id: 1, value: 100n },
      { id: 2, value: 200 },
      { id: 3, value: 300n },
      { id: 4, value: 400 },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT * FROM data WHERE value = 300',
    }))
    expect(result).toHaveLength(1)
    expect(result.map(r => r.id)).toEqual([3])
  })

  it('should use loose equality for bigints and numbers in IN lists', async () => {
    const data = [
      { id: 1, value: 100n },
      { id: 2, value: 200 },
      { id: 3, value: 300n },
      { id: 4, value: 400 },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT * FROM data WHERE value IN (100, 300)',
    }))
    expect(result).toHaveLength(2)
    expect(result.map(r => r.id)).toEqual([1, 3])
  })

  it('should handle null equality according to SQL semantics', async () => {
    const data = [
      { id: 1, value: null },
      { id: 2, value: 0 },
      { id: 3, value: 'test' },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT * FROM data WHERE value = NULL',
    }))
    expect(result).toHaveLength(0) // NULL comparisons should return false
  })

  it('should compare Date columns by instant, not object identity', async () => {
    const events = [
      { id: 1, ts: new Date('2026-01-02T10:00:00Z') },
      { id: 2, ts: new Date('2026-03-09T10:00:00Z') },
    ]
    // Distinct Date instances representing the same instant must compare equal.
    const eq = await collect(executeSql({
      tables: { events, probe: [{ t: new Date('2026-01-02T10:00:00Z') }] },
      query: 'SELECT events.id FROM events JOIN probe ON events.ts = probe.t',
    }))
    expect(eq).toEqual([{ id: 1 }])

    const lt = await collect(executeSql({
      tables: { events, probe: [{ t: new Date('2026-02-01T00:00:00Z') }] },
      query: 'SELECT events.id FROM events JOIN probe ON events.ts < probe.t',
    }))
    expect(lt).toEqual([{ id: 1 }])
  })

  it('should support IN over Date columns', async () => {
    const events = [
      { id: 1, ts: new Date('2026-01-02T10:00:00Z') },
      { id: 2, ts: new Date('2026-03-09T10:00:00Z') },
      { id: 3, ts: new Date('2026-05-15T10:00:00Z') },
    ]
    const wanted = [
      { t: new Date('2026-01-02T10:00:00Z') },
      { t: new Date('2026-05-15T10:00:00Z') },
    ]
    const result = await collect(executeSql({
      tables: { events, wanted },
      query: 'SELECT id FROM events WHERE ts IN (SELECT t FROM wanted)',
    }))
    expect(result).toEqual([{ id: 1 }, { id: 3 }])
  })
})

describe('WHERE three-valued logic over nulls', () => {
  // UNKNOWN and false both exclude a row, so the difference only shows
  // through NOT, which must keep UNKNOWN as UNKNOWN instead of flipping a
  // null comparison to true
  const readings = [
    { id: 1, ts: 100 },
    { id: 2, ts: null },
    { id: 3, ts: 300 },
    { id: 4, ts: null },
    { id: 5, ts: 500 },
  ]

  /**
   * @param {string} where
   * @returns {Promise<number[]>}
   */
  async function ids(where) {
    const rows = await collect(executeSql({ tables: { readings }, query: `SELECT id FROM readings WHERE ${where}` }))
    return rows.map(r => {
      if (typeof r.id !== 'number') throw new TypeError('Expected id to be a number')
      return r.id
    })
  }

  it('excludes null rows from a negated comparison', async () => {
    expect(await ids('NOT (ts < 300)')).toEqual([3, 5])
    expect(await ids('NOT (ts = 300)')).toEqual([1, 5])
    expect(await ids('NOT (ts >= 100)')).toEqual([])
  })

  it('returns no rows for a comparison against NULL, negated or not', async () => {
    expect(await ids('ts = NULL')).toEqual([])
    expect(await ids('NOT (ts = NULL)')).toEqual([])
    expect(await ids('NOT NOT (ts = NULL)')).toEqual([])
  })

  it('excludes null rows from negated AND / OR trees', async () => {
    expect(await ids('NOT (ts > 300 OR ts < 100)')).toEqual([1, 3])
    expect(await ids('NOT (ts >= 300 OR ts <= 100)')).toEqual([])
    expect(await ids('NOT (ts = 300 AND ts = 100)')).toEqual([1, 3, 5])
  })

  it('resolves AND / OR with a null operand only when the other side decides', async () => {
    // ts = NULL is UNKNOWN for every row. Where ts is non-null the false
    // conjunct decides and NOT flips it; where ts is null both conjuncts are
    // UNKNOWN and the row stays excluded
    expect(await ids('ts = NULL AND ts = -1')).toEqual([])
    expect(await ids('NOT (ts = NULL AND ts = -1)')).toEqual([1, 3, 5])
    // and the true disjunct decides
    expect(await ids('ts = NULL OR ts = 300')).toEqual([3])
    expect(await ids('NOT (ts = NULL OR ts = 300)')).toEqual([])
  })

  it('treats IN as a chain of equalities for null purposes', async () => {
    expect(await ids('ts IN (100, 500)')).toEqual([1, 5])
    expect(await ids('ts NOT IN (100, 500)')).toEqual([3])
    // a null member makes every non-match UNKNOWN
    expect(await ids('ts IN (100, NULL)')).toEqual([1])
    expect(await ids('ts NOT IN (100, NULL)')).toEqual([])
    expect(await ids('ts IN (NULL)')).toEqual([])
  })

  it('excludes null rows from NOT BETWEEN', async () => {
    expect(await ids('NOT (ts BETWEEN 100 AND 300)')).toEqual([5])
  })

  it('excludes null rows from NOT LIKE', async () => {
    const names = [
      { id: 1, name: 'alpha' },
      { id: 2, name: null },
      { id: 3, name: 'beta' },
    ]
    const rows = await collect(executeSql({ tables: { names }, query: 'SELECT id FROM names WHERE name NOT LIKE \'a%\'' }))
    expect(rows.map(r => r.id)).toEqual([3])
  })

  it('still answers IS NULL / IS NOT NULL two-valued', async () => {
    expect(await ids('ts IS NULL')).toEqual([2, 4])
    expect(await ids('NOT (ts IS NULL)')).toEqual([1, 3, 5])
  })

  it('excludes null rows when IN matches against a subquery with nulls', async () => {
    const rows = await collect(executeSql({
      tables: { readings, probe: [{ t: 100 }, { t: null }] },
      query: 'SELECT id FROM readings WHERE ts NOT IN (SELECT t FROM probe)',
    }))
    expect(rows.map(r => r.id)).toEqual([])
  })

  it('treats every value as outside an empty subquery', async () => {
    const rows = await collect(executeSql({
      tables: { readings, probe: [{ t: 100 }] },
      query: `
        SELECT
          id,
          ts IN (SELECT t FROM probe WHERE t < 0) AS is_in,
          ts NOT IN (SELECT t FROM probe WHERE t < 0) AS is_not_in
        FROM readings
      `,
    }))
    expect(rows).toEqual([
      { id: 1, is_in: false, is_not_in: true },
      { id: 2, is_in: false, is_not_in: true },
      { id: 3, is_in: false, is_not_in: true },
      { id: 4, is_in: false, is_not_in: true },
      { id: 5, is_in: false, is_not_in: true },
    ])
  })
})
