import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('subqueries', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, active: true },
    { id: 2, name: 'Bob', age: 25, active: true },
    { id: 3, name: 'Charlie', age: 35, active: false },
  ]

  const orders = [
    { id: 1, user_id: 1, amount: 100 },
    { id: 2, user_id: 2, amount: 200 },
    { id: 3, user_id: 1, amount: 150 },
    { id: 4, user_id: 999, amount: 50 }, // orphan order
  ]

  describe('FROM clause subquery (derived table)', () => {
    it('should execute simple derived table', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name FROM (SELECT * FROM users WHERE age > 25) AS older_users',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should handle derived table with column selection', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, age FROM (SELECT name, age FROM users WHERE active = TRUE) AS active_users',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])
    })

    it('should handle derived table with aggregates', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT * FROM (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) AS user_totals',
      }))
      expect(result).toHaveLength(3)
    })

    it('should allow filtering on derived table', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: `
          SELECT * FROM (
            SELECT user_id, SUM(amount) AS total
            FROM orders
            GROUP BY user_id
          ) AS user_totals
          WHERE total > 100
        `,
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.user_id).sort()).toEqual([1, 2])
    })

    it('should handle empty derived table', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM users WHERE age > 100) AS empty',
      }))
      expect(result).toHaveLength(0)
    })

    it('should handle derived table with ORDER BY in outer query', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, age FROM (SELECT * FROM users) AS all_users ORDER BY age DESC',
      }))
      expect(result).toHaveLength(3)
      expect(result[0].name).toBe('Charlie')
      expect(result[2].name).toBe('Bob')
    })

    it('should support subquery in FROM clause', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name FROM (SELECT * FROM users WHERE age > 25) AS u',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should preserve inner WHERE with SELECT * passthrough', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM users WHERE age > 25) AS u',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should apply outer WHERE on passthrough subquery', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM users) AS u WHERE age > 25',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should apply both inner and outer WHERE', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM users WHERE age > 25) AS u WHERE name = \'Alice\'',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice')
    })

    it('should handle nested passthrough subqueries', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM (SELECT * FROM users WHERE age > 25) AS a) AS b',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should preserve inner LIMIT with SELECT * passthrough', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM users ORDER BY id LIMIT 2) AS u',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.id)).toEqual([1, 2])
    })

    it('should preserve inner LIMIT and OFFSET with passthrough', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM users ORDER BY id LIMIT 2 OFFSET 1) AS u',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.id)).toEqual([2, 3])
    })

    it('should apply outer WHERE after inner LIMIT', async () => {
      // inner LIMIT 2 returns first 2 rows, outer WHERE filters those
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM users ORDER BY id LIMIT 2) AS u WHERE age > 28',
      }))
      // users by id: Alice(30), Bob(25) — only Alice passes age > 28
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice')
    })

    it('should apply outer LIMIT on passthrough subquery', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM users ORDER BY id) AS u LIMIT 1',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].id).toBe(1)
    })

    it('should apply inner and outer OFFSET independently', async () => {
      // inner OFFSET 1 skips Alice to Bob, Charlie; outer OFFSET 1 skips Bob to Charlie
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM users ORDER BY id OFFSET 1) AS u OFFSET 1',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })

    it('should apply OFFSET for derived table with LIMIT', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT id FROM (SELECT * FROM users ORDER BY id) AS u LIMIT 1 OFFSET 1',
      }))
      expect(result).toEqual([{ id: 2 }])
    })

    it('should execute set operations in FROM clause subqueries', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM (
            SELECT name FROM users WHERE age < 30
            UNION ALL
            SELECT name FROM users WHERE age >= 30
          ) AS u
        `,
      }))
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Charlie'])
    })
  })

  describe('IN subquery', () => {
    it('should filter with IN subquery', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])
    })

    it('should handle IN subquery with WHERE clause', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 150)',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Bob')
    })

    it('should handle IN subquery returning no rows', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 1000)',
      }))
      expect(result).toHaveLength(0)
    })

    it('should handle IN subquery with multiple matches', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = TRUE)',
      }))
      expect(result).toHaveLength(3) // orders 1, 2, 3
    })

    it('should execute IN with set-operation subquery', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT * FROM orders WHERE user_id IN (
            SELECT id FROM users WHERE active = TRUE
            EXCEPT
            SELECT user_id AS id FROM orders WHERE amount >= 200
          )
        `,
      }))
      expect(result.map(r => r.id).sort()).toEqual([1, 3])
    })

    it('should work with IN subquery on non-id columns', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM orders WHERE amount IN (SELECT age FROM users)',
      }))
      // age values: 30, 25, 35 - no orders match these amounts
      expect(result).toHaveLength(0)
    })
  })

  describe('NOT IN subquery', () => {
    it('should filter with NOT IN subquery', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })

    it('should handle NOT IN with filtered subquery', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE amount > 150)',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should return all rows when NOT IN subquery is empty', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE amount > 1000)',
      }))
      expect(result).toHaveLength(3)
    })
  })

  describe('EXISTS subquery', () => {
    it('should return all rows when EXISTS subquery has results', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE EXISTS (SELECT * FROM orders)',
      }))
      expect(result).toHaveLength(3) // all users
    })

    it('should return no rows when EXISTS subquery is empty', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE EXISTS (SELECT * FROM orders WHERE amount > 1000)',
      }))
      expect(result).toHaveLength(0)
    })

    it('should handle EXISTS with filtered subquery', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE EXISTS (SELECT * FROM orders WHERE amount > 100)',
      }))
      expect(result).toHaveLength(3)
    })

    it('should execute EXISTS with set-operation subquery', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT * FROM users WHERE EXISTS (
            SELECT id FROM users WHERE active = TRUE
            UNION
            SELECT user_id AS id FROM orders WHERE amount > 1000
          )
        `,
      }))
      expect(result).toHaveLength(3)
    })
  })

  describe('NOT EXISTS subquery', () => {
    it('should return no rows when NOT EXISTS subquery has results', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE NOT EXISTS (SELECT * FROM orders)',
      }))
      expect(result).toHaveLength(0)
    })

    it('should return all rows when NOT EXISTS subquery is empty', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE NOT EXISTS (SELECT * FROM orders WHERE amount > 1000)',
      }))
      expect(result).toHaveLength(3)
    })
  })

  describe('error cases', () => {
    it('should throw error for subquery referencing unknown table', async () => {
      await expect(async () => {
        await collect(executeSql({
          tables: { users },
          query: 'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)',
        }))
      }).rejects.toThrow('Table "orders" not found')
    })

    it('should support nested subqueries in FROM', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM (SELECT * FROM users) AS sub1) AS sub2',
      }))
      expect(result).toEqual(users)
    })

    it('should throw error for unknown table in FROM subquery', async () => {
      await expect(async () => {
        await collect(executeSql({
          tables: { users },
          query: 'SELECT * FROM (SELECT * FROM nonexistent) AS sub',
        }))
      }).rejects.toThrow('Table "nonexistent" not found')
    })
  })

  describe('complex scenarios', () => {
    it('should combine IN subquery with other WHERE conditions', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) AND age > 27',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice')
    })

    it('should handle subquery in OR condition', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE age > 34 OR id IN (SELECT user_id FROM orders WHERE amount > 150)',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Bob', 'Charlie'])
    })

    it('should combine EXISTS with AND condition', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users WHERE EXISTS (SELECT * FROM orders WHERE amount > 100) AND active = TRUE',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])
    })

    it('should handle multiple subqueries in same query', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT * FROM users
          WHERE id IN (SELECT user_id FROM orders)
          AND id NOT IN (SELECT user_id FROM orders WHERE amount > 150)
        `,
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice')
    })

    it('should handle derived table with DISTINCT', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT * FROM (SELECT DISTINCT user_id FROM orders) AS unique_users',
      }))
      expect(result).toHaveLength(3)
    })

    it('should handle derived table with LIMIT', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM (SELECT * FROM users ORDER BY age DESC LIMIT 2) AS top_users',
      }))
      expect(result).toHaveLength(2)
    })
  })

  it('should preserve columns needed by DISTINCT through COUNT(*) pushdown', async () => {
    const items = [
      { group: 'a', value: 1 },
      { group: 'a', value: 1 },
      { group: 'a', value: 2 },
    ]
    const result = await collect(executeSql({
      tables: { items },
      query: 'SELECT COUNT(*) AS n FROM (SELECT DISTINCT value FROM items) AS d',
    }))
    expect(result).toEqual([{ n: 2 }])
  })

  // scalar subqueries
  describe('scalar subqueries', () => {
    it('should handle simple scalar subquery in SELECT', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT
            name,
            (SELECT MAX(age) FROM users) AS max_age
          FROM users
        `,
      }))
      expect(result).toHaveLength(3)
      const alice = result.find(r => r.name === 'Alice')
      const bob = result.find(r => r.name === 'Bob')
      const charlie = result.find(r => r.name === 'Charlie')
      expect(alice?.max_age).toBe(35)
      expect(bob?.max_age).toBe(35)
      expect(charlie?.max_age).toBe(35)
    })

    it('should handle correlated scalar subquery', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT
            name,
            (SELECT SUM(amount) FROM orders WHERE user_id = users.id) AS total_orders
          FROM users
        `,
      }))
      expect(result).toHaveLength(3)
      const alice = result.find(r => r.name === 'Alice')
      const bob = result.find(r => r.name === 'Bob')
      const charlie = result.find(r => r.name === 'Charlie')
      expect(alice?.total_orders).toBe(250)
      expect(bob?.total_orders).toBe(200)
      expect(charlie?.total_orders).toBeNull()
    })

    it('should handle correlated scalar subquery with ORDER BY and LIMIT', async () => {
      const messages = [
        { session_id: 1, role: 'user', content: 'hello', timestamp: 1 },
        { session_id: 1, role: 'assistant', content: 'hi there', timestamp: 2 },
        { session_id: 1, role: 'user', content: 'how are you', timestamp: 3 },
        { session_id: 1, role: 'assistant', content: 'doing well', timestamp: 4 },
        { session_id: 2, role: 'user', content: 'hey', timestamp: 5 },
        { session_id: 2, role: 'assistant', content: 'howdy', timestamp: 6 },
      ]
      const result = await collect(executeSql({
        tables: { messages },
        query: `
          SELECT
            a.session_id,
            a.timestamp,
            a.content AS assistant_content,
            (SELECT u.content FROM messages u
             WHERE u.session_id = a.session_id
               AND u.role = 'user'
               AND u.timestamp < a.timestamp
             ORDER BY u.timestamp DESC
             LIMIT 1) AS prior_user
          FROM messages a
          WHERE a.role = 'assistant'
          ORDER BY a.timestamp
        `,
      }))
      expect(result).toEqual([
        { session_id: 1, timestamp: 2, assistant_content: 'hi there', prior_user: 'hello' },
        { session_id: 1, timestamp: 4, assistant_content: 'doing well', prior_user: 'how are you' },
        { session_id: 2, timestamp: 6, assistant_content: 'howdy', prior_user: 'hey' },
      ])
    })

    it('should return null when correlated subquery matches no rows', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT name, (SELECT amount FROM orders WHERE user_id = users.id AND amount > 9999) AS big_order
          FROM users
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice', big_order: null },
        { name: 'Bob', big_order: null },
        { name: 'Charlie', big_order: null },
      ])
    })

    it('should handle multiple correlated subqueries in same SELECT', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT
            name,
            (SELECT COUNT(*) FROM orders WHERE user_id = users.id) AS order_count,
            (SELECT MAX(amount) FROM orders WHERE user_id = users.id) AS max_order
          FROM users
          ORDER BY name
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice', order_count: 2, max_order: 150 },
        { name: 'Bob', order_count: 1, max_order: 200 },
        { name: 'Charlie', order_count: 0, max_order: null },
      ])
    })

    it('should disambiguate inner vs outer columns with same name', async () => {
      // Both tables have 'id' — outer users.id vs inner orders.id
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT name, (SELECT MIN(id) FROM orders WHERE user_id = users.id) AS first_order_id
          FROM users
          ORDER BY name
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice', first_order_id: 1 },
        { name: 'Bob', first_order_id: 2 },
        { name: 'Charlie', first_order_id: null },
      ])
    })

    it('should handle correlated subquery with aliased outer table', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT u.name, (SELECT SUM(amount) FROM orders o WHERE o.user_id = u.id) AS total
          FROM users u
          ORDER BY u.name
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice', total: 250 },
        { name: 'Bob', total: 200 },
        { name: 'Charlie', total: null },
      ])
    })

    it('should handle correlated subquery referencing outer WHERE-filtered rows', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT name, (SELECT SUM(amount) FROM orders WHERE user_id = users.id) AS total
          FROM users
          WHERE active = TRUE
          ORDER BY name
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice', total: 250 },
        { name: 'Bob', total: 200 },
      ])
    })

    it('should handle correlated subquery with self-join pattern', async () => {
      // Find users whose age is above the average of all other users
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT u.name, u.age,
            (SELECT AVG(age) FROM users WHERE id != u.id) AS others_avg
          FROM users u
          ORDER BY u.name
        `,
      }))
      expect(result).toHaveLength(3)
      const alice = result.find(r => r.name === 'Alice')
      expect(alice?.others_avg).toBe(30) // avg(25, 35)
    })

    it('should handle correlated subquery in expression context', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT
            name,
            age - (SELECT AVG(age) FROM users) AS age_diff
          FROM users
          ORDER BY name
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice', age_diff: 0 },
        { name: 'Bob', age_diff: -5 },
        { name: 'Charlie', age_diff: 5 },
      ])
    })

    it('should handle correlated subquery nested inside FROM subquery', async () => {
      // The correlated ref (users.id) is inside a FROM-clause subquery
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT name,
            (SELECT max_amt FROM (SELECT MAX(amount) AS max_amt FROM orders WHERE user_id = users.id) AS sub) AS biggest
          FROM users
          ORDER BY name
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice', biggest: 150 },
        { name: 'Bob', biggest: 200 },
        { name: 'Charlie', biggest: null },
      ])
    })
  })
})
