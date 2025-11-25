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
})
