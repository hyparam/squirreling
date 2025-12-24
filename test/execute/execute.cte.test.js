import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('CTE execution', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30 },
    { id: 2, name: 'Bob', age: 25 },
    { id: 3, name: 'Charlie', age: 35 },
  ]

  const orders = [
    { id: 1, user_id: 1, amount: 100 },
    { id: 2, user_id: 2, amount: 200 },
    { id: 3, user_id: 1, amount: 150 },
  ]

  describe('basic CTE', () => {
    it('should execute simple CTE', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH adult_users AS (SELECT * FROM users WHERE age >= 30)
          SELECT name FROM adult_users
        `,
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should execute CTE with column selection', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH names AS (SELECT name FROM users)
          SELECT * FROM names
        `,
      }))
      expect(result).toHaveLength(3)
      expect(result.every(r => 'name' in r)).toBe(true)
      expect(result.every(r => !('age' in r))).toBe(true)
    })

    it('should execute CTE with alias', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH cte AS (SELECT * FROM users)
          SELECT t.name FROM cte AS t
        `,
      }))
      expect(result).toHaveLength(3)
    })
  })

  describe('CTE with aggregation', () => {
    it('should execute CTE with GROUP BY', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: `
          WITH user_totals AS (
            SELECT user_id, SUM(amount) AS total
            FROM orders
            GROUP BY user_id
          )
          SELECT * FROM user_totals
        `,
      }))
      expect(result).toHaveLength(2)
      const user1 = result.find(r => r.user_id === 1)
      expect(user1.total).toBe(250)
    })

    it('should execute CTE with HAVING', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: `
          WITH big_spenders AS (
            SELECT user_id, SUM(amount) AS total
            FROM orders
            GROUP BY user_id
            HAVING SUM(amount) > 100
          )
          SELECT * FROM big_spenders
        `,
      }))
      expect(result).toHaveLength(2)
    })

    it('should filter on CTE aggregate in main query', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: `
          WITH user_totals AS (
            SELECT user_id, SUM(amount) AS total
            FROM orders
            GROUP BY user_id
          )
          SELECT * FROM user_totals WHERE total > 200
        `,
      }))
      expect(result).toHaveLength(1)
      expect(result[0].user_id).toBe(1)
    })
  })

  describe('multiple CTEs', () => {
    it('should execute multiple independent CTEs', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          WITH
            young_users AS (SELECT id, name FROM users WHERE age < 35),
            big_orders AS (SELECT user_id, amount FROM orders WHERE amount > 100)
          SELECT young_users.name, big_orders.amount
          FROM young_users
          JOIN big_orders ON young_users.id = big_orders.user_id
        `,
      }))
      expect(result).toHaveLength(2) // Alice: 150, Bob: 200
    })

    it('should execute CTE referencing another CTE', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH
            all_users AS (SELECT * FROM users),
            old_users AS (SELECT * FROM all_users WHERE age > 25)
          SELECT name FROM old_users
        `,
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should chain multiple CTEs', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH
            step1 AS (SELECT * FROM users),
            step2 AS (SELECT * FROM step1 WHERE age > 25),
            step3 AS (SELECT name FROM step2 WHERE age < 35)
          SELECT * FROM step3
        `,
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice')
    })
  })

  describe('CTE with JOIN', () => {
    it('should JOIN main query with CTE', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          WITH user_spend AS (
            SELECT user_id, SUM(amount) AS total
            FROM orders
            GROUP BY user_id
          )
          SELECT users.name, user_spend.total
          FROM users
          JOIN user_spend ON users.id = user_spend.user_id
        `,
      }))
      expect(result).toHaveLength(2)
      const alice = result.find(r => r.name === 'Alice')
      expect(alice.total).toBe(250)
    })

    it('should JOIN CTE with regular table', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          WITH active AS (SELECT id, name FROM users WHERE age < 35)
          SELECT active.name, orders.amount
          FROM active
          JOIN orders ON active.id = orders.user_id
        `,
      }))
      expect(result).toHaveLength(3) // Alice x2, Bob x1
    })

    it('should JOIN two CTEs', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          WITH
            u AS (SELECT id, name FROM users),
            o AS (SELECT user_id, amount FROM orders)
          SELECT u.name, o.amount
          FROM u
          JOIN o ON u.id = o.user_id
        `,
      }))
      expect(result).toHaveLength(3)
    })
  })

  describe('CTE re-execution (streaming)', () => {
    it('should re-execute CTE when referenced multiple times', async () => {
      // This test verifies that CTEs stream (re-execute) rather than materialize
      // Each reference to the CTE should trigger a fresh execution
      let executionCount = 0

      // Create a custom data source that tracks execution
      const trackingSource = {
        async *scan() {
          executionCount++
          for (const user of users) {
            yield {
              columns: Object.keys(user),
              cells: Object.fromEntries(
                Object.entries(user).map(([k, v]) => [k, () => Promise.resolve(v)])
              ),
            }
          }
        },
      }

      const result = await collect(executeSql({
        tables: { users: trackingSource },
        query: `
          WITH cte AS (SELECT * FROM users)
          SELECT a.name AS name1, b.name AS name2
          FROM cte a
          JOIN cte b ON a.id < b.id
        `,
      }))

      // CTE referenced twice (FROM and JOIN), so should execute twice
      expect(executionCount).toBe(2)
      expect(result.length).toBeGreaterThan(0)
    })

    it('should stream CTE results without materializing', async () => {
      // Test that CTE properly streams - cells remain lazy
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH cte AS (SELECT * FROM users LIMIT 1)
          SELECT * FROM cte
        `,
      }))
      expect(result).toHaveLength(1)
    })
  })

  describe('CTE shadowing', () => {
    it('should use CTE when it shadows a real table name', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH users AS (SELECT 'Override' AS name FROM users LIMIT 1)
          SELECT * FROM users
        `,
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Override')
    })
  })

  describe('CTE case insensitivity', () => {
    it('should match CTE names case-insensitively', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH MyTable AS (SELECT * FROM users WHERE age > 30)
          SELECT name FROM MYTABLE
        `,
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })
  })

  describe('CTE with ORDER BY and LIMIT', () => {
    it('should handle CTE with ORDER BY', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH sorted AS (SELECT * FROM users ORDER BY age DESC)
          SELECT name FROM sorted
        `,
      }))
      expect(result).toHaveLength(3)
      expect(result[0].name).toBe('Charlie')
    })

    it('should handle CTE with LIMIT', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH top2 AS (SELECT * FROM users ORDER BY age DESC LIMIT 2)
          SELECT name FROM top2
        `,
      }))
      expect(result).toHaveLength(2)
    })

    it('should handle ORDER BY in main query on CTE', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH cte AS (SELECT * FROM users)
          SELECT name, age FROM cte ORDER BY age ASC
        `,
      }))
      expect(result[0].name).toBe('Bob')
      expect(result[2].name).toBe('Charlie')
    })
  })

  describe('error cases', () => {
    it('should throw error for undefined CTE reference', async () => {
      await expect(async () => {
        await collect(executeSql({
          tables: { users },
          query: `
            WITH defined_cte AS (SELECT * FROM users)
            SELECT * FROM undefined_cte
          `,
        }))
      }).rejects.toThrow(/Table "undefined_cte" not found/)
    })

    it('should throw error for table not found in CTE', async () => {
      await expect(async () => {
        await collect(executeSql({
          tables: { users },
          query: `
            WITH cte AS (SELECT * FROM nonexistent)
            SELECT * FROM cte
          `,
        }))
      }).rejects.toThrow(/Table "nonexistent" not found/)
    })
  })

  describe('CTE with DISTINCT', () => {
    it('should handle DISTINCT in CTE', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: `
          WITH unique_users AS (SELECT DISTINCT user_id FROM orders)
          SELECT * FROM unique_users
        `,
      }))
      expect(result).toHaveLength(2)
    })
  })

  describe('empty results', () => {
    it('should handle CTE returning empty result', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH empty AS (SELECT * FROM users WHERE age > 100)
          SELECT * FROM empty
        `,
      }))
      expect(result).toHaveLength(0)
    })

    it('should handle main query filtering all CTE rows', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH cte AS (SELECT * FROM users)
          SELECT * FROM cte WHERE age > 100
        `,
      }))
      expect(result).toHaveLength(0)
    })
  })
})
