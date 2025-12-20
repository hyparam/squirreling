import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('executeSql', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
  ]

  describe('aggregate functions', () => {
    it('should count all rows with COUNT(*)', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT COUNT(*) FROM users' }))
      expect(result).toEqual([{ count_all: 5 }])
    })

    it('should count column with COUNT(column)', async () => {
      const users = [
        { id: 1, name: 'Alice' },
        { id: 2, name: null },
        { id: 3, name: 'Charlie' },
      ]
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT COUNT(name) FROM users' }))
      expect(result).toEqual([{ count_name: 2 }])
    })

    it('should count distinct values with COUNT(DISTINCT column)', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT COUNT(DISTINCT city) AS unique_cities FROM users',
      }))
      expect(result).toEqual([{ unique_cities: 2 }])
    })

    it('should calculate SUM', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT SUM(age) FROM users' }))
      expect(result).toEqual([{ sum_age: 148 }])
    })

    it('should calculate AVG', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT AVG(age) FROM users' }))
      expect(result).toEqual([{ avg_age: 29.6 }])
    })

    it('should calculate MIN and MAX', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT MIN(age) AS min_age, MAX(age) AS max_age FROM users',
      }))
      expect(result).toEqual([{ min_age: 25, max_age: 35 }])
    })

    it('should handle aggregate with alias', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT COUNT(*) AS total FROM users',
      }))
      expect(result).toEqual([{ total: 5 }])
    })

    it('should handle empty dataset for aggregates', async () => {
      const result = await collect(executeSql({
        tables: { users: [] },
        query: 'SELECT AVG(age) FROM users',
      }))
      expect(result).toEqual([{ avg_age: null }])
    })

    it('should skip non-numeric values in SUM/AVG/MIN/MAX', async () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 'abc' },
        { id: 4, value: 20 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUM(value) AS total, AVG(value) AS avg FROM data',
      }))
      expect(result).toEqual([{ total: 30, avg: 15 }])
    })

    it('should throw error for SUM/AVG/MIN/MAX with star', async () => {
      await expect(async () => {
        await collect(executeSql({ tables: { users }, query: 'SELECT SUM(*) FROM users' }))
      }).rejects.toThrow('SUM(*) is not supported')
    })

    it('should handle aggregate without GROUP BY (single group)', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT COUNT(*) FROM users' }))
      expect(result).toEqual([{ count_all: 5 }])
    })

    it('should handle mixing columns with aggregates without GROUP BY (takes first row)', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT name, COUNT(*) FROM users' }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice') // First row
      expect(result[0].count_all).toBe(5)
    })

    it('should handle nested cast in aggregate', async () => {
      const data = [
        { size: 10 },
        { size: 20 },
        { size: 30 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUM(CAST(size AS BIGINT)) AS total_size FROM data',
      }))
      expect(result).toEqual([{ total_size: 60 }])
    })

    it('should handle string functions in aggregate', async () => {
      const data = [
        { problem: 'short' },
        { problem: 'a bit longer' },
        { problem: 'the longest problem string' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT MAX(LENGTH(problem)) AS max_problem_len FROM data',
      }))
      expect(result).toEqual([{ max_problem_len: 26 }])
    })

    it('should collect values into array with JSON_ARRAYAGG', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT JSON_ARRAYAGG(name) AS names FROM users',
      }))
      expect(result).toEqual([{ names: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'] }])
    })

    it('should handle JSON_ARRAYAGG with GROUP BY', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT city, JSON_ARRAYAGG(name) AS names FROM users GROUP BY city ORDER BY city',
      }))
      expect(result).toEqual([
        { city: 'LA', names: ['Bob', 'Diana'] },
        { city: 'NYC', names: ['Alice', 'Charlie', 'Eve'] },
      ])
    })

    it('should handle JSON_ARRAYAGG DISTINCT', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT JSON_ARRAYAGG(DISTINCT city) AS cities FROM users',
      }))
      expect(result[0].cities).toHaveLength(2)
      expect(result[0].cities).toContain('NYC')
      expect(result[0].cities).toContain('LA')
    })

    it('should include nulls in JSON_ARRAYAGG', async () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: null },
        { id: 3, name: 'Charlie' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_ARRAYAGG(name) AS names FROM data',
      }))
      expect(result).toEqual([{ names: ['Alice', null, 'Charlie'] }])
    })

    it('should handle JSON_ARRAYAGG with numeric values', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT JSON_ARRAYAGG(age) AS ages FROM users',
      }))
      expect(result).toEqual([{ ages: [30, 25, 35, 28, 30] }])
    })

    it('should handle JSON_ARRAYAGG with boolean values', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT JSON_ARRAYAGG(active) AS active_status FROM users',
      }))
      expect(result).toEqual([{ active_status: [true, true, false, true, true] }])
    })

    it('should throw error for JSON_ARRAYAGG(*)', async () => {
      await expect(async () => {
        await collect(executeSql({
          tables: { users },
          query: 'SELECT JSON_ARRAYAGG(*) FROM users',
        }))
      }).rejects.toThrow('JSON_ARRAYAGG(*) is not supported')
    })

    it('should handle empty dataset for JSON_ARRAYAGG', async () => {
      const result = await collect(executeSql({
        tables: { users: [] },
        query: 'SELECT JSON_ARRAYAGG(name) AS names FROM users',
      }))
      expect(result).toEqual([{ names: [] }])
    })
  })

  describe('null handling in aggregates', () => {
    it('should handle null in aggregate functions correctly', async () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: null },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT COUNT(*) AS total, COUNT(value) AS non_null FROM data',
      }))
      expect(result[0]).toEqual({ total: 3, non_null: 1 })
    })

    it('should handle null in GROUP BY', async () => {
      const data = [
        { id: 1, category: 'A', value: 10 },
        { id: 2, category: null, value: 20 },
        { id: 3, category: null, value: 30 },
        { id: 4, category: 'A', value: 40 },
      ]
      const result = await collect(executeSql({ tables: { data }, query: `
        SELECT category, SUM(value) AS total
        FROM data
        GROUP BY category
      ` }))
      expect(result).toHaveLength(2)
      const nullGroup = result.find(r => r.category === null)
      expect(nullGroup?.total).toBe(50)
    })
  })

  describe('aggregate expressions', () => {
    it('should handle arithmetic on aggregate result: SUM(x) * 2', async () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUM(value) * 2 AS doubled FROM data',
      }))
      expect(result).toEqual([{ doubled: 120 }])
    })

    it('should handle expression inside aggregate: SUM(price * quantity)', async () => {
      const orders = [
        { id: 1, price: 10, quantity: 2 },
        { id: 2, price: 5, quantity: 4 },
        { id: 3, price: 8, quantity: 3 },
      ]
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT SUM(price * quantity) AS total_revenue FROM orders',
      }))
      expect(result).toEqual([{ total_revenue: 64 }]) // 20 + 20 + 24 = 64
    })

    it('should handle division of aggregates: SUM(price) / COUNT(*)', async () => {
      const items = [
        { id: 1, price: 10 },
        { id: 2, price: 20 },
        { id: 3, price: 30 },
        { id: 4, price: 40 },
      ]
      const result = await collect(executeSql({
        tables: { items },
        query: 'SELECT SUM(price) / COUNT(*) AS avg_price FROM items',
      }))
      expect(result).toEqual([{ avg_price: 25 }]) // 100 / 4 = 25
    })
  })
})
