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

  describe('basic SELECT queries', () => {
    it('should select all columns', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM users',
      }))
      expect(result).toEqual(users)
    })

    it('should select all columns with qualified asterisk', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT users.* FROM users',
      }))
      expect(result).toEqual(users)
    })

    it('should select specific columns', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, age FROM users',
      }))
      expect(result).toEqual([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
        { name: 'Charlie', age: 35 },
        { name: 'Diana', age: 28 },
        { name: 'Eve', age: 30 },
      ])
    })

    it('should handle column aliases', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name AS fullName, age AS years FROM users',
      }))
      expect(result[0]).toEqual({ fullName: 'Alice', years: 30 })
    })

    it('should handle empty dataset', async () => {
      const result = await collect(executeSql({
        tables: { users: [] },
        query: 'SELECT * FROM users',
      }))
      expect(result).toEqual([])
    })

    it('should error selecting from non-existent table', async () => {
      await expect(async () => {
        await collect(executeSql({ tables: { users }, query: 'SELECT * FROM orders' }))
      }).rejects.toThrow('Table "orders" not found')
    })
  })

  describe('DISTINCT', () => {
    it('should return distinct rows', async () => {
      const data = [
        { city: 'NYC', age: 30 },
        { city: 'LA', age: 25 },
        { city: 'NYC', age: 30 },
        { city: 'LA', age: 25 },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT DISTINCT city, age FROM data' }))
      expect(result).toHaveLength(2)
    })

    it('should handle DISTINCT with single column', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT DISTINCT city FROM users' }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.city).sort()).toEqual(['LA', 'NYC'])
    })

    it('should not affect non-distinct queries', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT city FROM users' }))
      expect(result).toHaveLength(5)
    })
  })

  describe('LIMIT and OFFSET', () => {
    it('should limit results', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users LIMIT 2' }))
      expect(result).toHaveLength(2)
    })

    it('should apply offset', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users OFFSET 2' }))
      expect(result).toHaveLength(3)
    })

    it('should apply both LIMIT and OFFSET', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users ORDER BY name LIMIT 2 OFFSET 1' }))
      expect(result).toHaveLength(2)
      expect(result[0].name).toBe('Bob')
      expect(result[1].name).toBe('Charlie')
    })

    it('should handle LIMIT larger than result set', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users LIMIT 100' }))
      expect(result).toHaveLength(5)
    })

    it('should handle OFFSET larger than result set', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users OFFSET 100' }))
      expect(result).toHaveLength(0)
    })
  })

  describe('complex queries', () => {
    it('should handle WHERE + GROUP BY + ORDER BY + LIMIT', async () => {
      const result = await collect(executeSql({ tables: { users }, query: `
        SELECT city, COUNT(*) AS count
        FROM users
        WHERE age >= 28
        GROUP BY city
        ORDER BY count DESC
        LIMIT 1
      ` }))
      expect(result).toHaveLength(1)
      expect(result[0].city).toBe('NYC')
      expect(result[0].count).toBe(3)
    })

    it('should handle DISTINCT + ORDER BY + LIMIT', async () => {
      const result = await collect(executeSql({ tables: { users }, query: `
        SELECT DISTINCT age
        FROM users
        ORDER BY age DESC
        LIMIT 3
      ` }))
      expect(result).toHaveLength(3)
      expect(result[0].age).toBe(35)
    })

    it('should apply operations in correct order', async () => {
      // WHERE -> DISTINCT -> ORDER BY -> LIMIT -> OFFSET
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT age FROM users WHERE city = \'NYC\' ORDER BY age LIMIT 1 OFFSET 1' }))
      expect(result).toHaveLength(1)
      expect(result[0].age).toBe(30) // Second age value after sorting (30, 30, 35)
    })
  })

  describe('error cases', () => {
    it('should throw error for SUM with star', async () => {
      await expect(async () => {
        await collect(executeSql({ tables: { users }, query: 'SELECT SUM(*) FROM users' }))
      }).rejects.toThrow('SUM(*) is not supported')
    })

    it('should throw error for AVG with star', async () => {
      await expect(async () => {
        await collect(executeSql({ tables: { users }, query: 'SELECT AVG(*) FROM users' }))
      }).rejects.toThrow('AVG(*) is not supported')
    })

    it('should throw error for MIN with star', async () => {
      await expect(async () => {
        await collect(executeSql({ tables: { users }, query: 'SELECT MIN(*) FROM users' }))
      }).rejects.toThrow('MIN(*) is not supported')
    })

    it('should throw error for MAX with star', async () => {
      await expect(async () => {
        await collect(executeSql({ tables: { users }, query: 'SELECT MAX(*) FROM users' }))
      }).rejects.toThrow('MAX(*) is not supported')
    })
  })

  describe('CAST calls', () => {
    it('should handle CAST to INTEGER in SELECT', async () => {
      const data = [
        { id: 1, age: '30' },
        { id: 2, age: '25' },
        { id: 3, age: '35' },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT CAST(age AS INTEGER) as age_int FROM data' }))
      expect(result).toHaveLength(3)
      expect(result[0].age_int).toBe(30)
      expect(result[1].age_int).toBe(25)
      expect(result[2].age_int).toBe(35)
    })

    it('should handle CAST in WHERE clause', async () => {
      const data = [
        { id: 1, age: '30' },
        { id: 2, age: '25' },
        { id: 3, age: '35' },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data WHERE CAST(age AS INTEGER) > 28' }))
      expect(result).toHaveLength(2)
      expect(result[0].id).toBe(1)
      expect(result[1].id).toBe(3)
    })

    it('should handle CAST in HAVING clause', async () => {
      const data = [
        { city: 'NYC', count: 5 },
        { city: 'NYC', count: 3 },
        { city: 'LA', count: 10 },
        { city: 'SF', count: 2 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT city, SUM(count) as total FROM data GROUP BY city HAVING total > CAST(\'5\' AS INTEGER)',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.city).sort()).toEqual(['LA', 'NYC'])
    })
  })

  describe('edge cases', () => {
    it('should handle negative select', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT -age as neg_age FROM users' }))
      expect(result).toHaveLength(5)
      expect(result[0].neg_age).toBe(-30)
    })

    it('should handle negative where', async () => {
      const data = [
        { id: 1, value: -10 },
        { id: 2, value: 5 },
        { id: 3, value: -3 },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT value as neg_value FROM data WHERE -value > 8' }))
      expect(result).toHaveLength(1)
      expect(result[0].neg_value).toBe(-10)
    })

    it('should handle rows with different keys', async () => {
      const users = [
        { id: 1, name: 'Alice' },
        { id: 2, email: 'bob@example.com' },
        { id: 3, name: 'Charlie', email: 'charlie@example.com' },
      ]
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users' }))
      expect(result).toEqual(users)
    })

    it('should handle string comparisons lexicographically', async () => {
      const data = [
        { id: 1, value: '10' },
        { id: 2, value: '5' },
        { id: 3, value: '20' },
      ]
      // Lexicographic comparison: '5' > '2' and '20' > '2' are both true
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data WHERE value > \'2\'' }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.value).sort()).toEqual(['20', '5'])
    })

    it('should handle boolean values correctly', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE active' }))
      expect(result).toHaveLength(4)
    })

    it('should handle falsy values in WHERE clause', async () => {
      const data = [
        { id: 1, value: 0 },
        { id: 2, value: 1 },
        { id: 3, value: false },
        { id: 4, value: true },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data WHERE value' }))
      expect(result).toHaveLength(2)
      expect(result.every(r => r.value)).toBe(true)
    })

    it('should handle empty string in comparisons', async () => {
      const data = [
        { id: 1, value: '' },
        { id: 2, value: 'hello' },
        { id: 3, value: null },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT * FROM data WHERE value = \'\'',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].id).toBe(1)
    })

    it('should handle special characters in strings', async () => {
      const users = [
        { id: 1, name: 'O\'Brien' },
        { id: 2, name: 'Smith' },
      ]
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM users WHERE name = \'O\'\'Brien\'',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('O\'Brien')
    })

    it('should handle very long column names', async () => {
      const data = [{ id: 1, very_long_column_name_that_exceeds_normal_limits: 'value' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT very_long_column_name_that_exceeds_normal_limits FROM data',
      }))
      expect(result[0].very_long_column_name_that_exceeds_normal_limits).toBe('value')
    })

    it('should handle column names with spaces using double quotes', async () => {
      const users = [
        { id: 1, 'first name': 'Alice', 'last name': 'Smith', age: 30 },
        { id: 2, 'first name': 'Bob', 'last name': 'Jones', age: 25 },
        { id: 3, 'first name': 'Charlie', 'last name': 'Brown', age: 35 },
      ]
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT "first name", "last name", age FROM users WHERE age > 25',
      }))
      expect(result).toHaveLength(2)
      expect(result[0]).toEqual({ 'first name': 'Alice', 'last name': 'Smith', age: 30 })
      expect(result[1]).toEqual({ 'first name': 'Charlie', 'last name': 'Brown', age: 35 })
    })

    it('should handle column names with spaces in aggregates', async () => {
      const data = [
        { id: 1, 'product name': 'Widget', 'total sales': 100 },
        { id: 2, 'product name': 'Gadget', 'total sales': 200 },
        { id: 3, 'product name': 'Widget', 'total sales': 150 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT "product name", SUM("total sales") AS total FROM data GROUP BY "product name"',
      }))
      expect(result).toHaveLength(2)
      const widget = result.find(r => r['product name'] === 'Widget')
      expect(widget?.total).toBe(250)
      const gadget = result.find(r => r['product name'] === 'Gadget')
      expect(gadget?.total).toBe(200)
    })
  })

  describe('CASE expressions', () => {
    it('should handle searched CASE expression', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, CASE WHEN age >= 30 THEN \'senior\' ELSE \'junior\' END AS category FROM users',
      }))
      expect(result).toHaveLength(5)
      expect(result[0]).toEqual({ name: 'Alice', category: 'senior' })
      expect(result[1]).toEqual({ name: 'Bob', category: 'junior' })
      expect(result[2]).toEqual({ name: 'Charlie', category: 'senior' })
    })

    it('should handle simple CASE expression', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, CASE city WHEN \'NYC\' THEN \'New York\' WHEN \'LA\' THEN \'Los Angeles\' END AS city_full FROM users',
      }))
      expect(result).toHaveLength(5)
      expect(result[0]).toEqual({ name: 'Alice', city_full: 'New York' })
      expect(result[1]).toEqual({ name: 'Bob', city_full: 'Los Angeles' })
    })
  })
})
