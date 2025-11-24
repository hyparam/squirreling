import { describe, expect, it } from 'vitest'
import { executeSql } from '../../src/execute/execute.js'

describe('executeSql', () => {
  const source = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
  ]

  describe('WHERE clause', () => {
    it('should filter with equality', () => {
      const result = executeSql({ source, query: 'SELECT * FROM users WHERE name = \'Alice\'' })
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice')
    })

    it('should filter with comparison operators', () => {
      const result = executeSql({ source, query: 'SELECT * FROM users WHERE age > 30' })
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })

    it('should filter with AND', () => {
      const result = executeSql({ source, query: 'SELECT * FROM users WHERE city = \'NYC\' AND age = 30' })
      expect(result).toHaveLength(2)
      expect(result.every(u => u.city === 'NYC' && u.age === 30)).toBe(true)
    })

    it('should filter with OR', () => {
      const result = executeSql({ source, query: 'SELECT * FROM users WHERE age < 26 OR age > 33' })
      expect(result).toHaveLength(2)
      expect(result.map(u => u.name).sort()).toEqual(['Bob', 'Charlie'])
    })

    it('should handle complex WHERE with parentheses', () => {
      const result = executeSql({ source, query: `
        SELECT * FROM users
        WHERE (age < 28 OR age > 32) AND city = 'NYC'
      ` })
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })

    it('should handle OR precedence without parentheses', () => {
      const result = executeSql({ source, query: `
        SELECT * FROM users
        WHERE city = 'NYC' AND age = 30 OR city = 'LA'
      ` })
      // Should be: (city = 'NYC' AND age = 30) OR (city = 'LA')
      expect(result).toHaveLength(4)
    })

    it('should filter with NOT', () => {
      const result = executeSql({ source, query: 'SELECT * FROM users WHERE NOT active' })
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })

    it('should handle inequality operators', () => {
      const result1 = executeSql({ source, query: 'SELECT * FROM users WHERE age != 30' })
      expect(result1).toHaveLength(3)

      const result2 = executeSql({ source, query: 'SELECT * FROM users WHERE age <> 30' })
      expect(result2).toHaveLength(3)
    })

    it('should handle <= and >= operators', () => {
      const result1 = executeSql({ source, query: 'SELECT * FROM users WHERE age <= 28' })
      expect(result1).toHaveLength(2)

      const result2 = executeSql({ source, query: 'SELECT * FROM users WHERE age >= 30' })
      expect(result2).toHaveLength(3)
    })

    it('should handle literal values in WHERE', () => {
      const result = executeSql({ source, query: 'SELECT * FROM users WHERE active = TRUE' })
      expect(result).toHaveLength(4)
    })

    it('should handle IS NULL', () => {
      const data = [
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: null },
        { id: 3, name: 'Charlie', email: null },
        { id: 4, name: 'Diana', email: 'diana@example.com' },
      ]
      const result = executeSql({ source: data, query: 'SELECT * FROM users WHERE email IS NULL' })
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Bob', 'Charlie'])
    })

    it('should handle IS NOT NULL', () => {
      const data = [
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: null },
        { id: 3, name: 'Charlie', email: null },
        { id: 4, name: 'Diana', email: 'diana@example.com' },
      ]
      const result = executeSql({ source: data, query: 'SELECT * FROM users WHERE email IS NOT NULL' })
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Diana'])
    })

    it('should handle IS NULL with undefined values', () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob', email: 'bob@example.com' },
        { id: 3, name: 'Charlie' },
      ]
      const result = executeSql({ source: data, query: 'SELECT * FROM users WHERE email IS NULL' })
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should handle IS NULL/IS NOT NULL with AND/OR', () => {
      const data = [
        { id: 1, name: 'Alice', email: 'alice@example.com', phone: '123' },
        { id: 2, name: 'Bob', email: null, phone: '456' },
        { id: 3, name: 'Charlie', email: null, phone: null },
        { id: 4, name: 'Diana', email: 'diana@example.com', phone: null },
      ]

      const result1 = executeSql({ source: data, query: 'SELECT * FROM users WHERE email IS NULL AND phone IS NOT NULL' })
      expect(result1).toHaveLength(1)
      expect(result1[0].name).toBe('Bob')

      const result2 = executeSql({ source: data, query: 'SELECT * FROM users WHERE email IS NULL OR phone IS NULL' })
      expect(result2).toHaveLength(3)
      expect(result2.map(r => r.name).sort()).toEqual(['Bob', 'Charlie', 'Diana'])
    })

    it('should NOT match NULL with equality', () => {
      const data = [
        { id: 1, value: null },
        { id: 2, value: 0 },
        { id: 3, value: false },
      ]
      const result = executeSql({ source: data, query: 'SELECT * FROM users WHERE value = NULL' })
      expect(result).toHaveLength(0) // NULL comparisons should return false
    })

    it('should filter with LIKE', () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
        { id: 4, name: 'Diana' },
      ]
      const result = executeSql({ source: data, query: 'SELECT * FROM users WHERE name LIKE \'%li%\'' })
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should filter with LIKE using underscore wildcard', () => {
      const data = [
        { id: 1, code: 'A123' },
        { id: 2, code: 'B456' },
        { id: 3, code: 'A1X3' },
        { id: 4, code: 'A12' },
      ]
      const result = executeSql({ source: data, query: 'SELECT * FROM users WHERE code LIKE \'A1_3\'' })
      expect(result).toHaveLength(2)
      expect(result.map(r => r.code).sort()).toEqual(['A123', 'A1X3'])
    })

    it('should filter with LIKE combining % and _ wildcards', () => {
      const data = [
        { id: 1, email: 'alice@example.com' },
        { id: 2, email: 'bob@test.com' },
        { id: 3, email: 'charlie@example.org' },
        { id: 4, email: 'diana@example.com' },
      ]
      const result = executeSql({ source: data, query: 'SELECT * FROM users WHERE email LIKE \'_____@example.___\'' })
      expect(result).toHaveLength(2)
      expect(result.map(r => r.email).sort()).toEqual(['alice@example.com', 'diana@example.com'])
    })

    it('should filter with IN value list', () => {
      const result = executeSql({ source, query: 'SELECT * FROM users WHERE name IN (\'Alice\', \'Charlie\', \'Eve\')' })
      expect(result).toHaveLength(3)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie', 'Eve'])
    })

    it('should filter with IN value list of numbers', () => {
      const result = executeSql({ source, query: 'SELECT * FROM users WHERE age IN (25, 28, 30)' })
      expect(result).toHaveLength(4)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Diana', 'Eve'])
    })

    it('should filter with NOT IN value list', () => {
      const result = executeSql({ source, query: 'SELECT * FROM users WHERE name NOT IN (\'Alice\', \'Bob\')' })
      expect(result).toHaveLength(3)
      expect(result.map(r => r.name).sort()).toEqual(['Charlie', 'Diana', 'Eve'])
    })

    it('should handle IN with empty result', () => {
      const result = executeSql({ source, query: 'SELECT * FROM users WHERE name IN (\'Zara\', \'Xander\')' })
      expect(result).toHaveLength(0)
    })

    it('should filter with IN subquery', () => {
      const orders = [
        { id: 1, user_id: 1, amount: 100 },
        { id: 2, user_id: 2, amount: 200 },
        { id: 3, user_id: 3, amount: 150 },
        { id: 4, user_id: 1, amount: 50 },
      ]
      // TODO: Need to support multiple tables/sources for this to work
      // For now, this test verifies we throw a clear error message
      expect(() => executeSql({
        source: orders,
        query: 'SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = TRUE)',
      })).toThrow('WHERE IN with subqueries is not yet supported.')
    })

    it('should filter with NOT IN subquery', () => {
      const orders = [
        { id: 1, user_id: 1, amount: 100 },
        { id: 2, user_id: 2, amount: 200 },
        { id: 3, user_id: 3, amount: 150 },
        { id: 4, user_id: 1, amount: 50 },
      ]
      // TODO: Need to support multiple tables/sources for this to work
      // For now, this test verifies we throw a clear error message
      expect(() => executeSql({
        source: orders,
        query: 'SELECT * FROM orders WHERE user_id NOT IN (SELECT id FROM users WHERE active = FALSE)',
      })).toThrow('WHERE NOT IN with subqueries is not yet supported.')
    })

    it('should filter with EXISTS subquery', () => {
      const orders = [
        { id: 1, user_id: 1, amount: 100 },
        { id: 2, user_id: 2, amount: 200 },
        { id: 3, user_id: 999, amount: 150 },
        { id: 4, user_id: 1, amount: 50 },
      ]
      // TODO: Need to support multiple tables/sources and correlated subqueries
      // For now, this test verifies we throw a clear error message
      expect(() => executeSql({
        source: orders,
        query: 'SELECT * FROM orders WHERE EXISTS (SELECT * FROM users WHERE users.id = orders.user_id)',
      })).toThrow('WHERE EXISTS with subqueries is not yet supported.')
    })

    it('should filter with NOT EXISTS subquery', () => {
      const orders = [
        { id: 1, user_id: 1, amount: 100 },
        { id: 2, user_id: 2, amount: 200 },
        { id: 3, user_id: 999, amount: 150 },
        { id: 4, user_id: 1, amount: 50 },
      ]
      // TODO: Need to support multiple tables/sources and correlated subqueries
      // For now, this test verifies we throw a clear error message
      expect(() => executeSql({
        source: orders,
        query: 'SELECT * FROM orders WHERE NOT EXISTS (SELECT * FROM users WHERE users.id = orders.user_id)',
      })).toThrow('WHERE NOT EXISTS with subqueries is not yet supported.')
    })
  })
})
