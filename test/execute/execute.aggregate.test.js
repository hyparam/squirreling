import { describe, expect, it } from 'vitest'
import { executeSql } from '../../src/execute/execute.js'

describe('executeSql', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
  ]

  describe('aggregate functions', () => {
    it('should count all rows with COUNT(*)', () => {
      const result = executeSql(users, 'SELECT COUNT(*) FROM users')
      expect(result).toEqual([{ count_all: 5 }])
    })

    it('should count column with COUNT(column)', () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: null },
        { id: 3, name: 'Charlie' },
      ]
      const result = executeSql(data, 'SELECT COUNT(name) FROM users')
      expect(result).toEqual([{ count_name: 2 }])
    })

    it('should calculate SUM', () => {
      const result = executeSql(users, 'SELECT SUM(age) FROM users')
      expect(result).toEqual([{ sum_age: 148 }])
    })

    it('should calculate AVG', () => {
      const result = executeSql(users, 'SELECT AVG(age) FROM users')
      expect(result).toEqual([{ avg_age: 29.6 }])
    })

    it('should calculate MIN and MAX', () => {
      const result = executeSql(users, 'SELECT MIN(age) AS min_age, MAX(age) AS max_age FROM users')
      expect(result).toEqual([{ min_age: 25, max_age: 35 }])
    })

    it('should handle aggregate with alias', () => {
      const result = executeSql(users, 'SELECT COUNT(*) AS total FROM users')
      expect(result).toEqual([{ total: 5 }])
    })

    it('should handle empty dataset for aggregates', () => {
      const result = executeSql([], 'SELECT AVG(age) FROM users')
      expect(result).toEqual([{ avg_age: null }])
    })

    it('should skip non-numeric values in SUM/AVG/MIN/MAX', () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 'abc' },
        { id: 4, value: 20 },
      ]
      const result = executeSql(data, 'SELECT SUM(value) AS total, AVG(value) AS avg FROM users')
      expect(result).toEqual([{ total: 30, avg: 15 }])
    })

    it('should throw error for SUM/AVG/MIN/MAX with star', () => {
      expect(() => executeSql(users, 'SELECT SUM(*) FROM users'))
        .toThrow('SUM(*) is not supported')
    })

    it('should handle aggregate without GROUP BY (single group)', () => {
      const result = executeSql(users, 'SELECT COUNT(*) FROM users')
      expect(result).toEqual([{ count_all: 5 }])
    })

    it('should handle mixing columns with aggregates without GROUP BY (takes first row)', () => {
      const result = executeSql(users, 'SELECT name, COUNT(*) FROM users')
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice') // First row
      expect(result[0].count_all).toBe(5)
    })
  })

  describe('null handling in aggregates', () => {
    it('should handle null in aggregate functions correctly', () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: null },
      ]
      const result = executeSql(data, 'SELECT COUNT(*) AS total, COUNT(value) AS non_null FROM users')
      expect(result[0]).toEqual({ total: 3, non_null: 1 })
    })

    it('should handle null in GROUP BY', () => {
      const data = [
        { id: 1, category: 'A', value: 10 },
        { id: 2, category: null, value: 20 },
        { id: 3, category: null, value: 30 },
        { id: 4, category: 'A', value: 40 },
      ]
      const result = executeSql(data, `
        SELECT category, SUM(value) AS total
        FROM users
        GROUP BY category
      `)
      expect(result).toHaveLength(2)
      const nullGroup = result.find(r => r.category === null)
      expect(nullGroup?.total).toBe(50)
    })
  })
})
