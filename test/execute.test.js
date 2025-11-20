import { describe, expect, it } from 'vitest'
import { executeSql } from '../src/execute.js'

describe('executeSql', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
  ]

  describe('basic SELECT queries', () => {
    it('should select all columns', () => {
      const result = executeSql(users, 'SELECT * FROM users')
      expect(result).toEqual(users)
    })

    it('should select specific columns', () => {
      const result = executeSql(users, 'SELECT name, age FROM users')
      expect(result).toEqual([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
        { name: 'Charlie', age: 35 },
        { name: 'Diana', age: 28 },
        { name: 'Eve', age: 30 },
      ])
    })

    it('should handle column aliases', () => {
      const result = executeSql(users, 'SELECT name AS fullName, age AS years FROM users')
      expect(result[0]).toEqual({ fullName: 'Alice', years: 30 })
    })

    it('should handle empty dataset', () => {
      const result = executeSql([], 'SELECT * FROM users')
      expect(result).toEqual([])
    })
  })

  describe('WHERE clause', () => {
    it('should filter with equality', () => {
      const result = executeSql(users, 'SELECT * FROM users WHERE name = \'Alice\'')
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice')
    })

    it('should filter with comparison operators', () => {
      const result = executeSql(users, 'SELECT * FROM users WHERE age > 30')
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })

    it('should filter with AND', () => {
      const result = executeSql(users, 'SELECT * FROM users WHERE city = \'NYC\' AND age = 30')
      expect(result).toHaveLength(2)
      expect(result.every(u => u.city === 'NYC' && u.age === 30)).toBe(true)
    })

    it('should filter with OR', () => {
      const result = executeSql(users, 'SELECT * FROM users WHERE age < 26 OR age > 33')
      expect(result).toHaveLength(2)
      expect(result.map(u => u.name).sort()).toEqual(['Bob', 'Charlie'])
    })

    it('should handle complex WHERE with parentheses', () => {
      const result = executeSql(users, `
        SELECT * FROM users
        WHERE (age < 28 OR age > 32) AND city = 'NYC'
      `)
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })

    it('should handle OR precedence without parentheses', () => {
      const result = executeSql(users, `
        SELECT * FROM users
        WHERE city = 'NYC' AND age = 30 OR city = 'LA'
      `)
      // Should be: (city = 'NYC' AND age = 30) OR (city = 'LA')
      expect(result).toHaveLength(4)
    })

    it('should filter with NOT', () => {
      const result = executeSql(users, 'SELECT * FROM users WHERE NOT active')
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })

    it('should handle inequality operators', () => {
      const result1 = executeSql(users, 'SELECT * FROM users WHERE age != 30')
      expect(result1).toHaveLength(3)

      const result2 = executeSql(users, 'SELECT * FROM users WHERE age <> 30')
      expect(result2).toHaveLength(3)
    })

    it('should handle <= and >= operators', () => {
      const result1 = executeSql(users, 'SELECT * FROM users WHERE age <= 28')
      expect(result1).toHaveLength(2)

      const result2 = executeSql(users, 'SELECT * FROM users WHERE age >= 30')
      expect(result2).toHaveLength(3)
    })

    it('should handle literal values in WHERE', () => {
      const result = executeSql(users, 'SELECT * FROM users WHERE active = TRUE')
      expect(result).toHaveLength(4)
    })

    it('should handle IS NULL', () => {
      const data = [
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: null },
        { id: 3, name: 'Charlie', email: null },
        { id: 4, name: 'Diana', email: 'diana@example.com' },
      ]
      const result = executeSql(data, 'SELECT * FROM users WHERE email IS NULL')
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
      const result = executeSql(data, 'SELECT * FROM users WHERE email IS NOT NULL')
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Diana'])
    })

    it('should handle IS NULL with undefined values', () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob', email: 'bob@example.com' },
        { id: 3, name: 'Charlie' },
      ]
      const result = executeSql(data, 'SELECT * FROM users WHERE email IS NULL')
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

      const result1 = executeSql(data, 'SELECT * FROM users WHERE email IS NULL AND phone IS NOT NULL')
      expect(result1).toHaveLength(1)
      expect(result1[0].name).toBe('Bob')

      const result2 = executeSql(data, 'SELECT * FROM users WHERE email IS NULL OR phone IS NULL')
      expect(result2).toHaveLength(3)
      expect(result2.map(r => r.name).sort()).toEqual(['Bob', 'Charlie', 'Diana'])
    })

    it('should NOT match NULL with equality', () => {
      const data = [
        { id: 1, value: null },
        { id: 2, value: 0 },
        { id: 3, value: false },
      ]
      const result = executeSql(data, 'SELECT * FROM users WHERE value = NULL')
      expect(result).toHaveLength(0) // NULL comparisons should return false
    })

    it('should filter with LIKE', () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
        { id: 4, name: 'Diana' },
      ]
      const result = executeSql(data, 'SELECT * FROM users WHERE name LIKE \'%li%\'')
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
      const result = executeSql(data, 'SELECT * FROM users WHERE code LIKE \'A1_3\'')
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
      const result = executeSql(data, 'SELECT * FROM users WHERE email LIKE \'_____@example.___\'')
      expect(result).toHaveLength(2)
      expect(result.map(r => r.email).sort()).toEqual(['alice@example.com', 'diana@example.com'])
    })
  })

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

  describe('DISTINCT', () => {
    it('should return distinct rows', () => {
      const data = [
        { city: 'NYC', age: 30 },
        { city: 'LA', age: 25 },
        { city: 'NYC', age: 30 },
        { city: 'LA', age: 25 },
      ]
      const result = executeSql(data, 'SELECT DISTINCT city, age FROM users')
      expect(result).toHaveLength(2)
    })

    it('should handle DISTINCT with single column', () => {
      const result = executeSql(users, 'SELECT DISTINCT city FROM users')
      expect(result).toHaveLength(2)
      expect(result.map(r => r.city).sort()).toEqual(['LA', 'NYC'])
    })

    it('should not affect non-distinct queries', () => {
      const result = executeSql(users, 'SELECT city FROM users')
      expect(result).toHaveLength(5)
    })
  })

  describe('ORDER BY', () => {
    it('should sort ascending by default', () => {
      const result = executeSql(users, 'SELECT * FROM users ORDER BY age')
      expect(result[0].age).toBe(25)
      expect(result[result.length - 1].age).toBe(35)
    })

    it('should sort descending', () => {
      const result = executeSql(users, 'SELECT * FROM users ORDER BY age DESC')
      expect(result[0].age).toBe(35)
      expect(result[result.length - 1].age).toBe(25)
    })

    it('should sort by multiple columns', () => {
      const result = executeSql(users, 'SELECT * FROM users ORDER BY age ASC, name DESC')
      expect(result[0].name).toBe('Bob') // age 25
      const age30s = result.filter(r => r.age === 30)
      expect(age30s[0].name).toBe('Eve') // DESC order
    })

    it('should handle null/undefined values in sorting', () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 5 },
      ]
      const result = executeSql(data, 'SELECT * FROM users ORDER BY value')
      expect(result[0].value).toBe(null) // null comes first
      expect(result[1].value).toBe(5)
    })

    it('should handle string sorting', () => {
      const result = executeSql(users, 'SELECT * FROM users ORDER BY name')
      expect(result[0].name).toBe('Alice')
      expect(result[result.length - 1].name).toBe('Eve')
    })
  })

  describe('LIMIT and OFFSET', () => {
    it('should limit results', () => {
      const result = executeSql(users, 'SELECT * FROM users LIMIT 2')
      expect(result).toHaveLength(2)
    })

    it('should apply offset', () => {
      const result = executeSql(users, 'SELECT * FROM users OFFSET 2')
      expect(result).toHaveLength(3)
    })

    it('should apply both LIMIT and OFFSET', () => {
      const result = executeSql(users, 'SELECT * FROM users ORDER BY name LIMIT 2 OFFSET 1')
      expect(result).toHaveLength(2)
      expect(result[0].name).toBe('Bob')
      expect(result[1].name).toBe('Charlie')
    })

    it('should handle LIMIT larger than result set', () => {
      const result = executeSql(users, 'SELECT * FROM users LIMIT 100')
      expect(result).toHaveLength(5)
    })

    it('should handle OFFSET larger than result set', () => {
      const result = executeSql(users, 'SELECT * FROM users OFFSET 100')
      expect(result).toHaveLength(0)
    })
  })

  describe('complex queries', () => {
    it('should handle WHERE + GROUP BY + ORDER BY + LIMIT', () => {
      const result = executeSql(users, `
        SELECT city, COUNT(*) AS count
        FROM users
        WHERE age >= 28
        GROUP BY city
        ORDER BY count DESC
        LIMIT 1
      `)
      expect(result).toHaveLength(1)
      expect(result[0].city).toBe('NYC')
      expect(result[0].count).toBe(3)
    })

    it('should handle DISTINCT + ORDER BY + LIMIT', () => {
      const result = executeSql(users, `
        SELECT DISTINCT age
        FROM users
        ORDER BY age DESC
        LIMIT 3
      `)
      expect(result).toHaveLength(3)
      expect(result[0].age).toBe(35)
    })

    it('should apply operations in correct order', () => {
      // WHERE -> DISTINCT -> ORDER BY -> LIMIT -> OFFSET
      const result = executeSql(users, 'SELECT age FROM users WHERE city = \'NYC\' ORDER BY age LIMIT 1 OFFSET 1')
      expect(result).toHaveLength(1)
      expect(result[0].age).toBe(30) // Second age value after sorting (30, 30, 35)
    })
  })

  describe('error cases', () => {
    it('should throw error for SUM with star', () => {
      expect(() => executeSql(users, 'SELECT SUM(*) FROM users'))
        .toThrow('SUM(*) is not supported')
    })

    it('should throw error for AVG with star', () => {
      expect(() => executeSql(users, 'SELECT AVG(*) FROM users'))
        .toThrow('AVG(*) is not supported')
    })

    it('should throw error for MIN with star', () => {
      expect(() => executeSql(users, 'SELECT MIN(*) FROM users'))
        .toThrow('MIN(*) is not supported')
    })

    it('should throw error for MAX with star', () => {
      expect(() => executeSql(users, 'SELECT MAX(*) FROM users'))
        .toThrow('MAX(*) is not supported')
    })
  })

  describe('JOIN queries', () => {
    it('should throw error for JOIN queries', () => {
      expect(() => executeSql(users, 'SELECT * FROM users JOIN orders ON users.id = orders.user_id'))
        .toThrow('JOIN is not supported')
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
      expect(nullGroup.total).toBe(50)
    })
  })

  describe('edge cases', () => {
    it('should handle rows with different keys', () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, email: 'bob@example.com' },
        { id: 3, name: 'Charlie', email: 'charlie@example.com' },
      ]
      const result = executeSql(data, 'SELECT * FROM users')
      expect(result).toEqual(data)
    })

    it('should handle string comparisons lexicographically', () => {
      const data = [
        { id: 1, value: '10' },
        { id: 2, value: '5' },
        { id: 3, value: '20' },
      ]
      // Lexicographic comparison: '5' > '2' and '20' > '2' are both true
      const result = executeSql(data, 'SELECT * FROM users WHERE value > \'2\'')
      expect(result).toHaveLength(2)
      expect(result.map(r => r.value).sort()).toEqual(['20', '5'])
    })

    it('should handle boolean values correctly', () => {
      const result = executeSql(users, 'SELECT * FROM users WHERE active')
      expect(result).toHaveLength(4)
    })

    it('should handle falsy values in WHERE clause', () => {
      const data = [
        { id: 1, value: 0 },
        { id: 2, value: 1 },
        { id: 3, value: false },
        { id: 4, value: true },
      ]
      const result = executeSql(data, 'SELECT * FROM users WHERE value')
      expect(result).toHaveLength(2)
      expect(result.every(r => r.value)).toBe(true)
    })

    it('should handle empty string in comparisons', () => {
      const data = [
        { id: 1, value: '' },
        { id: 2, value: 'hello' },
        { id: 3, value: null },
      ]
      const result = executeSql(data, 'SELECT * FROM users WHERE value = \'\'')
      expect(result).toHaveLength(1)
      expect(result[0].id).toBe(1)
    })

    it('should handle special characters in strings', () => {
      const data = [
        { id: 1, name: 'O\'Brien' },
        { id: 2, name: 'Smith' },
      ]
      const result = executeSql(data, 'SELECT * FROM users WHERE name = \'O\'\'Brien\'')
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('O\'Brien')
    })

    it('should handle mixed types in ORDER BY', () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: '5' },
        { id: 3, value: 20 },
        { id: 4, value: '15' },
      ]
      const result = executeSql(data, 'SELECT * FROM users ORDER BY value')
      // Should sort lexicographically for mixed types
      expect(result[0].value).toBe(10)
    })

    it('should handle very long column names', () => {
      const data = [{ id: 1, very_long_column_name_that_exceeds_normal_limits: 'value' }]
      const result = executeSql(data, 'SELECT very_long_column_name_that_exceeds_normal_limits FROM users')
      expect(result[0].very_long_column_name_that_exceeds_normal_limits).toBe('value')
    })
  })
})
