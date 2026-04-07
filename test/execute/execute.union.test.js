import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('UNION execution', () => {
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

  describe('UNION ALL', () => {
    it('should combine results from two queries', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age < 30
          UNION ALL
          SELECT name FROM users WHERE age > 30
        `,
      }))
      expect(result).toEqual([
        { name: 'Bob' },
        { name: 'Charlie' },
      ])
    })

    it('should include duplicates', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          UNION ALL
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice' },
        { name: 'Bob' },
        { name: 'Charlie' },
        { name: 'Alice' },
        { name: 'Charlie' },
      ])
    })

    it('should handle empty left side', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age > 100
          UNION ALL
          SELECT name FROM users WHERE age < 30
        `,
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })

    it('should handle empty right side', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age < 30
          UNION ALL
          SELECT name FROM users WHERE age > 100
        `,
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })
  })

  describe('UNION (distinct)', () => {
    it('should remove duplicates', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          UNION
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      const names = result.map(r => r.name).sort()
      expect(names).toEqual(['Alice', 'Bob', 'Charlie'])
    })

    it('should combine from different tables', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT id FROM users
          UNION
          SELECT id FROM orders
        `,
      }))
      expect(result).toHaveLength(3)
    })

    it('should reject set operations with mismatched column names', () => {
      expect(() => executeSql({
        tables: { users },
        query: `
          SELECT id AS a FROM users WHERE id = 1
          UNION
          SELECT id AS b FROM users WHERE id = 1
        `,
      })).toThrow('Set operation operands must have identical columns')
    })

    it('should reject set operations with mismatched column counts', () => {
      expect(() => executeSql({
        tables: { users },
        query: `
          SELECT id FROM users
          UNION
          SELECT id, name FROM users
        `,
      })).toThrow('Set operation operands must have identical columns')
    })
  })

  describe('INTERSECT', () => {
    it('should return only common rows', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          INTERSECT
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      const names = result.map(r => r.name).sort()
      expect(names).toEqual(['Alice', 'Charlie'])
    })

    it('should return empty when no overlap', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age < 30
          INTERSECT
          SELECT name FROM users WHERE age > 30
        `,
      }))
      expect(result).toEqual([])
    })

    it('should support INTERSECT ALL', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          INTERSECT ALL
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      const names = result.map(r => r.name).sort()
      expect(names).toEqual(['Alice', 'Charlie'])
    })
  })

  describe('EXCEPT', () => {
    it('should return rows from left not in right', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          EXCEPT
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })

    it('should return all when no overlap', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age < 30
          EXCEPT
          SELECT name FROM users WHERE age > 30
        `,
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })

    it('should return empty when all overlap', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 30
          EXCEPT
          SELECT name FROM users WHERE age >= 25
        `,
      }))
      expect(result).toEqual([])
    })

    it('should support EXCEPT ALL', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          EXCEPT ALL
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })
  })

  describe('chained set operations', () => {
    it('should chain UNION ALL', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE name = 'Alice'
          UNION ALL
          SELECT name FROM users WHERE name = 'Bob'
          UNION ALL
          SELECT name FROM users WHERE name = 'Charlie'
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice' },
        { name: 'Bob' },
        { name: 'Charlie' },
      ])
    })

    it('should chain mixed operations', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users
          UNION ALL
          SELECT name FROM users
          EXCEPT
          SELECT name FROM users WHERE name = 'Bob'
        `,
      }))
      // First UNION ALL produces 6 rows, then EXCEPT removes Bob
      const names = result.map(r => r.name).sort()
      expect(names).toEqual(['Alice', 'Charlie'])
    })

    it('should give INTERSECT higher precedence than UNION', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE name = 'Alice'
          UNION
          SELECT name FROM users WHERE name = 'Bob'
          INTERSECT
          SELECT name FROM users WHERE name = 'Bob'
        `,
      }))
      const names = result.map(r => r.name).sort()
      expect(names).toEqual(['Alice', 'Bob'])
    })
  })

  describe('UNION with ORDER BY and LIMIT', () => {
    it('should apply ORDER BY to full result', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name, age FROM users WHERE age < 30
          UNION ALL
          SELECT name, age FROM users WHERE age > 30
          ORDER BY age ASC
        `,
      }))
      expect(result).toEqual([
        { name: 'Bob', age: 25 },
        { name: 'Charlie', age: 35 },
      ])
    })

    it('should apply LIMIT to full result', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          UNION ALL
          SELECT name FROM users WHERE age >= 30
          LIMIT 3
        `,
      }))
      expect(result).toHaveLength(3)
    })

    it('should apply ORDER BY and LIMIT together', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name, age FROM users
          UNION ALL
          SELECT name, age FROM users
          ORDER BY age ASC
          LIMIT 2
        `,
      }))
      expect(result).toEqual([
        { name: 'Bob', age: 25 },
        { name: 'Bob', age: 25 },
      ])
    })
  })

  describe('UNION with CTE', () => {
    it('should work with CTE', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH young AS (SELECT name FROM users WHERE age < 30)
          SELECT name FROM young
          UNION ALL
          SELECT name FROM users WHERE age > 30
        `,
      }))
      expect(result).toEqual([
        { name: 'Bob' },
        { name: 'Charlie' },
      ])
    })
  })
})
