import { describe, expect, it } from 'vitest'
import { executeSql } from '../../src/index.js'
import { memorySource } from '../../src/backend/dataSource.js'

const users = [
  { id: 1, name: 'Alice', age: 30 },
  { id: 2, name: 'Bob', age: 25 },
  { id: 3, name: 'Charlie', age: 35 },
]

describe('QueryResults.columns', () => {
  describe('scan', () => {
    it('should return all columns for SELECT *', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users',
      })
      expect(result.columns).toEqual(['id', 'name', 'age'])
    })

    it('should return hinted columns for single column scan', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name FROM users',
      })
      expect(result.columns).toEqual(['name'])
    })
  })

  describe('project', () => {
    it('should return projected column names', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name, age FROM users',
      })
      expect(result.columns).toEqual(['name', 'age'])
    })

    it('should return aliased column names', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name AS n, age AS a FROM users',
      })
      expect(result.columns).toEqual(['n', 'a'])
    })

    it('should return derived expression aliases', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT age + 1 FROM users',
      })
      expect(result.columns).toEqual(['age_+_1'])
    })
  })

  describe('filter', () => {
    it('should pass through columns from child', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users WHERE age > 25',
      })
      expect(result.columns).toEqual(['id', 'name', 'age'])
    })
  })

  describe('sort', () => {
    it('should pass through columns from child', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users ORDER BY age',
      })
      expect(result.columns).toEqual(['id', 'name', 'age'])
    })
  })

  describe('distinct', () => {
    it('should pass through columns from child', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT DISTINCT age FROM users',
      })
      expect(result.columns).toEqual(['age'])
    })
  })

  describe('limit', () => {
    it('should pass through columns from child', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name FROM users LIMIT 1',
      })
      expect(result.columns).toEqual(['name'])
    })
  })

  describe('count', () => {
    it('should return count alias', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT COUNT(*) FROM users',
      })
      expect(result.columns).toEqual(['count_all'])
    })

    it('should return custom alias', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT COUNT(*) AS total FROM users',
      })
      expect(result.columns).toEqual(['total'])
    })
  })

  describe('scalar aggregate', () => {
    it('should return aggregate column names', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT SUM(age) AS total, AVG(age) AS avg_age FROM users',
      })
      expect(result.columns).toEqual(['total', 'avg_age'])
    })
  })

  describe('group by', () => {
    it('should return grouped and aggregate columns', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT age, COUNT(*) AS cnt FROM users GROUP BY age',
      })
      expect(result.columns).toEqual(['age', 'cnt'])
    })
  })

  describe('join', () => {
    it('should return projected columns from hash join', () => {
      const result = executeSql({
        tables: {
          users: memorySource({ data: [{ id: 1, name: 'Alice' }] }),
          orders: memorySource({ data: [{ user_id: 1, item: 'book' }] }),
        },
        query: 'SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.user_id',
      })
      expect(result.columns).toEqual(['name', 'item'])
    })

    it('should return projected columns from positional join', () => {
      const result = executeSql({
        tables: {
          a: memorySource({ data: [{ x: 1 }] }),
          b: memorySource({ data: [{ y: 2 }] }),
        },
        query: 'SELECT a.x, b.y FROM a POSITIONAL JOIN b',
      })
      expect(result.columns).toEqual(['x', 'y'])
    })
  })

  describe('set operations', () => {
    it('should return columns for UNION ALL', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name, age FROM users UNION ALL SELECT name, age FROM users',
      })
      expect(result.columns).toEqual(['name', 'age'])
    })

    it('should return columns for UNION', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name FROM users UNION SELECT name FROM users',
      })
      expect(result.columns).toEqual(['name'])
    })

    it('should return columns for INTERSECT', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name FROM users INTERSECT SELECT name FROM users',
      })
      expect(result.columns).toEqual(['name'])
    })

    it('should return columns for EXCEPT', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name FROM users EXCEPT SELECT name FROM users',
      })
      expect(result.columns).toEqual(['name'])
    })
  })
})
