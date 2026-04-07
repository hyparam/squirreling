import { describe, expect, it } from 'vitest'
import { executeSql } from '../../src/index.js'
import { memorySource } from '../../src/backend/dataSource.js'

/**
 * @import { AsyncDataSource, ScanOptions } from '../../src/types.js'
 */

const users = [
  { id: 1, name: 'Alice', age: 30 },
  { id: 2, name: 'Bob', age: 25 },
  { id: 3, name: 'Charlie', age: 35 },
]

/** @type {AsyncDataSource} */
const noNumRowsSource = {
  columns: ['x'],
  scan() {
    async function* gen() {
      yield { columns: ['x'], cells: { x: () => Promise.resolve(1) } }
    }
    return {
      rows: gen,
      appliedWhere: false,
      appliedLimitOffset: false,
    }
  },
}

describe('numRows and maxRows', () => {
  describe('scan', () => {
    it('should return numRows and maxRows from data source', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users',
      })
      expect(result.numRows).toBe(3)
      expect(result.maxRows).toBe(3)
    })

    it('should return numRows and maxRows with LIMIT', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users LIMIT 2',
      })
      expect(result.numRows).toBe(2)
      expect(result.maxRows).toBe(2)
    })

    it('should return numRows and maxRows with OFFSET', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users LIMIT 10 OFFSET 2',
      })
      expect(result.numRows).toBe(1)
      expect(result.maxRows).toBe(1)
    })

    it('should return 0 with OFFSET exceeding rows', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users LIMIT 10 OFFSET 5',
      })
      expect(result.numRows).toBe(0)
      expect(result.maxRows).toBe(0)
    })

    it('should return maxRows but not numRows with WHERE', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users WHERE age > 25',
      })
      expect(result.numRows).toBeUndefined()
      expect(result.maxRows).toBe(3)
    })

    it('should return neither when data source lacks numRows', () => {
      const result = executeSql({
        tables: { data: noNumRowsSource },
        query: 'SELECT * FROM data',
      })
      expect(result.numRows).toBeUndefined()
      expect(result.maxRows).toBeUndefined()
    })
  })

  describe('scan fast path', () => {
    it('should return numRows and maxRows for single column scan', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name FROM users',
      })
      expect(result.numRows).toBe(3)
      expect(result.maxRows).toBe(3)
    })
  })

  describe('project', () => {
    it('should propagate numRows and maxRows through projection', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name, age FROM users',
      })
      expect(result.numRows).toBe(3)
      expect(result.maxRows).toBe(3)
    })
  })

  describe('sort', () => {
    it('should propagate numRows and maxRows through ORDER BY', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users ORDER BY age',
      })
      expect(result.numRows).toBe(3)
      expect(result.maxRows).toBe(3)
    })
  })

  describe('count', () => {
    it('should return 1 for COUNT(*)', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT COUNT(*) FROM users',
      })
      expect(result.numRows).toBe(1)
      expect(result.maxRows).toBe(1)
    })
  })

  describe('scalar aggregate', () => {
    it('should return 1 for aggregate without GROUP BY', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT SUM(age) FROM users',
      })
      expect(result.numRows).toBe(1)
      expect(result.maxRows).toBe(1)
    })

    it('should return maxRows 1 but not numRows with HAVING', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT SUM(age) FROM users HAVING SUM(age) > 100',
      })
      expect(result.numRows).toBeUndefined()
      expect(result.maxRows).toBe(1)
    })
  })

  describe('group by', () => {
    it('should return maxRows but not numRows for GROUP BY', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT age, COUNT(*) FROM users GROUP BY age',
      })
      expect(result.numRows).toBeUndefined()
      expect(result.maxRows).toBe(3)
    })
  })

  describe('distinct', () => {
    it('should return maxRows but not numRows for DISTINCT', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT DISTINCT age FROM users',
      })
      expect(result.numRows).toBeUndefined()
      expect(result.maxRows).toBe(3)
    })
  })

  describe('filter', () => {
    it('should return maxRows but not numRows through WHERE', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users WHERE age > 25',
      })
      expect(result.numRows).toBeUndefined()
      expect(result.maxRows).toBe(3)
    })
  })

  describe('limit', () => {
    it('should compute numRows and maxRows through LIMIT on sorted results', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users ORDER BY age LIMIT 2',
      })
      expect(result.numRows).toBe(2)
      expect(result.maxRows).toBe(2)
    })

    it('should compute maxRows through LIMIT with WHERE', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT * FROM users WHERE age > 25 LIMIT 2',
      })
      expect(result.numRows).toBeUndefined()
      expect(result.maxRows).toBe(2)
    })
  })

  describe('union', () => {
    it('should return numRows and maxRows for UNION ALL', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name FROM users UNION ALL SELECT name FROM users',
      })
      expect(result.numRows).toBe(6)
      expect(result.maxRows).toBe(6)
    })

    it('should return maxRows but not numRows for UNION', () => {
      const result = executeSql({
        tables: { users: memorySource({ data: users }) },
        query: 'SELECT name FROM users UNION SELECT name FROM users',
      })
      expect(result.numRows).toBeUndefined()
      expect(result.maxRows).toBe(6)
    })
  })

  describe('intersect', () => {
    it('should return min of maxRows for INTERSECT', () => {
      const result = executeSql({
        tables: {
          users: memorySource({ data: users }),
          others: memorySource({ data: [{ name: 'Alice' }, { name: 'Bob' }] }),
        },
        query: 'SELECT name FROM users INTERSECT SELECT name FROM others',
      })
      expect(result.numRows).toBeUndefined()
      expect(result.maxRows).toBe(2)
    })
  })

  describe('except', () => {
    it('should return left maxRows for EXCEPT', () => {
      const result = executeSql({
        tables: {
          users: memorySource({ data: users }),
          others: memorySource({ data: [{ name: 'Alice' }] }),
        },
        query: 'SELECT name FROM users EXCEPT SELECT name FROM others',
      })
      expect(result.numRows).toBeUndefined()
      expect(result.maxRows).toBe(3)
    })
  })

  describe('positional join', () => {
    it('should return max of both sides', () => {
      const result = executeSql({
        tables: {
          a: memorySource({ data: [{ x: 1 }, { x: 2 }] }),
          b: memorySource({ data: [{ y: 1 }, { y: 2 }, { y: 3 }] }),
        },
        query: 'SELECT * FROM a POSITIONAL JOIN b',
      })
      expect(result.numRows).toBe(3)
      expect(result.maxRows).toBe(3)
    })
  })
})
