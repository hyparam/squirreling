import { describe, expect, it } from 'vitest'
import { executeSql } from '../../src/execute/execute.js'
import { collect } from '../../src/index.js'

const users = [
  { id: 1, name: 'Alice', age: 30 },
  { id: 2, name: 'Bob', age: 25 },
]

/** @type {null} */
const NULL = null

describe('executeSql error handling', () => {
  describe('table not found errors', () => {
    it('should throw error when table does not exist', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM nonexistent',
      }))).rejects.toThrow('Table "nonexistent" not found')
    })

    it('should throw error for missing JOIN table', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM users JOIN orders ON users.id = orders.user_id',
      }))).rejects.toThrow('Table "orders" not found')
    })
  })

  describe('aggregate function errors', () => {
    it('should throw error for SUM(*)', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT SUM(*) FROM users',
      }))).rejects.toThrow('SUM(*) is not supported')
    })

    it('should throw error for AVG(*)', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT AVG(*) FROM users',
      }))).rejects.toThrow('AVG(*) is not supported')
    })

    it('should throw error for MIN(*)', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT MIN(*) FROM users',
      }))).rejects.toThrow('MIN(*) is not supported')
    })

    it('should throw error for MAX(*)', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT MAX(*) FROM users',
      }))).rejects.toThrow('MAX(*) is not supported')
    })

    it('should throw error for JSON_ARRAYAGG(*)', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT JSON_ARRAYAGG(*) FROM users',
      }))).rejects.toThrow('JSON_ARRAYAGG(*) is not supported')
    })
  })

  describe('function argument count errors', () => {
    it('should throw error for UPPER with wrong arg count', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT UPPER(name, age) FROM users',
      }))).rejects.toThrow('UPPER(string) function requires 1 argument, got 2')
    })

    it('should throw error for LOWER with wrong arg count', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT LOWER(name, age) FROM users',
      }))).rejects.toThrow('LOWER(string) function requires 1 argument, got 2')
    })

    it('should throw error for LENGTH with wrong arg count', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT LENGTH(name, age) FROM users',
      }))).rejects.toThrow('LENGTH(string) function requires 1 argument, got 2')
    })

    it('should throw error for TRIM with wrong arg count', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT TRIM(name, age) FROM users',
      }))).rejects.toThrow('TRIM(string) function requires 1 argument, got 2')
    })

    it('should throw error for REPLACE with wrong arg count', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT REPLACE(name, \'a\') FROM users',
      }))).rejects.toThrow('REPLACE(string, search, replacement) function requires 3 arguments, got 2')
    })

    it('should throw error for SUBSTRING with wrong arg count', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT SUBSTRING(name) FROM users',
      }))).rejects.toThrow('SUBSTRING(string, start[, length]) function requires 2 or 3 arguments, got 1')
    })

    it('should throw error for CONCAT with no args', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT CONCAT() FROM users',
      }))).rejects.toThrow('CONCAT(value1, value2[, ...]) function requires at least 1 argument, got 0')
    })

    it('should throw error for RANDOM with args', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT RANDOM(1) FROM users',
      }))).rejects.toThrow('RANDOM() function requires no arguments, got 1')
    })

    it('should throw error for CURRENT_DATE with args', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT CURRENT_DATE(1) FROM users',
      }))).rejects.toThrow('CURRENT_DATE() function requires no arguments, got 1')
    })

    it('should throw error for JSON_VALUE with wrong arg count', async () => {
      const data = [{ json: '{"a":1}' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json) FROM data',
      }))).rejects.toThrow('JSON_VALUE(expression, path) function requires 2 arguments, got 1')
    })
  })

  describe('function argument value errors', () => {
    it('should throw error for SUBSTRING with invalid start position', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT SUBSTRING(name, 0, 3) FROM users',
      }))).rejects.toThrow('SUBSTRING(string, start[, length]): start position must be a positive integer, got 0')
    })

    it('should throw error for SUBSTRING with negative length', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT SUBSTRING(name, 1, -1) FROM users',
      }))).rejects.toThrow('SUBSTRING(string, start[, length]): length must be a non-negative integer, got -1')
    })

    it('should throw error for JSON_VALUE with invalid JSON', async () => {
      const data = [{ json: 'not json' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.a\') FROM data',
      }))).rejects.toThrow('JSON_VALUE(expression, path): invalid JSON string')
    })

    it('should throw error for JSON_VALUE with non-object argument', async () => {
      const data = [{ val: 123 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(val, \'$.a\') FROM data',
      }))).rejects.toThrow('JSON_VALUE(expression, path): first argument must be JSON string or object, got number')
    })

    it('should throw error for JSON_OBJECT with null key', async () => {
      const data = [{ key: NULL, val: 1 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(key, val) FROM data',
      }))).rejects.toThrow('JSON_OBJECT(key1, value1[, ...]): key cannot be null. All keys must be non-null values.')
    })

    it('should throw error for JSON_OBJECT with odd arguments', async () => {
      const data = [{ id: 1 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'a\', 1, \'b\') FROM data',
      }))).rejects.toThrow('JSON_OBJECT(key1, value1[, ...]) function requires even number arguments, got 3')
    })
  })

  describe('CAST errors', () => {
    it('should throw error for unsupported CAST type', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT CAST(age AS BINARY) FROM users',
      }))).rejects.toThrow('Unsupported CAST to type BINARY. Supported types: TEXT, VARCHAR, INTEGER, INT, BIGINT, FLOAT, REAL, DOUBLE, BOOLEAN')
    })

    it('should throw error when casting object to non-string type', async () => {
      const data = [{ obj: { a: 1 } }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT CAST(obj AS INTEGER) FROM data',
      }))).rejects.toThrow('Cannot CAST object to INTEGER. Supported types: TEXT, VARCHAR, INTEGER, INT, BIGINT, FLOAT, REAL, DOUBLE, BOOLEAN')
    })
  })

  describe('INTERVAL errors', () => {
    it('should throw error for standalone INTERVAL', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT INTERVAL 1 DAY FROM users',
      }))).rejects.toThrow('INTERVAL can only be used with date arithmetic (+ or -')
    })

    it('should throw error for invalid INTERVAL unit', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT CURRENT_DATE + INTERVAL 1 FORTNIGHT FROM users',
      }))).rejects.toThrow('Invalid interval unit FORTNIGHT at position 33. Valid values: DAY, MONTH, YEAR, HOUR, MINUTE, SECOND')
    })
  })

  describe('row number in errors', () => {
    const bad = [
      { id: 1, val: '{"x":1}' },
      { id: 2, val: '{"x":2}' },
      { id: 3, val: 'bad json' },
    ]
    it('should include row number in WHERE clause errors (streaming)', async () => {
      await expect(collect(executeSql({
        tables: { bad },
        query: 'SELECT * FROM bad WHERE JSON_VALUE(val, \'$.x\') IS NOT NULL',
      }))).rejects.toThrow('JSON_VALUE(expression, path): invalid JSON string. First argument must be valid JSON. (row 3)')
    })

    it('should include row number in streaming projection errors', async () => {
      await expect(collect(executeSql({
        tables: { bad },
        query: 'SELECT JSON_VALUE(val, \'$.x\') FROM bad',
      }))).rejects.toThrow('JSON_VALUE(expression, path): invalid JSON string. First argument must be valid JSON. (row 3)')
    })

    it('should include row number for specific row in multi-row error', async () => {
      const data = [
        { id: 1, val: 5 },
        { id: 2, val: 0 }, // SUBSTRING with 0 start will fail
        { id: 3, val: 3 },
      ]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTRING(\'hello\', val, 2) FROM data',
      }))).rejects.toThrow('SUBSTRING(string, start[, length]): start position must be a positive integer, got 0. SQL uses 1-based indexing. (row 2)')
    })

    it('should include row number in WHERE clause errors (buffered)', async () => {
      // ORDER BY forces buffered path
      await expect(collect(executeSql({
        tables: { bad },
        query: 'SELECT * FROM bad WHERE JSON_VALUE(val, \'$.x\') IS NOT NULL ORDER BY id',
      }))).rejects.toThrow('JSON_VALUE(expression, path): invalid JSON string. First argument must be valid JSON. (row 3)')
    })
  })
})
