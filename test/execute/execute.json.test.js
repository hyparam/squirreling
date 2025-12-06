import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/** @type {null} */
const NULL = null

describe('string functions', () => {
  describe('JSON_VALUE', () => {
    it('should extract a simple property', async () => {
      const data = [{ id: 1, json: { name: 'Alice' } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.name\') AS name FROM data',
      }))
      expect(result).toEqual([{ name: 'Alice' }])
    })

    it('should extract a nested property', async () => {
      const data = [{ id: 1, json: { user: { name: 'Bob', age: 30 } } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.user.name\') AS name FROM data',
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })

    it('should extract array element by index', async () => {
      const data = [{ id: 1, json: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$[1]\') AS val FROM data',
      }))
      expect(result).toEqual([{ val: 20 }])
    })

    it('should handle mixed object and array paths', async () => {
      const data = [{ id: 1, json: { items: [{ id: 1, name: 'foo' }, { id: 2, name: 'bar' }] } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.items[1].name\') AS name FROM data',
      }))
      expect(result).toEqual([{ name: 'bar' }])
    })

    it('should return null for non-existent path', async () => {
      const data = [{ id: 1, json: { name: 'Alice' } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.age\') AS age FROM data',
      }))
      expect(result).toEqual([{ age: null }])
    })

    it('should return null when JSON input is null', async () => {
      const data = [{ id: 1, json: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.name\') AS name FROM data',
      }))
      expect(result).toEqual([{ name: null }])
    })

    it('should return null when path is null', async () => {
      const data = [{ id: 1, json: { name: 'Alice' }, path: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, path) AS name FROM data',
      }))
      expect(result).toEqual([{ name: null }])
    })

    it('should extract numeric values', async () => {
      const data = [{ id: 1, json: { count: 42 } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.count\') AS count FROM data',
      }))
      expect(result).toEqual([{ count: 42 }])
    })

    it('should extract boolean values', async () => {
      const data = [{ id: 1, json: { active: true } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.active\') AS active FROM data',
      }))
      expect(result).toEqual([{ active: true }])
    })

    it('should extract null values from JSON', async () => {
      const data = [{ id: 1, json: { value: NULL } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.value\') AS val FROM data',
      }))
      expect(result).toEqual([{ val: null }])
    })

    it('should return nested object when path points to object', async () => {
      const data = [{ id: 1, json: { user: { name: 'Alice', age: 30 } } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.user\') AS user FROM data',
      }))
      expect(result).toEqual([{ user: { name: 'Alice', age: 30 } }])
    })

    it('should return array when path points to array', async () => {
      const data = [{ id: 1, json: { items: [1, 2, 3] } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.items\') AS items FROM data',
      }))
      expect(result).toEqual([{ items: [1, 2, 3] }])
    })

    it('should parse and extract from JSON string', async () => {
      const data = [{ id: 1, json: '{"name":"Alice","age":30}' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.name\') AS name FROM data',
      }))
      expect(result).toEqual([{ name: 'Alice' }])
    })

    it('should work without $ prefix in path', async () => {
      const data = [{ id: 1, json: { name: 'Alice' } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'name\') AS name FROM data',
      }))
      expect(result).toEqual([{ name: 'Alice' }])
    })

    it('should throw for invalid JSON string', async () => {
      const data = [{ id: 1, json: 'not valid json' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.name\') AS name FROM data' })))
        .rejects.toThrow('JSON_VALUE: invalid JSON string')
    })

    it('should throw when first argument is neither string nor object', async () => {
      const data = [{ id: 1, num: 42 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(num, \'$.name\') AS name FROM data' })))
        .rejects.toThrow('JSON_VALUE: first argument must be JSON string or object')
    })

    it('should return null when array index is out of bounds', async () => {
      const data = [{ id: 1, json: [1, 2, 3] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$[10]\') AS val FROM data',
      }))
      expect(result).toEqual([{ val: null }])
    })

    it('should work with deeply nested paths', async () => {
      const data = [{ id: 1, json: { a: { b: { c: { d: 'deep' } } } } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.a.b.c.d\') AS val FROM data',
      }))
      expect(result).toEqual([{ val: 'deep' }])
    })

    it('should work without alias', async () => {
      const data = [{ id: 1, json: { name: 'Alice' } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(json, \'$.name\') FROM data',
      }))
      expect(result[0]).toHaveProperty('json_value_json_$.name')
    })
  })

  describe('JSON_QUERY', () => {
    it('should work as an alias for JSON_VALUE', async () => {
      const data = [{ id: 1, json: { user: { name: 'Alice' } } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_QUERY(json, \'$.user.name\') AS name FROM data',
      }))
      expect(result).toEqual([{ name: 'Alice' }])
    })
  })

})
