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
        .rejects.toThrow('JSON_VALUE(expression, path): invalid JSON string')
    })

    it('should throw when first argument is neither string nor object', async () => {
      const data = [{ id: 1, num: 42 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALUE(num, \'$.name\') AS name FROM data' })))
        .rejects.toThrow('JSON_VALUE(expression, path): first argument must be JSON string or object')
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

  describe('JSON_EXTRACT', () => {
    it('should work as an alias for JSON_VALUE', async () => {
      const data = [{ id: 1, json: { user: { name: 'Alice' } } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_EXTRACT(json, \'$.user.name\') AS name FROM data',
      }))
      expect(result).toEqual([{ name: 'Alice' }])
    })
  })

  describe('JSON_ARRAY_LENGTH', () => {
    it('should return the length of a JSON array', async () => {
      const data = [{ id: 1, json: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_ARRAY_LENGTH(json) AS len FROM data',
      }))
      expect(result).toEqual([{ len: 3 }])
    })

    it('should return 0 for an empty array', async () => {
      const result = await collect(executeSql({
        tables: { data: [{ id: 1, json: [] }] },
        query: 'SELECT JSON_ARRAY_LENGTH(json) AS len FROM data',
      }))
      expect(result).toEqual([{ len: 0 }])
    })

    it('should parse a JSON string array', async () => {
      const data = [{ id: 1, json: '[1,2,3,4]' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_ARRAY_LENGTH(json) AS len FROM data',
      }))
      expect(result).toEqual([{ len: 4 }])
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, json: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_ARRAY_LENGTH(json) AS len FROM data',
      }))
      expect(result).toEqual([{ len: null }])
    })

    it('should return null when input is an object, not an array', async () => {
      const data = [{ id: 1, json: { a: 1 } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_ARRAY_LENGTH(json) AS len FROM data',
      }))
      expect(result).toEqual([{ len: null }])
    })

    it('should throw for invalid JSON string', async () => {
      const data = [{ id: 1, json: 'not valid json' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_ARRAY_LENGTH(json) AS len FROM data' })))
        .rejects.toThrow('JSON_ARRAY_LENGTH(array): invalid JSON string')
    })

    it('should throw for wrong argument count', () => {
      const data = [{ id: 1 }]
      expect(() => executeSql({
        tables: { data },
        query: 'SELECT JSON_ARRAY_LENGTH() FROM data',
      })).toThrow('JSON_ARRAY_LENGTH(array) function requires 1 argument, got 0')
    })
  })

  describe('JSON_VALID', () => {
    it('should return true for a valid JSON string', async () => {
      const data = [{ id: 1, json: '{"name":"Alice"}' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALID(json) AS valid FROM data',
      }))
      expect(result).toEqual([{ valid: true }])
    })

    it('should return true for a valid JSON array string', async () => {
      const data = [{ id: 1, json: '[1,2,3]' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALID(json) AS valid FROM data',
      }))
      expect(result).toEqual([{ valid: true }])
    })

    it('should return true for a valid JSON primitive string', async () => {
      const data = [{ id: 1, json: '42' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALID(json) AS valid FROM data',
      }))
      expect(result).toEqual([{ valid: true }])
    })

    it('should return false for an invalid JSON string', async () => {
      const data = [{ id: 1, json: 'not valid json' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALID(json) AS valid FROM data',
      }))
      expect(result).toEqual([{ valid: false }])
    })

    it('should return false for an unterminated JSON string', async () => {
      const data = [{ id: 1, json: '{"name":"Alice"' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALID(json) AS valid FROM data',
      }))
      expect(result).toEqual([{ valid: false }])
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, json: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALID(json) AS valid FROM data',
      }))
      expect(result).toEqual([{ valid: null }])
    })

    it('should return false for non-string input', async () => {
      const data = [{ id: 1, num: 42 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALID(num) AS valid FROM data',
      }))
      expect(result).toEqual([{ valid: false }])
    })

    it('should return false for object input', async () => {
      const data = [{ id: 1, json: { name: 'Alice' } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALID(json) AS valid FROM data',
      }))
      expect(result).toEqual([{ valid: false }])
    })

    it('should throw for wrong argument count', () => {
      const data = [{ id: 1 }]
      expect(() => executeSql({
        tables: { data },
        query: 'SELECT JSON_VALID() FROM data',
      })).toThrow('JSON_VALID(value) function requires 1 argument, got 0')
    })

    it('should work without alias', async () => {
      const data = [{ id: 1, json: '{}' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_VALID(json) FROM data',
      }))
      expect(result[0]).toHaveProperty('json_valid_json')
    })
  })

  describe('JSON_TYPE', () => {
    it('should return object for a JSON object string', async () => {
      const data = [{ id: 1, json: '{"name":"Alice"}' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) AS type FROM data',
      }))
      expect(result).toEqual([{ type: 'object' }])
    })

    it('should return array for a JSON array string', async () => {
      const data = [{ id: 1, json: '[1,2,3]' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) AS type FROM data',
      }))
      expect(result).toEqual([{ type: 'array' }])
    })

    it('should return string for a JSON string literal', async () => {
      const data = [{ id: 1, json: '"hello"' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) AS type FROM data',
      }))
      expect(result).toEqual([{ type: 'string' }])
    })

    it('should return number for a JSON number literal', async () => {
      const data = [{ id: 1, json: '42' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) AS type FROM data',
      }))
      expect(result).toEqual([{ type: 'number' }])
    })

    it('should return boolean for a JSON boolean literal', async () => {
      const data = [{ id: 1, json: 'true' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) AS type FROM data',
      }))
      expect(result).toEqual([{ type: 'boolean' }])
    })

    it('should return null for a JSON null literal', async () => {
      const data = [{ id: 1, json: 'null' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) AS type FROM data',
      }))
      expect(result).toEqual([{ type: 'null' }])
    })

    it('should return object for an object value', async () => {
      const data = [{ id: 1, json: { name: 'Alice' } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) AS type FROM data',
      }))
      expect(result).toEqual([{ type: 'object' }])
    })

    it('should return array for an array value', async () => {
      const data = [{ id: 1, json: [1, 2, 3] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) AS type FROM data',
      }))
      expect(result).toEqual([{ type: 'array' }])
    })

    it('should return null when input is SQL null', async () => {
      const data = [{ id: 1, json: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) AS type FROM data',
      }))
      expect(result).toEqual([{ type: null }])
    })

    it('should throw for an invalid JSON string', async () => {
      const data = [{ id: 1, json: 'not valid json' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) AS type FROM data',
      }))).rejects.toThrow('JSON_TYPE(value): invalid JSON string. Argument must be valid JSON. (row 1)')
    })

    it('should throw for wrong argument count', () => {
      const data = [{ id: 1 }]
      expect(() => executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE() FROM data',
      })).toThrow('JSON_TYPE(value) function requires 1 argument, got 0')
    })

    it('should work without alias', async () => {
      const data = [{ id: 1, json: '{}' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_TYPE(json) FROM data',
      }))
      expect(result[0]).toHaveProperty('json_type_json')
    })
  })

  describe('JSON_OBJECT', () => {
    it('should create an object with string key and value', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'name\', \'Alice\') AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { name: 'Alice' } }])
    })

    it('should create an object with multiple key-value pairs', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'name\', \'Alice\', \'age\', 30) AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { name: 'Alice', age: 30 } }])
    })

    it('should create an empty object with no arguments', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT() AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: {} }])
    })

    it('should support column references as values', async () => {
      const data = [{ id: 1, name: 'Bob', age: 25 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'name\', name, \'age\', age) AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { name: 'Bob', age: 25 } }])
    })

    it('should support column references as keys', async () => {
      const data = [{ id: 1, key: 'username', value: 'alice' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(key, value) AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { username: 'alice' } }])
    })

    it('should allow null values', async () => {
      const data = [{ id: 1, val: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'name\', val) AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { name: null } }])
    })

    it('should throw for null keys', async () => {
      const data = [{ id: 1, key: NULL }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(key, \'value\') AS obj FROM data',
      }))).rejects.toThrow('JSON_OBJECT(key1, value1[, ...]): key cannot be null')
    })

    it('should throw for odd number of arguments', async () => {
      const data = [{ id: 1 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'name\', \'Alice\', \'age\') AS obj FROM data',
      }))).rejects.toThrow('JSON_OBJECT(key1, value1[, ...]): requires an even number of arguments (key-value pairs)')
    })

    it('should convert numeric keys to strings', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(123, \'value\') AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { '123': 'value' } }])
    })

    it('should support boolean values', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'active\', 1 = 1) AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { active: true } }])
    })

    it('should support nested objects as values', async () => {
      const data = [{ id: 1, nested: { x: 1, y: 2 } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'outer\', nested) AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { outer: { x: 1, y: 2 } } }])
    })

    it('should support arrays as values', async () => {
      const data = [{ id: 1, items: [1, 2, 3] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'items\', items) AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { items: [1, 2, 3] } }])
    })

    it('should work with multiple rows', async () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'id\', id, \'name\', name) AS obj FROM data',
      }))
      expect(result).toEqual([
        { obj: { id: 1, name: 'Alice' } },
        { obj: { id: 2, name: 'Bob' } },
      ])
    })

    it('should work without alias', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'a\', 1) FROM data',
      }))
      expect(result[0]).toHaveProperty('json_object_a_1')
    })

    it('should work with expressions as values', async () => {
      const data = [{ id: 1, price: 100 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'doubled\', price * 2) AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { doubled: 200 } }])
    })

    it('should support nested JSON_OBJECT calls', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'user\', JSON_OBJECT(\'id\', 1, \'name\', \'Alice\')) AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { user: { id: 1, name: 'Alice' } } }])
    })

    it('should work with JSON_VALUE to extract and reconstruct', async () => {
      const data = [{ id: 1, json: { name: 'Alice', age: 30 } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT JSON_OBJECT(\'person\', JSON_VALUE(json, \'$.name\')) AS obj FROM data',
      }))
      expect(result).toEqual([{ obj: { person: 'Alice' } }])
    })
  })

  describe('JSON_EACH', () => {
    it('should iterate over object key/value pairs', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT * FROM JSON_EACH(\'{"a":1,"b":2}\')',
      }))
      expect(result).toEqual([
        { key: 'a', value: 1 },
        { key: 'b', value: 2 },
      ])
    })

    it('should iterate over array elements with integer keys', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT * FROM JSON_EACH(\'[10,20,30]\')',
      }))
      expect(result).toEqual([
        { key: 0, value: 10 },
        { key: 1, value: 20 },
        { key: 2, value: 30 },
      ])
    })

    it('should accept an object value directly', async () => {
      const data = [{ id: 1, json: { x: 'foo', y: 'bar' } }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT j.key, j.value FROM data JOIN JSON_EACH(data.json) AS j ON TRUE',
      }))
      expect(result).toEqual([
        { key: 'x', value: 'foo' },
        { key: 'y', value: 'bar' },
      ])
    })

    it('should accept an array value directly', async () => {
      const data = [{ id: 1, json: ['a', 'b'] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT j.key, j.value FROM data JOIN JSON_EACH(data.json) AS j ON TRUE',
      }))
      expect(result).toEqual([
        { key: 0, value: 'a' },
        { key: 1, value: 'b' },
      ])
    })

    it('should produce zero rows for NULL input', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT * FROM JSON_EACH(NULL)',
      }))
      expect(result).toEqual([])
    })

    it('should produce zero rows for an empty object', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT * FROM JSON_EACH(\'{}\')',
      }))
      expect(result).toEqual([])
    })

    it('should produce zero rows for an empty array', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT * FROM JSON_EACH(\'[]\')',
      }))
      expect(result).toEqual([])
    })

    it('should support column aliases like AS j(k, v)', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT k, v FROM JSON_EACH(\'{"a":1,"b":2}\') AS j(k, v)',
      }))
      expect(result).toEqual([
        { k: 'a', v: 1 },
        { k: 'b', v: 2 },
      ])
    })

    it('should support a single column alias for just key', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT k FROM JSON_EACH(\'{"a":1,"b":2}\') AS j(k)',
      }))
      expect(result).toEqual([{ k: 'a' }, { k: 'b' }])
    })

    it('should work with WHERE', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT key, value FROM JSON_EACH(\'[10,20,30,40]\') WHERE value > 15',
      }))
      expect(result).toEqual([
        { key: 1, value: 20 },
        { key: 2, value: 30 },
        { key: 3, value: 40 },
      ])
    })

    it('should work with aggregation', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT SUM(value) AS total FROM JSON_EACH(\'[1,2,3,4]\')',
      }))
      expect(result).toEqual([{ total: 10 }])
    })

    it('should iterate over nested object values without recursion', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT * FROM JSON_EACH(\'{"a":{"x":1},"b":[1,2]}\')',
      }))
      expect(result).toEqual([
        { key: 'a', value: { x: 1 } },
        { key: 'b', value: [1, 2] },
      ])
    })

    it('should throw for invalid JSON string', async () => {
      await expect(collect(executeSql({
        tables: {},
        query: 'SELECT * FROM JSON_EACH(\'not valid json\')',
      }))).rejects.toThrow('JSON_EACH(value): invalid JSON string')
    })

    it('should throw for non-object, non-array JSON', async () => {
      await expect(collect(executeSql({
        tables: {},
        query: 'SELECT * FROM JSON_EACH(\'42\')',
      }))).rejects.toThrow('JSON_EACH(value): argument must be a JSON object or array')
    })

    it('should throw for wrong argument count', () => {
      expect(() => executeSql({
        tables: {},
        query: 'SELECT * FROM JSON_EACH()',
      })).toThrow('JSON_EACH(value) function requires 1 argument, got 0')
    })

    it('should throw when used as a scalar expression', () => {
      expect(() => executeSql({
        tables: {},
        query: 'SELECT JSON_EACH(\'{}\')',
      })).toThrow('JSON_EACH is a table function and can only be used in FROM clauses at position 7')
    })

    it('should work with LATERAL join over multiple rows', async () => {
      const data = [
        { id: 1, json: { a: 1, b: 2 } },
        { id: 2, json: { c: 3 } },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT data.id, j.key, j.value FROM data JOIN JSON_EACH(data.json) AS j ON TRUE',
      }))
      expect(result).toEqual([
        { id: 1, key: 'a', value: 1 },
        { id: 1, key: 'b', value: 2 },
        { id: 2, key: 'c', value: 3 },
      ])
    })

    it('should throw for too many column aliases', () => {
      expect(() => executeSql({
        tables: {},
        query: 'SELECT * FROM JSON_EACH(\'{}\') AS j(a, b, c)',
      })).toThrow('JSON_EACH produces at most 2 columns (key, value); too many column aliases')
    })
  })

})
