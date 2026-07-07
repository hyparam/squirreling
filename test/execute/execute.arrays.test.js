import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/** @type {null} */
const NULL = null

describe('array functions', () => {
  describe('ARRAY_LENGTH', () => {
    it('should return the length of an array', async () => {
      const data = [
        { id: 1, items: [1, 2] },
        { id: 2, items: [10, 20, 30] },
        { id: 3, items: [] },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_LENGTH(items) AS len FROM data',
      }))
      expect(result).toEqual([{ len: 2 }, { len: 3 }, { len: 0 }])
    })

    it('should return null for null input', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_LENGTH(items) AS len FROM data',
      }))
      expect(result).toEqual([{ len: null }])
    })

    it('should return null for non-array input', async () => {
      const data = [{ id: 1, name: 'Alice' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_LENGTH(name) AS len FROM data',
      }))
      expect(result).toEqual([{ len: null }])
    })

    it('should throw for wrong argument count', () => {
      const data = [{ id: 1, items: [1, 2] }]
      expect(() => executeSql({
        tables: { data },
        query: 'SELECT ARRAY_LENGTH(items, 1, 2) FROM data',
      })).toThrow('ARRAY_LENGTH(array[, dimension]) function requires 1-2 arguments, got 3')
    })

    it('should return length along dimension 1', async () => {
      const data = [
        { id: 1, items: [1, 2, 3] },
        { id: 2, items: [] },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_LENGTH(items, 1) AS len FROM data',
      }))
      expect(result).toEqual([{ len: 3 }, { len: 0 }])
    })

    it('should return length along dimension 2 for nested arrays', async () => {
      const data = [{ id: 1, matrix: [[1, 2, 3], [4, 5, 6]] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_LENGTH(matrix, 2) AS len FROM data',
      }))
      expect(result).toEqual([{ len: 3 }])
    })

    it('should return null for dimension beyond array depth', async () => {
      const data = [{ id: 1, items: [1, 2, 3] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_LENGTH(items, 2) AS len FROM data',
      }))
      expect(result).toEqual([{ len: null }])
    })

    it('should return null for non-positive dimension', async () => {
      const data = [{ id: 1, items: [1, 2, 3] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_LENGTH(items, 0) AS len FROM data',
      }))
      expect(result).toEqual([{ len: null }])
    })
  })

  describe('ARRAY_POSITION', () => {
    it('should return the 1-based index of an element', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_POSITION(items, 20) AS pos FROM data',
      }))
      expect(result).toEqual([{ pos: 2 }])
    })

    it('should return null when element is not found', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_POSITION(items, 99) AS pos FROM data',
      }))
      expect(result).toEqual([{ pos: null }])
    })

    it('should return null for null array', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_POSITION(items, 1) AS pos FROM data',
      }))
      expect(result).toEqual([{ pos: null }])
    })

    it('should return null for non-array input', async () => {
      const data = [{ id: 1, name: 'Alice' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_POSITION(name, \'A\') AS pos FROM data',
      }))
      expect(result).toEqual([{ pos: null }])
    })

    it('should return the first occurrence', async () => {
      const data = [{ id: 1, items: [1, 2, 3, 2, 1] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_POSITION(items, 2) AS pos FROM data',
      }))
      expect(result).toEqual([{ pos: 2 }])
    })

    it('should find string elements', async () => {
      const data = [{ id: 1, items: ['a', 'b', 'c'] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_POSITION(items, \'b\') AS pos FROM data',
      }))
      expect(result).toEqual([{ pos: 2 }])
    })
  })

  describe('ARRAY_SORT', () => {
    it('should sort a numeric array', async () => {
      const data = [{ id: 1, items: [30, 10, 20] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_SORT(items) AS sorted FROM data',
      }))
      expect(result).toEqual([{ sorted: [10, 20, 30] }])
    })

    it('should sort a string array', async () => {
      const data = [{ id: 1, items: ['banana', 'apple', 'cherry'] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_SORT(items) AS sorted FROM data',
      }))
      expect(result).toEqual([{ sorted: ['apple', 'banana', 'cherry'] }])
    })

    it('should not mutate the original array', async () => {
      const items = [3, 1, 2]
      const data = [{ id: 1, items }]
      await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_SORT(items) AS sorted FROM data',
      }))
      expect(items).toEqual([3, 1, 2])
    })

    it('should return null for null input', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_SORT(items) AS sorted FROM data',
      }))
      expect(result).toEqual([{ sorted: null }])
    })

    it('should return null for non-array input', async () => {
      const data = [{ id: 1, name: 'Alice' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_SORT(name) AS sorted FROM data',
      }))
      expect(result).toEqual([{ sorted: null }])
    })

    it('should sort nulls last', async () => {
      const data = [{ id: 1, items: [3, NULL, 1, NULL, 2] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_SORT(items) AS sorted FROM data',
      }))
      expect(result).toEqual([{ sorted: [1, 2, 3, null, null] }])
    })
  })

  describe('CARDINALITY', () => {
    it('should return the length of an array', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT CARDINALITY(items) AS len FROM data',
      }))
      expect(result).toEqual([{ len: 3 }])
    })

    it('should return null for null input', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT CARDINALITY(items) AS len FROM data',
      }))
      expect(result).toEqual([{ len: null }])
    })
  })

  describe('SIZE', () => {
    it('should return the length of an array', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SIZE(items) AS len FROM data',
      }))
      expect(result).toEqual([{ len: 3 }])
    })

    it('should return null for null input', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SIZE(items) AS len FROM data',
      }))
      expect(result).toEqual([{ len: null }])
    })

    it('should throw for wrong argument count', () => {
      const data = [{ id: 1, items: [1, 2] }]
      expect(() => executeSql({
        tables: { data },
        query: 'SELECT SIZE(items, 1) FROM data',
      })).toThrow('SIZE(array) function requires 1 argument, got 2')
    })
  })

  describe('ARRAY_CONTAINS', () => {
    it('should return true when element is in the array', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONTAINS(items, 20) AS found FROM data',
      }))
      expect(result).toEqual([{ found: true }])
    })

    it('should return false when element is not in the array', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONTAINS(items, 99) AS found FROM data',
      }))
      expect(result).toEqual([{ found: false }])
    })

    it('should find string elements', async () => {
      const data = [{ id: 1, items: ['a', 'b', 'c'] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONTAINS(items, \'b\') AS found FROM data',
      }))
      expect(result).toEqual([{ found: true }])
    })

    it('should return false for empty array', async () => {
      /** @type {{ id: number, items: number[] }[]} */
      const data = [{ id: 1, items: [] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONTAINS(items, 1) AS found FROM data',
      }))
      expect(result).toEqual([{ found: false }])
    })

    it('should return null for null array', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONTAINS(items, 1) AS found FROM data',
      }))
      expect(result).toEqual([{ found: null }])
    })

    it('should return null for non-array input', async () => {
      const data = [{ id: 1, name: 'Alice' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONTAINS(name, \'A\') AS found FROM data',
      }))
      expect(result).toEqual([{ found: null }])
    })

    it('should throw for wrong argument count', () => {
      const data = [{ id: 1, items: [1, 2] }]
      expect(() => executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONTAINS(items) FROM data',
      })).toThrow('ARRAY_CONTAINS(array, element) function requires 2 arguments, got 1')
    })
  })

  describe('LIST_CONTAINS', () => {
    it('should behave like ARRAY_CONTAINS', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LIST_CONTAINS(items, 20) AS found FROM data',
      }))
      expect(result).toEqual([{ found: true }])
    })

    it('should return false when element is not in the array', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LIST_CONTAINS(items, 99) AS found FROM data',
      }))
      expect(result).toEqual([{ found: false }])
    })

    it('should return null for null array', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LIST_CONTAINS(items, 1) AS found FROM data',
      }))
      expect(result).toEqual([{ found: null }])
    })
  })

  describe('LIST_POSITION', () => {
    it('should behave like ARRAY_POSITION', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LIST_POSITION(items, 20) AS pos FROM data',
      }))
      expect(result).toEqual([{ pos: 2 }])
    })

    it('should return null when element is not found', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LIST_POSITION(items, 99) AS pos FROM data',
      }))
      expect(result).toEqual([{ pos: null }])
    })

    it('should return null for null array', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LIST_POSITION(items, 1) AS pos FROM data',
      }))
      expect(result).toEqual([{ pos: null }])
    })
  })

  describe('LIST_LENGTH', () => {
    it('should behave like ARRAY_LENGTH', async () => {
      const data = [
        { id: 1, items: [1, 2] },
        { id: 2, items: [10, 20, 30] },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LIST_LENGTH(items) AS len FROM data',
      }))
      expect(result).toEqual([{ len: 2 }, { len: 3 }])
    })

    it('should return null for null input', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LIST_LENGTH(items) AS len FROM data',
      }))
      expect(result).toEqual([{ len: null }])
    })
  })

  describe('LEN', () => {
    it('should return the length of an array', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LEN(items) AS len FROM data',
      }))
      expect(result).toEqual([{ len: 3 }])
    })

    it('should return null for null input', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LEN(items) AS len FROM data',
      }))
      expect(result).toEqual([{ len: null }])
    })
  })

  describe('ARRAY_APPEND', () => {
    it('should append an element to the end of the array', async () => {
      const data = [{ id: 1, items: [1, 2, 3] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_APPEND(items, 4) AS appended FROM data',
      }))
      expect(result).toEqual([{ appended: [1, 2, 3, 4] }])
    })

    it('should append to an empty array', async () => {
      /** @type {{ id: number, items: number[] }[]} */
      const data = [{ id: 1, items: [] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_APPEND(items, 1) AS appended FROM data',
      }))
      expect(result).toEqual([{ appended: [1] }])
    })

    it('should not mutate the original array', async () => {
      const items = [1, 2, 3]
      const data = [{ id: 1, items }]
      await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_APPEND(items, 4) AS appended FROM data',
      }))
      expect(items).toEqual([1, 2, 3])
    })

    it('should append null', async () => {
      const data = [{ id: 1, items: [1, 2] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_APPEND(items, NULL) AS appended FROM data',
      }))
      expect(result).toEqual([{ appended: [1, 2, null] }])
    })

    it('should return null for null array', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_APPEND(items, 1) AS appended FROM data',
      }))
      expect(result).toEqual([{ appended: null }])
    })

    it('should return null for non-array input', async () => {
      const data = [{ id: 1, name: 'Alice' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_APPEND(name, \'B\') AS appended FROM data',
      }))
      expect(result).toEqual([{ appended: null }])
    })

    it('should throw for wrong argument count', () => {
      const data = [{ id: 1, items: [1, 2] }]
      expect(() => executeSql({
        tables: { data },
        query: 'SELECT ARRAY_APPEND(items) FROM data',
      })).toThrow('ARRAY_APPEND(array, element) function requires 2 arguments, got 1')
    })
  })

  describe('LIST_APPEND', () => {
    it('should behave like ARRAY_APPEND', async () => {
      const data = [{ id: 1, items: [1, 2, 3] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LIST_APPEND(items, 4) AS appended FROM data',
      }))
      expect(result).toEqual([{ appended: [1, 2, 3, 4] }])
    })
  })

  describe('ARRAY_CONCAT', () => {
    it('should concatenate two arrays', async () => {
      const data = [{ id: 1, a: [1, 2], b: [3, 4] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONCAT(a, b) AS combined FROM data',
      }))
      expect(result).toEqual([{ combined: [1, 2, 3, 4] }])
    })

    it('should concatenate with empty arrays', async () => {
      /** @type {{ id: number, a: number[], b: number[] }[]} */
      const data = [{ id: 1, a: [], b: [1, 2] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONCAT(a, b) AS combined FROM data',
      }))
      expect(result).toEqual([{ combined: [1, 2] }])
    })

    it('should not mutate the original arrays', async () => {
      const a = [1, 2]
      const b = [3, 4]
      const data = [{ id: 1, a, b }]
      await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONCAT(a, b) AS combined FROM data',
      }))
      expect(a).toEqual([1, 2])
      expect(b).toEqual([3, 4])
    })

    it('should return null for null first array', async () => {
      const data = [{ id: 1, a: NULL, b: [1, 2] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONCAT(a, b) AS combined FROM data',
      }))
      expect(result).toEqual([{ combined: null }])
    })

    it('should return null for null second array', async () => {
      const data = [{ id: 1, a: [1, 2], b: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONCAT(a, b) AS combined FROM data',
      }))
      expect(result).toEqual([{ combined: null }])
    })

    it('should return null for non-array input', async () => {
      const data = [{ id: 1, a: 'hello', b: [1, 2] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONCAT(a, b) AS combined FROM data',
      }))
      expect(result).toEqual([{ combined: null }])
    })

    it('should throw for wrong argument count', () => {
      const data = [{ id: 1, items: [1, 2] }]
      expect(() => executeSql({
        tables: { data },
        query: 'SELECT ARRAY_CONCAT(items) FROM data',
      })).toThrow('ARRAY_CONCAT(array1, array2) function requires 2 arguments, got 1')
    })
  })

  describe('LIST_CONCAT', () => {
    it('should behave like ARRAY_CONCAT', async () => {
      const data = [{ id: 1, a: [1, 2], b: [3, 4] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LIST_CONCAT(a, b) AS combined FROM data',
      }))
      expect(result).toEqual([{ combined: [1, 2, 3, 4] }])
    })
  })

  describe('array subscript', () => {
    it('should index an array column with a numeric subscript', async () => {
      const data = [
        { id: 1, items: [10, 20, 30] },
        { id: 2, items: [40] },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT items[0] AS first FROM data',
      }))
      expect(result).toEqual([{ first: 10 }, { first: 40 }])
    })

    it('should generate a default alias for a numeric subscript', async () => {
      const data = [{ id: 1, items: [10, 20] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT items[1] FROM data',
      }))
      expect(result).toEqual([{ 'items[1]': 20 }])
    })

    it('should return null for an out-of-range index', async () => {
      const data = [{ id: 1, items: [10, 20] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT items[5] AS missing FROM data',
      }))
      expect(result).toEqual([{ missing: null }])
    })

    it('should return null for a null array', async () => {
      const data = [{ id: 1, items: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT items[0] AS first FROM data',
      }))
      expect(result).toEqual([{ first: null }])
    })

    it('should return null for numeric subscript on a non-array', async () => {
      const data = [{ id: 1, name: 'Alice' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT name[0] AS first FROM data',
      }))
      expect(result).toEqual([{ first: null }])
    })

    it('should support an expression as the index', async () => {
      const data = [{ id: 1, items: [10, 20, 30] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT items[1 + 1] AS third FROM data',
      }))
      expect(result).toEqual([{ third: 30 }])
    })

    it('should support a column as the index', async () => {
      const data = [
        { idx: 0, items: [10, 20, 30] },
        { idx: 2, items: [10, 20, 30] },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT items[idx] AS value FROM data',
      }))
      expect(result).toEqual([{ value: 10 }, { value: 30 }])
    })

    it('should access struct fields of an indexed element with dot notation', async () => {
      const data = [
        { id: 1, tools: [{ name: 'web_search', calls: 3 }, { name: 'calculator', calls: 1 }] },
        { id: 2, tools: [{ name: 'code', calls: 7 }] },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT tools[0].name FROM data',
      }))
      expect(result).toEqual([{ name: 'web_search' }, { name: 'code' }])
    })

    it('should access struct fields of an indexed element with string subscript', async () => {
      const data = [{ id: 1, tools: [{ name: 'web_search' }] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT tools[0][\'name\'] AS tool FROM data',
      }))
      expect(result).toEqual([{ tool: 'web_search' }])
    })

    it('should return null for a missing struct field', async () => {
      const data = [{ id: 1, tools: [{ name: 'web_search' }] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT tools[0].missing AS field FROM data',
      }))
      expect(result).toEqual([{ field: null }])
    })

    it('should support chained numeric subscripts on nested arrays', async () => {
      const data = [{ id: 1, matrix: [[1, 2], [3, 4]] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT matrix[1][0] AS cell FROM data',
      }))
      expect(result).toEqual([{ cell: 3 }])
    })

    it('should index a table-qualified column', async () => {
      const data = [{ id: 1, items: [10, 20] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT data.items[1] AS second FROM data',
      }))
      expect(result).toEqual([{ second: 20 }])
    })

    it('should support subscripts in the WHERE clause', async () => {
      const data = [
        { id: 1, items: [10, 20] },
        { id: 2, items: [30, 40] },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT id FROM data WHERE items[0] = 30',
      }))
      expect(result).toEqual([{ id: 2 }])
    })

    it('should support subscripts in GROUP BY', async () => {
      const data = [
        { id: 1, items: ['a', 'x'] },
        { id: 2, items: ['a', 'y'] },
        { id: 3, items: ['b', 'z'] },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT items[0] AS key, COUNT(*) AS cnt FROM data GROUP BY items[0] ORDER BY key',
      }))
      expect(result).toEqual([{ key: 'a', cnt: 2 }, { key: 'b', cnt: 1 }])
    })

    it('should index an array literal', async () => {
      const result = await collect(executeSql({
        tables: {},
        query: 'SELECT [10, 20, 30][2] AS third',
      }))
      expect(result).toEqual([{ third: 30 }])
    })

    it('should index the result of a function call', async () => {
      const data = [{ id: 1, csv: 'a,b,c' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT STRING_SPLIT(csv, \',\')[1] AS second FROM data',
      }))
      expect(result).toEqual([{ second: 'b' }])
    })

    it('should return null for a negative index', async () => {
      const data = [{ id: 1, items: [10, 20] }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT items[-1] AS last FROM data',
      }))
      expect(result).toEqual([{ last: null }])
    })

    it('should return null for a null index', async () => {
      const data = [{ id: 1, items: [10, 20], idx: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT items[idx] AS value FROM data',
      }))
      expect(result).toEqual([{ value: null }])
    })
  })
})
