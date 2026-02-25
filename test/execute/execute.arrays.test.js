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

    it('should throw for wrong argument count', async () => {
      const data = [{ id: 1, items: [1, 2] }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ARRAY_LENGTH(items, 1) FROM data',
      }))).rejects.toThrow()
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
})
