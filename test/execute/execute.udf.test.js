import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('user-defined functions', () => {
  const users = [
    { id: 1, name: 'Alice', score: 10 },
    { id: 2, name: 'Bob', score: 20 },
    { id: 3, name: 'Charlie', score: 30 },
  ]

  describe('basic UDF', () => {
    it('should call a sync user-defined function', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT DOUBLE(score) AS doubled FROM users',
        functions: {
          DOUBLE: x => Number(x) * 2,
        },
      }))
      expect(result).toEqual([
        { doubled: 20 },
        { doubled: 40 },
        { doubled: 60 },
      ])
    })

    it('should call an async user-defined function', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT ASYNC_DOUBLE(score) AS doubled FROM users',
        functions: {
          ASYNC_DOUBLE: async x => {
            await new Promise(resolve => setTimeout(resolve, 1))
            return Number(x) * 2
          },
        },
      }))
      expect(result).toEqual([
        { doubled: 20 },
        { doubled: 40 },
        { doubled: 60 },
      ])
    })
  })

  describe('UDF with multiple arguments', () => {
    it('should pass multiple arguments to UDF', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT ADD(score, id) AS sum FROM users',
        functions: {
          ADD: (a, b) => Number(a) + Number(b),
        },
      }))
      expect(result).toEqual([
        { sum: 11 },
        { sum: 22 },
        { sum: 33 },
      ])
    })
  })

  describe('UDF returning null', () => {
    it('should handle UDF returning null', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT NULLIFY(score) AS nulled FROM users',
        functions: {
          NULLIFY: () => null,
        },
      }))
      expect(result).toEqual([
        { nulled: null },
        { nulled: null },
        { nulled: null },
      ])
    })
  })

  describe('UDF case-insensitivity', () => {
    it('should find UDF regardless of case in SQL', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT double(score) AS doubled FROM users WHERE id = 1',
        functions: {
          DOUBLE: x => Number(x) * 2,
        },
      }))
      expect(result).toEqual([{ doubled: 20 }])
    })

    it('should find UDF regardless of case in registration', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT DOUBLE(score) AS doubled FROM users WHERE id = 1',
        functions: {
          double: x => Number(x) * 2,
        },
      }))
      expect(result).toEqual([{ doubled: 20 }])
    })
  })

  describe('UDF in WHERE clause', () => {
    it('should use UDF in WHERE clause', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name FROM users WHERE IS_EVEN(score)',
        functions: {
          IS_EVEN: x => Number(x) % 2 === 0,
        },
      }))
      expect(result).toEqual([
        { name: 'Alice' },
        { name: 'Bob' },
        { name: 'Charlie' },
      ])
    })
  })

  describe('unknown function error', () => {
    it('should throw error for unknown function when no UDFs provided', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT UNKNOWN_FUNC(score) FROM users',
      }))).rejects.toThrow(/unknown function/i)
    })

    it('should throw error for unknown function when UDFs provided but not matching', async () => {
      await expect(collect(executeSql({
        tables: { users },
        query: 'SELECT UNKNOWN_FUNC(score) FROM users',
        functions: {
          OTHER_FUNC: x => x,
        },
      }))).rejects.toThrow(/unknown function/i)
    })
  })

  describe('UDF with expressions', () => {
    it('should work with UDF in complex expressions', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, DOUBLE(score) + 5 AS calc FROM users WHERE id = 1',
        functions: {
          DOUBLE: x => Number(x) * 2,
        },
      }))
      expect(result).toEqual([{ name: 'Alice', calc: 25 }])
    })

    it('should work with nested UDF calls', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT DOUBLE(DOUBLE(score)) AS quadrupled FROM users WHERE id = 1',
        functions: {
          DOUBLE: x => Number(x) * 2,
        },
      }))
      expect(result).toEqual([{ quadrupled: 40 }])
    })
  })
})
