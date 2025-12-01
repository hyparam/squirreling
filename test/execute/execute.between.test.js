import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('executeSql - BETWEEN', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
  ]

  describe('basic BETWEEN queries', () => {
    it('should filter with BETWEEN using numbers', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age BETWEEN 28 AND 32' }))
      expect(result).toHaveLength(3)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Diana', 'Eve'])
    })

    it('should include boundary values', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age BETWEEN 25 AND 30' }))
      expect(result).toHaveLength(4)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Diana', 'Eve'])
    })

    it('should filter with NOT BETWEEN', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age NOT BETWEEN 28 AND 32' }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Bob', 'Charlie'])
    })

    it('should handle BETWEEN with equal bounds', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age BETWEEN 30 AND 30' }))
      expect(result).toHaveLength(2)
      expect(result.every(r => r.age === 30)).toBe(true)
    })
  })

  describe('BETWEEN with strings', () => {
    it('should filter with BETWEEN using strings', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE name BETWEEN \'B\' AND \'D\'' }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Bob', 'Charlie'])
    })

    it('should handle string boundaries correctly', async () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
        { id: 4, name: 'Diana' },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data WHERE name BETWEEN \'Alice\' AND \'Charlie\'' }))
      expect(result).toHaveLength(3)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Charlie'])
    })
  })

  describe('BETWEEN in complex expressions', () => {
    it('should filter with BETWEEN and AND', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age BETWEEN 25 AND 30 AND city = \'NYC\'' }))
      expect(result).toHaveLength(2)
      // @ts-expect-error null equality
      expect(result.every(r => r.city === 'NYC' && r.age >= 25 && r.age <= 30)).toBe(true)
    })

    it('should filter with BETWEEN and OR', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE age BETWEEN 25 AND 28 OR age = 35' }))
      expect(result).toHaveLength(3)
      expect(result.map(r => r.name).sort()).toEqual(['Bob', 'Charlie', 'Diana'])
    })

    it('should handle multiple BETWEEN clauses', async () => {
      const data = [
        { id: 1, age: 25, score: 80 },
        { id: 2, age: 30, score: 90 },
        { id: 3, age: 35, score: 70 },
        { id: 4, age: 28, score: 85 },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data WHERE age BETWEEN 25 AND 30 AND score BETWEEN 80 AND 90' }))
      expect(result).toHaveLength(3)
      expect(result.map(r => r.id).sort()).toEqual([1, 2, 4])
    })

    it('should handle BETWEEN with NOT', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users WHERE NOT (age BETWEEN 28 AND 32)' }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Bob', 'Charlie'])
    })
  })

  describe('edge cases', () => {
    it('should handle BETWEEN with null values', async () => {
      const data = [
        { id: 1, value: 5 },
        { id: 2, value: 10 },
        { id: 3, value: null },
        { id: 4, value: 15 },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data WHERE value BETWEEN 8 AND 12' }))
      expect(result).toHaveLength(1)
      expect(result[0].id).toBe(2)
    })

    it('should handle BETWEEN with undefined values', async () => {
      const data = [
        { id: 1, value: 5 },
        { id: 2, value: 10 },
        { id: 3 },
        { id: 4, value: 15 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT * FROM data WHERE value BETWEEN 8 AND 12',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].id).toBe(2)
    })

    it('should handle BETWEEN with inverted bounds', async () => {
      // BETWEEN with lower > upper should return no results
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM users WHERE age BETWEEN 35 AND 25',
      }))
      expect(result).toHaveLength(0)
    })

    it('should handle BETWEEN with negative numbers', async () => {
      const data = [
        { id: 1, value: -10 },
        { id: 2, value: -5 },
        { id: 3, value: 0 },
        { id: 4, value: 5 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT * FROM data WHERE value BETWEEN -8 AND 2',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.id).sort()).toEqual([2, 3])
    })

    it('should handle BETWEEN with floats', async () => {
      const data = [
        { id: 1, price: 9.99 },
        { id: 2, price: 15.50 },
        { id: 3, price: 20.00 },
        { id: 4, price: 25.99 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT * FROM data WHERE price BETWEEN 10.00 AND 20.00',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.id).sort()).toEqual([2, 3])
    })
  })

  describe('BETWEEN with SELECT columns', () => {
    it('should work with BETWEEN and column selection', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, age FROM users WHERE age BETWEEN 28 AND 32',
      }))
      expect(result).toHaveLength(3)
      expect(result[0]).toHaveProperty('name')
      expect(result[0]).toHaveProperty('age')
      expect(result[0]).not.toHaveProperty('city')
    })

    it('should work with BETWEEN and ORDER BY', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM users WHERE age BETWEEN 25 AND 32 ORDER BY age ASC',
      }))
      expect(result).toHaveLength(4)
      expect(result[0].age).toBe(25)
      expect(result[result.length - 1].age).toBe(30)
    })

    it('should work with BETWEEN and LIMIT', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age LIMIT 2',
      }))
      expect(result).toHaveLength(2)
      expect(result[0].age).toBe(25)
      expect(result[1].age).toBe(28)
    })
  })
})
