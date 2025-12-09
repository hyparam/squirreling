import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('POSITIONAL JOIN', () => {
  const tableA = [
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
    { id: 3, name: 'Charlie' },
  ]

  const tableB = [
    { code: 'A', value: 100 },
    { code: 'B', value: 200 },
    { code: 'C', value: 300 },
  ]

  // Tables with null values for testing NULL padding
  /** @type {typeof tableA} */
  const nullTableA = [
    { id: null, name: null },
    { id: null, name: null },
    { id: null, name: null },
  ]
  /** @type {typeof tableB} */
  const nullTableB = [
    { code: null, value: null },
    { code: null, value: null },
    { code: null, value: null },
  ]

  it('should join tables by row position with equal lengths', async () => {
    const result = await collect(executeSql({
      tables: { tableA, tableB },
      query: 'SELECT tableA.name, tableB.code FROM tableA POSITIONAL JOIN tableB',
    }))
    expect(result).toHaveLength(3)
    expect(result[0]).toEqual({ name: 'Alice', code: 'A' })
    expect(result[1]).toEqual({ name: 'Bob', code: 'B' })
    expect(result[2]).toEqual({ name: 'Charlie', code: 'C' })
  })

  it('should pad right table with NULLs when left is longer', async () => {
    const shortB = [
      { code: 'A', value: 100 },
    ]
    const result = await collect(executeSql({
      tables: { tableA, tableB: shortB },
      query: 'SELECT tableA.name, tableB.code FROM tableA POSITIONAL JOIN tableB',
    }))
    expect(result).toHaveLength(3)
    expect(result[0]).toEqual({ name: 'Alice', code: 'A' })
    expect(result[1]).toEqual({ name: 'Bob', code: null })
    expect(result[2]).toEqual({ name: 'Charlie', code: null })
  })

  it('should pad left table with NULLs when right is longer', async () => {
    const shortA = [
      { id: 1, name: 'Alice' },
    ]
    const result = await collect(executeSql({
      tables: { tableA: shortA, tableB },
      query: 'SELECT tableA.name, tableB.code FROM tableA POSITIONAL JOIN tableB',
    }))
    expect(result).toHaveLength(3)
    expect(result[0]).toEqual({ name: 'Alice', code: 'A' })
    expect(result[1]).toEqual({ name: null, code: 'B' })
    expect(result[2]).toEqual({ name: null, code: 'C' })
  })

  it('should return empty result when both tables are empty', async () => {
    const result = await collect(executeSql({
      tables: { tableA: [], tableB: [] },
      query: 'SELECT * FROM tableA POSITIONAL JOIN tableB',
    }))
    expect(result).toHaveLength(0)
  })

  it('should return right rows with NULL left columns when left has all nulls', async () => {
    const result = await collect(executeSql({
      tables: { tableA: nullTableA, tableB },
      query: 'SELECT tableA.name, tableB.code FROM tableA POSITIONAL JOIN tableB',
    }))
    expect(result).toHaveLength(3)
    expect(result.every(r => r.name === null)).toBe(true)
    expect(result.map(r => r.code)).toEqual(['A', 'B', 'C'])
  })

  it('should return left rows with NULL right columns when right has all nulls', async () => {
    const result = await collect(executeSql({
      tables: { tableA, tableB: nullTableB },
      query: 'SELECT tableA.name, tableB.code FROM tableA POSITIONAL JOIN tableB',
    }))
    expect(result).toHaveLength(3)
    expect(result.map(r => r.name)).toEqual(['Alice', 'Bob', 'Charlie'])
    expect(result.every(r => r.code === null)).toBe(true)
  })

  it('should throw error for non-existent column in POSITIONAL JOIN', async () => {
    await expect(collect(executeSql({
      tables: { tableA, tableB },
      query: 'SELECT tableA.nonexistent FROM tableA POSITIONAL JOIN tableB',
    }))).rejects.toThrow('Column "tableA.nonexistent" not found')
  })

  it('should work with table aliases', async () => {
    const result = await collect(executeSql({
      tables: { tableA, tableB },
      query: 'SELECT a.name, b.code FROM tableA a POSITIONAL JOIN tableB b',
    }))
    expect(result).toHaveLength(3)
    expect(result[0]).toEqual({ name: 'Alice', code: 'A' })
  })

  it('should work with WHERE clause filtering', async () => {
    const result = await collect(executeSql({
      tables: { tableA, tableB },
      query: 'SELECT tableA.name, tableB.value FROM tableA POSITIONAL JOIN tableB WHERE tableB.value > 150',
    }))
    expect(result).toHaveLength(2)
    expect(result.map(r => r.name)).toEqual(['Bob', 'Charlie'])
  })

  it('should work with SELECT *', async () => {
    const result = await collect(executeSql({
      tables: { tableA, tableB },
      query: 'SELECT * FROM tableA POSITIONAL JOIN tableB',
    }))
    expect(result).toHaveLength(3)
    expect(result[0]).toHaveProperty('id', 1)
    expect(result[0]).toHaveProperty('name', 'Alice')
    expect(result[0]).toHaveProperty('code', 'A')
    expect(result[0]).toHaveProperty('value', 100)
  })

  it('should work with ORDER BY', async () => {
    const result = await collect(executeSql({
      tables: { tableA, tableB },
      query: 'SELECT tableA.name, tableB.code FROM tableA POSITIONAL JOIN tableB ORDER BY tableB.code DESC',
    }))
    expect(result).toHaveLength(3)
    expect(result[0].code).toBe('C')
    expect(result[2].code).toBe('A')
  })

  it('should work with LIMIT', async () => {
    const result = await collect(executeSql({
      tables: { tableA, tableB },
      query: 'SELECT tableA.name, tableB.code FROM tableA POSITIONAL JOIN tableB LIMIT 2',
    }))
    expect(result).toHaveLength(2)
    expect(result[0]).toEqual({ name: 'Alice', code: 'A' })
    expect(result[1]).toEqual({ name: 'Bob', code: 'B' })
  })
})
