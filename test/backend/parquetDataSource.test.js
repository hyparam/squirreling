import { asyncBufferFromFile, parquetMetadataAsync } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { describe, expect, it } from 'vitest'
import { parquetDataSource } from '../../src/backend/parquetDataSource.js'
import { executeSql } from '../../src/execute/execute.js'
import { collect } from '../../src/index.js'
import { countingBuffer } from '../helpers.js'

describe('parquet backend', async () => {
  const file = await asyncBufferFromFile('test/files/users.parquet')
  const metadata = await parquetMetadataAsync(file)
  const users = parquetDataSource(file, metadata, compressors)

  it('should read all columns from parquet file', async () => {
    const counting = countingBuffer(file)
    const users = parquetDataSource(counting, metadata, compressors)
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT * FROM users',
    }))
    expect(result).toEqual([
      { id: 1, name: 'Alice', age: 25, city: 'New York' },
      { id: 2, name: 'Bob', age: 30, city: 'San Francisco' },
      { id: 3, name: 'Charlie', age: 35, city: 'Los Angeles' },
      { id: 4, name: 'Diana', age: 28, city: 'Chicago' },
      { id: 5, name: 'Eve', age: 42, city: 'Boston' },
    ])
    expect(counting.fetches).toBe(1)
    expect(counting.bytes).toBe(250)
  })

  it('should support column projection - single column', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT name FROM users',
    }))
    expect(result).toEqual([
      { name: 'Alice' },
      { name: 'Bob' },
      { name: 'Charlie' },
      { name: 'Diana' },
      { name: 'Eve' },
    ])
  })

  it('should support column projection - multiple columns', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT id, name FROM users',
    }))
    expect(result).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Charlie' },
      { id: 4, name: 'Diana' },
      { id: 5, name: 'Eve' },
    ])
  })

  it('should support WHERE clause with parquet', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT * FROM users WHERE age > 30',
    }))
    expect(result).toEqual([
      { id: 3, name: 'Charlie', age: 35, city: 'Los Angeles' },
      { id: 5, name: 'Eve', age: 42, city: 'Boston' },
    ])
  })

  it('should support ORDER BY with parquet', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT name, age FROM users ORDER BY age DESC',
    }))
    expect(result).toEqual([
      { name: 'Eve', age: 42 },
      { name: 'Charlie', age: 35 },
      { name: 'Bob', age: 30 },
      { name: 'Diana', age: 28 },
      { name: 'Alice', age: 25 },
    ])
  })

  it('should support LIMIT with parquet', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT * FROM users LIMIT 2',
    }))
    expect(result).toHaveLength(2)
    expect(result).toEqual([
      { id: 1, name: 'Alice', age: 25, city: 'New York' },
      { id: 2, name: 'Bob', age: 30, city: 'San Francisco' },
    ])
  })

  it('should support aggregates with parquet', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT COUNT(*) as count, AVG(age) as avg_age FROM users',
    }))
    expect(result).toEqual([
      { count: 5, avg_age: 32 },
    ])
  })

  it('should support GROUP BY with parquet', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) as count FROM users GROUP BY city ORDER BY count DESC',
    }))
    expect(result).toHaveLength(5)
    expect(result[0]).toEqual({ city: expect.any(String), count: 1 })
  })

  it('should support string functions with parquet', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT UPPER(name) as upper_name FROM users LIMIT 2',
    }))
    expect(result).toEqual([
      { upper_name: 'ALICE' },
      { upper_name: 'BOB' },
    ])
  })

  it('should prune row groups with WHERE equality filter', async () => {
    // alpha.parquet has multiple row groups with alphabetically sorted data (aa, ab, ... zz)
    const alphaFile = await asyncBufferFromFile('test/files/alpha.parquet')
    const alphaMetadata = await parquetMetadataAsync(alphaFile)
    const counting = countingBuffer(alphaFile)
    const alpha = parquetDataSource(counting, alphaMetadata, compressors)

    const result = await collect(executeSql({
      tables: { alpha },
      query: 'SELECT id FROM alpha WHERE id = \'kk\'',
    }))

    expect(result).toEqual([{ id: 'kk' }])
    // With row group pruning, should only read 1 row group
    expect(counting.fetches).toBe(1)
    expect(counting.bytes).toBe(437)
  })
})
