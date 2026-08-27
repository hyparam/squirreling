import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('SHOW TABLES', () => {
  const users = [{ id: 1, name: 'Alice' }]
  const orders = [{ id: 1, total: 10 }]

  it('should list table names sorted alphabetically', async () => {
    const result = await collect(executeSql({
      tables: { users, orders },
      query: 'SHOW TABLES',
    }))
    expect(result).toEqual([
      { name: 'orders' },
      { name: 'users' },
    ])
  })

  it('should be case-insensitive', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'show tables',
    }))
    expect(result).toEqual([{ name: 'users' }])
  })

  it('should list empty tables', async () => {
    const results = executeSql({
      tables: { users: [], orders },
      query: 'SHOW TABLES',
    })
    expect(results.columns).toEqual(['name'])
    expect(results.numRows).toEqual(2)
    expect(results.maxRows).toEqual(2)
    expect(await collect(results)).toEqual([
      { name: 'orders' },
      { name: 'users' },
    ])
  })

  it('should allow a trailing semicolon', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SHOW TABLES;',
    }))
    expect(result).toEqual([{ name: 'users' }])
  })

  it('should return no rows when there are no tables', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SHOW TABLES',
    }))
    expect(result).toEqual([])
  })

  it('should throw when SHOW is not followed by TABLES', () => {
    expect(() => executeSql({ tables: { users }, query: 'SHOW COLUMNS' }))
      .toThrow('Expected TABLES after "SHOW" but found "COLUMNS" at position 5')
  })

  it('should throw when SHOW ends the query', () => {
    expect(() => executeSql({ tables: { users }, query: 'SHOW' }))
      .toThrow('Expected TABLES after "SHOW" but found end of query at position 4')
  })

  it('should throw on trailing tokens after SHOW TABLES', () => {
    expect(() => executeSql({ tables: { users }, query: 'SHOW TABLES users' }))
      .toThrow('Expected end of query after "TABLES" but found "users" at position 12')
  })
})
