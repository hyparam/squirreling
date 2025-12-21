import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql - JOIN queries', () => {
  it('should parse simple INNER JOIN', () => {
    const select = parseSql({ query: 'SELECT * FROM users JOIN orders ON users.id = orders.user_id' })
    expect(select.from).toEqual({ kind: 'table', table: 'users' })
    expect(select.joins).toEqual([
      {
        joinType: 'INNER',
        table: 'orders',
        alias: undefined,
        on: {
          type: 'binary',
          op: '=',
          left: {
            type: 'identifier',
            name: 'users.id',
            positionStart: 35,
            positionEnd: 43,
          },
          right: {
            type: 'identifier',
            name: 'orders.user_id',
            positionStart: 46,
            positionEnd: 60,
          },
          positionStart: 35,
          positionEnd: 60,
        },
      },
    ])
  })

  it('should parse explicit INNER JOIN', () => {
    const select = parseSql({ query: 'SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id' })
    expect(select.joins[0].joinType).toBe('INNER')
  })

  it('should parse LEFT JOIN', () => {
    const select = parseSql({ query: 'SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id' })
    expect(select.joins).toEqual([
      {
        joinType: 'LEFT',
        table: 'orders',
        alias: undefined,
        on: {
          type: 'binary',
          op: '=',
          left: {
            type: 'identifier',
            name: 'users.id',
            positionStart: 40,
            positionEnd: 48,
          },
          right: {
            type: 'identifier',
            name: 'orders.user_id',
            positionStart: 51,
            positionEnd: 65,
          },
          positionStart: 40,
          positionEnd: 65,
        },
      },
    ])
  })

  it('should parse LEFT OUTER JOIN', () => {
    const select = parseSql({ query: 'SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id' })
    expect(select.joins[0].joinType).toBe('LEFT')
  })

  it('should parse RIGHT JOIN', () => {
    const select = parseSql({ query: 'SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id' })
    expect(select.joins[0].joinType).toBe('RIGHT')
  })

  it('should parse FULL JOIN', () => {
    const select = parseSql({ query: 'SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id' })
    expect(select.joins[0].joinType).toBe('FULL')
  })

  it('should parse multiple JOINs', () => {
    const select = parseSql({ query: 'SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id' })
    expect(select.joins).toHaveLength(2)
    expect(select.joins[0].table).toBe('orders')
    expect(select.joins[1].table).toBe('products')
  })

  it('should parse JOIN with WHERE clause', () => {
    const select = parseSql({ query: 'SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE orders.total > 100' })
    expect(select.joins).toHaveLength(1)
    expect(select.where).toBeTruthy()
  })

  it('should parse qualified column names in WHERE', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE users.age > 18' })
    expect(select.where).toEqual({
      type: 'binary',
      op: '>',
      left: {
        type: 'identifier',
        name: 'users.age',
        positionStart: 26,
        positionEnd: 35,
      },
      right: { type: 'literal', value: 18, positionStart: 38, positionEnd: 40 },
      positionStart: 26,
      positionEnd: 40,
    })
  })
})
