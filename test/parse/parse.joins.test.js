import { describe, expect, it } from 'vitest'
import { parseSelect } from '../helpers.js'

describe('parseSql - JOIN queries', () => {
  it('should parse simple INNER JOIN', () => {
    const select = parseSelect('SELECT * FROM users JOIN orders ON users.id = orders.user_id')
    expect(select.from).toEqual({ type: 'table', table: 'users', positionStart: 14, positionEnd: 19 })
    expect(select.joins).toEqual([
      {
        joinType: 'INNER',
        table: 'orders',
        positionStart: 20,
        positionEnd: 31,
        on: {
          type: 'binary',
          op: '=',
          left: {
            type: 'identifier',
            name: 'id',
            prefix: 'users',
            positionStart: 35,
            positionEnd: 43,
          },
          right: {
            type: 'identifier',
            name: 'user_id',
            prefix: 'orders',
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
    const select = parseSelect('SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id')
    expect(select.joins[0].joinType).toBe('INNER')
  })

  it('should parse LEFT JOIN', () => {
    const select = parseSelect('SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id')
    expect(select.joins).toEqual([
      {
        joinType: 'LEFT',
        table: 'orders',
        positionStart: 20,
        positionEnd: 36,
        on: {
          type: 'binary',
          op: '=',
          left: {
            type: 'identifier',
            name: 'id',
            prefix: 'users',
            positionStart: 40,
            positionEnd: 48,
          },
          right: {
            type: 'identifier',
            name: 'user_id',
            prefix: 'orders',
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
    const select = parseSelect('SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id')
    expect(select.joins[0].joinType).toBe('LEFT')
  })

  it('should parse RIGHT JOIN', () => {
    const select = parseSelect('SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id')
    expect(select.joins[0].joinType).toBe('RIGHT')
  })

  it('should parse FULL JOIN', () => {
    const select = parseSelect('SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id')
    expect(select.joins[0].joinType).toBe('FULL')
  })

  it('should parse multiple JOINs', () => {
    const select = parseSelect('SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id')
    expect(select.joins).toHaveLength(2)
    expect(select.joins[0].table).toBe('orders')
    expect(select.joins[1].table).toBe('products')
  })

  it('should parse JOIN with WHERE clause', () => {
    const select = parseSelect('SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE orders.total > 100')
    expect(select.joins).toHaveLength(1)
    expect(select.where).toBeTruthy()
  })

  it('should parse qualified column names in WHERE', () => {
    const select = parseSelect('SELECT * FROM users WHERE users.age > 18')
    expect(select.where).toEqual({
      type: 'binary',
      op: '>',
      left: {
        type: 'identifier',
        name: 'age',
        prefix: 'users',
        positionStart: 26,
        positionEnd: 35,
      },
      right: { type: 'literal', value: 18, positionStart: 38, positionEnd: 40 },
      positionStart: 26,
      positionEnd: 40,
    })
  })

  it('should parse POSITIONAL JOIN', () => {
    const select = parseSelect('SELECT * FROM users POSITIONAL JOIN orders')
    expect(select.from).toEqual({ type: 'table', table: 'users', positionStart: 14, positionEnd: 19 })
    expect(select.joins).toEqual([
      {
        joinType: 'POSITIONAL',
        table: 'orders',
        positionStart: 20,
        positionEnd: 42,
      },
    ])
  })

  it('should parse POSITIONAL JOIN with alias', () => {
    const select = parseSelect('SELECT * FROM users u POSITIONAL JOIN orders o')
    expect(select.from).toEqual({ type: 'table', table: 'users', alias: 'u', positionStart: 14, positionEnd: 21 })
    expect(select.joins).toEqual([
      {
        joinType: 'POSITIONAL',
        table: 'orders',
        alias: 'o',
        positionStart: 22,
        positionEnd: 44,
      },
    ])
  })

  it('should parse POSITIONAL JOIN with WHERE clause', () => {
    const select = parseSelect('SELECT * FROM users POSITIONAL JOIN orders WHERE users.id > 1')
    expect(select.joins[0].joinType).toBe('POSITIONAL')
    expect(select.joins[0].on).toBeUndefined()
    expect(select.where).toBeTruthy()
  })
})
