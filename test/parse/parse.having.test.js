import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql - HAVING clause', () => {
  it('should parse HAVING with simple condition', () => {
    const select = parseSql('SELECT city, COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 5')
    expect(select.having).toEqual({
      type: 'binary',
      op: '>',
      left: {
        type: 'function',
        name: 'COUNT',
        args: [{ type: 'identifier', name: '*' }],
      },
      right: { type: 'literal', value: 5 },
    })
  })

  it('should parse HAVING with aggregate comparison', () => {
    const select = parseSql('SELECT product, SUM(sales) as total FROM orders GROUP BY product HAVING SUM(sales) > 1000')
    expect(select.having).toEqual({
      type: 'binary',
      op: '>',
      left: {
        type: 'function',
        name: 'SUM',
        args: [{ type: 'identifier', name: 'sales' }],
      },
      right: { type: 'literal', value: 1000 },
    })
  })

  it('should parse HAVING with multiple conditions', () => {
    const select = parseSql('SELECT city, COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 5 AND AVG(age) > 25')
    expect(select.having).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>',
        left: {
          type: 'function',
          name: 'COUNT',
          args: [{ type: 'identifier', name: '*' }],
        },
        right: { type: 'literal', value: 5 },
      },
      right: {
        type: 'binary',
        op: '>',
        left: {
          type: 'function',
          name: 'AVG',
          args: [{ type: 'identifier', name: 'age' }],
        },
        right: { type: 'literal', value: 25 },
      },
    })
  })

  it('should parse HAVING with column reference', () => {
    const select = parseSql('SELECT city, COUNT(*) as cnt FROM users GROUP BY city HAVING city = \'NYC\'')
    expect(select.having).toEqual({
      type: 'binary',
      op: '=',
      left: { type: 'identifier', name: 'city' },
      right: { type: 'literal', value: 'NYC' },
    })
  })

  it('should parse GROUP BY with HAVING and ORDER BY', () => {
    const select = parseSql('SELECT city, COUNT(*) as cnt FROM users GROUP BY city HAVING COUNT(*) > 5 ORDER BY cnt DESC')
    expect(select.having).toBeTruthy()
    expect(select.orderBy).toHaveLength(1)
  })

  it('should parse complex query with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT', () => {
    const select = parseSql(`
      SELECT city, COUNT(*) as cnt, AVG(age) as avg_age
      FROM users
      WHERE age > 18
      GROUP BY city
      HAVING COUNT(*) >= 10 AND AVG(age) < 50
      ORDER BY cnt DESC
      LIMIT 5
    `)
    expect(select.where).toEqual({
      type: 'binary',
      op: '>',
      left: { type: 'identifier', name: 'age' },
      right: { type: 'literal', value: 18 },
    })
    expect(select.groupBy).toEqual([
      { type: 'identifier', name: 'city' },
    ])
    expect(select.having).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>=',
        left: {
          type: 'function',
          name: 'COUNT',
          args: [{ type: 'identifier', name: '*' }],
        },
        right: { type: 'literal', value: 10 },
      },
      right: {
        type: 'binary',
        op: '<',
        left: {
          type: 'function',
          name: 'AVG',
          args: [{ type: 'identifier', name: 'age' }],
        },
        right: { type: 'literal', value: 50 },
      },
    })
    expect(select.orderBy).toEqual([
      { expr: { type: 'identifier', name: 'cnt' }, direction: 'DESC' },
    ])
    expect(select.limit).toBe(5)
  })
})
