import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql - HAVING clause', () => {
  it('should parse HAVING with simple condition', () => {
    const select = parseSql({ query: 'SELECT city, COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 5' })
    expect(select.having).toEqual({
      type: 'binary',
      op: '>',
      left: {
        type: 'function',
        name: 'COUNT',
        args: [
          {
            type: 'identifier',
            name: '*',
            positionStart: 60,
            positionEnd: 61,
          },
        ],
        positionStart: 54,
        positionEnd: 62,
      },
      right: { type: 'literal', value: 5, positionStart: 65, positionEnd: 66 },
      positionStart: 54,
      positionEnd: 66,
    })
  })

  it('should parse HAVING with aggregate comparison', () => {
    const select = parseSql({ query: 'SELECT product, SUM(sales) as total FROM orders GROUP BY product HAVING SUM(sales) > 1000' })
    expect(select.having).toEqual({
      type: 'binary',
      op: '>',
      left: {
        type: 'function',
        name: 'SUM',
        args: [
          {
            type: 'identifier',
            name: 'sales',
            positionStart: 76,
            positionEnd: 81,
          },
        ],
        positionStart: 72,
        positionEnd: 82,
      },
      right: {
        type: 'literal',
        value: 1000,
        positionStart: 85,
        positionEnd: 89,
      },
      positionStart: 72,
      positionEnd: 89,
    })
  })

  it('should parse HAVING with multiple conditions', () => {
    const select = parseSql({ query: 'SELECT city, COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 5 AND AVG(age) > 25' })
    expect(select.having).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>',
        left: {
          type: 'function',
          name: 'COUNT',
          args: [
            {
              type: 'identifier',
              name: '*',
              positionStart: 60,
              positionEnd: 61,
            },
          ],
          positionStart: 54,
          positionEnd: 62,
        },
        right: { type: 'literal', value: 5, positionStart: 65, positionEnd: 66 },
        positionStart: 54,
        positionEnd: 66,
      },
      right: {
        type: 'binary',
        op: '>',
        left: {
          type: 'function',
          name: 'AVG',
          args: [
            {
              type: 'identifier',
              name: 'age',
              positionStart: 75,
              positionEnd: 78,
            },
          ],
          positionStart: 71,
          positionEnd: 79,
        },
        right: {
          type: 'literal',
          value: 25,
          positionStart: 82,
          positionEnd: 84,
        },
        positionStart: 71,
        positionEnd: 84,
      },
      positionStart: 54,
      positionEnd: 84,
    })
  })

  it('should parse HAVING with column reference', () => {
    const select = parseSql({ query: 'SELECT city, COUNT(*) as cnt FROM users GROUP BY city HAVING city = \'NYC\'' })
    expect(select.having).toEqual({
      type: 'binary',
      op: '=',
      left: {
        type: 'identifier',
        name: 'city',
        positionStart: 61,
        positionEnd: 65,
      },
      right: {
        type: 'literal',
        value: 'NYC',
        positionStart: 68,
        positionEnd: 73,
      },
      positionStart: 61,
      positionEnd: 73,
    })
  })

  it('should parse GROUP BY with HAVING and ORDER BY', () => {
    const select = parseSql({ query: 'SELECT city, COUNT(*) as cnt FROM users GROUP BY city HAVING COUNT(*) > 5 ORDER BY cnt DESC' })
    expect(select.having).toBeTruthy()
    expect(select.orderBy).toHaveLength(1)
  })

  it('should parse complex query with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT', () => {
    const select = parseSql({ query: `
      SELECT city, COUNT(*) as cnt, AVG(age) as avg_age
      FROM users
      WHERE age > 18
      GROUP BY city
      HAVING COUNT(*) >= 10 AND AVG(age) < 50
      ORDER BY cnt DESC
      LIMIT 5
    ` })
    expect(select.where).toEqual({
      type: 'binary',
      op: '>',
      left: {
        type: 'identifier',
        name: 'age',
        positionStart: 86,
        positionEnd: 89,
      },
      right: { type: 'literal', value: 18, positionStart: 92, positionEnd: 94 },
      positionStart: 86,
      positionEnd: 94,
    })
    expect(select.groupBy).toEqual([
      { type: 'identifier', name: 'city', positionStart: 110, positionEnd: 114 },
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
          args: [
            {
              type: 'identifier',
              name: '*',
              positionStart: 134,
              positionEnd: 135,
            },
          ],
          positionStart: 128,
          positionEnd: 136,
        },
        right: {
          type: 'literal',
          value: 10,
          positionStart: 140,
          positionEnd: 142,
        },
        positionStart: 128,
        positionEnd: 142,
      },
      right: {
        type: 'binary',
        op: '<',
        left: {
          type: 'function',
          name: 'AVG',
          args: [
            {
              type: 'identifier',
              name: 'age',
              positionStart: 151,
              positionEnd: 154,
            },
          ],
          positionStart: 147,
          positionEnd: 155,
        },
        right: {
          type: 'literal',
          value: 50,
          positionStart: 158,
          positionEnd: 160,
        },
        positionStart: 147,
        positionEnd: 160,
      },
      positionStart: 128,
      positionEnd: 160,
    })
    expect(select.orderBy).toEqual([
      {
        expr: {
          type: 'identifier',
          name: 'cnt',
          positionStart: 176,
          positionEnd: 179,
        },
        direction: 'DESC',
        nulls: undefined,
      },
    ])
    expect(select.limit).toBe(5)
  })
})
