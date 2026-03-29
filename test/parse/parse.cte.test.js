import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'
import { parseWith } from '../helpers.js'

describe('parseSql - CTE (WITH clause)', () => {
  it('should parse simple CTE', () => {
    const stmt = parseWith({
      query: 'WITH cte AS (SELECT * FROM users) SELECT * FROM cte',
    })
    expect(stmt.ctes).toHaveLength(1)
    expect(stmt.ctes[0].name).toBe('cte')
    expect(stmt.ctes[0].query).toEqual({
      type: 'select',
      distinct: false,
      columns: [{ type: 'star' }],
      from: { type: 'table', table: 'users', positionStart: 27, positionEnd: 32 },
      joins: [],
      groupBy: [],
      orderBy: [],
      positionStart: 13,
      positionEnd: 32,
    })
    expect(stmt.query).toEqual({
      type: 'select',
      distinct: false,
      columns: [{ type: 'star' }],
      from: { type: 'table', table: 'cte', positionStart: 48, positionEnd: 51 },
      joins: [],
      groupBy: [],
      orderBy: [],
      positionStart: 34,
      positionEnd: 51,
    })
  })

  it('should parse CTE with columns and WHERE', () => {
    const stmt = parseWith({
      query: 'WITH active AS (SELECT id, name FROM users WHERE active = TRUE) SELECT name FROM active',
    })
    expect(stmt.ctes[0].query).toEqual({
      type: 'select',
      distinct: false,
      columns: [
        { type: 'derived', expr: { type: 'identifier', name: 'id', positionStart: 23, positionEnd: 25 } },
        { type: 'derived', expr: { type: 'identifier', name: 'name', positionStart: 27, positionEnd: 31 } },
      ],
      from: { type: 'table', table: 'users', positionStart: 37, positionEnd: 42 },
      joins: [],
      where: {
        type: 'binary',
        op: '=',
        left: { type: 'identifier', name: 'active', positionStart: 49, positionEnd: 55 },
        right: { type: 'literal', value: true, positionStart: 58, positionEnd: 62 },
        positionStart: 49,
        positionEnd: 62,
      },
      groupBy: [],
      orderBy: [],
      positionStart: 16,
      positionEnd: 62,
    })
  })

  it('should parse CTE with alias', () => {
    const stmt = parseWith({
      query: 'WITH cte AS (SELECT * FROM users) SELECT * FROM cte AS t',
    })
    expect(stmt.query).toEqual({
      type: 'select',
      distinct: false,
      columns: [{ type: 'star' }],
      from: { type: 'table', table: 'cte', alias: 't', positionStart: 48, positionEnd: 56 },
      joins: [],
      groupBy: [],
      orderBy: [],
      positionStart: 34,
      positionEnd: 56,
    })
  })

  it('should parse multiple CTEs', () => {
    const stmt = parseWith({
      query: 'WITH cte1 AS (SELECT id FROM users), cte2 AS (SELECT id FROM orders) SELECT * FROM cte1',
    })
    expect(stmt.ctes).toHaveLength(2)
    expect(stmt.ctes[0].name).toBe('cte1')
    expect(stmt.ctes[0].query).toEqual({
      type: 'select',
      distinct: false,
      columns: [{ type: 'derived', expr: { type: 'identifier', name: 'id', positionStart: 21, positionEnd: 23 } }],
      from: { type: 'table', table: 'users', positionStart: 29, positionEnd: 34 },
      joins: [],
      groupBy: [],
      orderBy: [],
      positionStart: 14,
      positionEnd: 34,
    })
    expect(stmt.ctes[1].name).toBe('cte2')
    expect(stmt.ctes[1].query).toEqual({
      type: 'select',
      distinct: false,
      columns: [{ type: 'derived', expr: { type: 'identifier', name: 'id', positionStart: 53, positionEnd: 55 } }],
      from: { type: 'table', table: 'orders', positionStart: 61, positionEnd: 67 },
      joins: [],
      groupBy: [],
      orderBy: [],
      positionStart: 46,
      positionEnd: 67,
    })
  })

  it('should parse CTE referencing another CTE', () => {
    const stmt = parseWith({
      query: 'WITH base AS (SELECT id FROM users), filtered AS (SELECT * FROM base WHERE id > 1) SELECT * FROM filtered',
    })
    expect(stmt.ctes[1].query).toEqual({
      type: 'select',
      distinct: false,
      columns: [{ type: 'star' }],
      from: { type: 'table', table: 'base', positionStart: 64, positionEnd: 68 },
      joins: [],
      where: {
        type: 'binary',
        op: '>',
        left: { type: 'identifier', name: 'id', positionStart: 75, positionEnd: 77 },
        right: { type: 'literal', value: 1, positionStart: 80, positionEnd: 81 },
        positionStart: 75,
        positionEnd: 81,
      },
      groupBy: [],
      orderBy: [],
      positionStart: 50,
      positionEnd: 81,
    })
  })

  it('should parse CTE with GROUP BY, HAVING, ORDER BY, and LIMIT', () => {
    const stmt = parseWith({
      query: 'WITH totals AS (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id HAVING SUM(amount) > 100 ORDER BY total DESC LIMIT 10) SELECT * FROM totals',
    })
    expect(stmt.ctes[0].query).toEqual({
      type: 'select',
      distinct: false,
      columns: [
        { type: 'derived', expr: { type: 'identifier', name: 'user_id', positionStart: 23, positionEnd: 30 } },
        { type: 'derived', expr: { type: 'function', funcName: 'SUM', args: [{ type: 'identifier', name: 'amount', positionStart: 36, positionEnd: 42 }], positionStart: 32, positionEnd: 43 }, alias: 'total' },
      ],
      from: { type: 'table', table: 'orders', positionStart: 58, positionEnd: 64 },
      joins: [],
      groupBy: [{ type: 'identifier', name: 'user_id', positionStart: 74, positionEnd: 81 }],
      having: {
        type: 'binary',
        op: '>',
        left: { type: 'function', funcName: 'SUM', args: [{ type: 'identifier', name: 'amount', positionStart: 93, positionEnd: 99 }], positionStart: 89, positionEnd: 100 },
        right: { type: 'literal', value: 100, positionStart: 103, positionEnd: 106 },
        positionStart: 89,
        positionEnd: 106,
      },
      orderBy: [{ expr: { type: 'identifier', name: 'total', positionStart: 116, positionEnd: 121 }, direction: 'DESC' }],
      limit: 10,
      positionStart: 16,
      positionEnd: 135,
    })
  })

  it('should parse CTE with JOIN in main query', () => {
    const stmt = parseWith({
      query: 'WITH active AS (SELECT id FROM users) SELECT active.id, orders.amount FROM active JOIN orders ON active.id = orders.user_id',
    })
    expect(stmt.query).toEqual({
      type: 'select',
      distinct: false,
      columns: [
        { type: 'derived', expr: { type: 'identifier', name: 'active.id', positionStart: 45, positionEnd: 54 } },
        { type: 'derived', expr: { type: 'identifier', name: 'orders.amount', positionStart: 56, positionEnd: 69 } },
      ],
      from: { type: 'table', table: 'active', positionStart: 75, positionEnd: 81 },
      joins: [{
        joinType: 'INNER',
        table: 'orders',
        on: {
          type: 'binary',
          op: '=',
          left: { type: 'identifier', name: 'active.id', positionStart: 97, positionEnd: 106 },
          right: { type: 'identifier', name: 'orders.user_id', positionStart: 109, positionEnd: 123 },
          positionStart: 97,
          positionEnd: 123,
        },
        positionStart: 82,
        positionEnd: 93,
      }],
      groupBy: [],
      orderBy: [],
      positionStart: 38,
      positionEnd: 123,
    })
  })

  it('should throw error for duplicate CTE names', () => {
    expect(() => {
      parseSql({ query: 'WITH cte AS (SELECT 1 FROM a), cte AS (SELECT 2 FROM b) SELECT * FROM cte' })
    }).toThrow('CTE "cte" is defined more than once at position 0')
  })

  it('should throw error for duplicate CTE names (case-insensitive)', () => {
    expect(() => {
      parseSql({ query: 'WITH Cte AS (SELECT 1 FROM a), CTE AS (SELECT 2 FROM b) SELECT * FROM cte' })
    }).toThrow('CTE "CTE" is defined more than once at position 0')
  })

  it('should preserve CTE name case', () => {
    const stmt = parseWith({
      query: 'WITH MyTable AS (SELECT * FROM users) SELECT * FROM MyTable',
    })
    expect(stmt.ctes[0].name).toBe('MyTable')
  })

  it('should parse set operations inside a CTE body', () => {
    const stmt = parseWith({
      query: `
        WITH cte AS (
          SELECT name FROM users WHERE age < 30
          UNION
          SELECT name FROM users WHERE age >= 30
        )
        SELECT * FROM cte
      `,
    })

    expect(stmt.ctes[0].query.type).toBe('compound')
    if (stmt.ctes[0].query.type === 'compound') {
      expect(stmt.ctes[0].query.operator).toBe('UNION')
      expect(stmt.ctes[0].query.left.type).toBe('select')
      expect(stmt.ctes[0].query.right.type).toBe('select')
    }
  })
})
