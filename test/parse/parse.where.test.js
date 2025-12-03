import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql - WHERE clause', () => {
  it('should parse WHERE with equality', () => {
    const select = parseSql('SELECT * FROM users WHERE age = 25')
    expect(select.where).toEqual({
      type: 'binary',
      op: '=',
      left: { type: 'identifier', name: 'age' },
      right: { type: 'literal', value: 25 },
    })
  })

  it('should parse WHERE with string literal', () => {
    const select = parseSql('SELECT * FROM users WHERE name = \'John\'')
    expect(select.where).toEqual({
      type: 'binary',
      op: '=',
      left: { type: 'identifier', name: 'name' },
      right: { type: 'literal', value: 'John' },
    })
  })

  it('should parse WHERE with comparison operators', () => {
    const select = parseSql('SELECT * FROM users WHERE age > 18')
    expect(select.where?.type).toBe('binary')
    if (select.where?.type === 'binary') {
      expect(select.where.op).toBe('>')
    }
  })

  it('should parse WHERE with negative numbers', () => {
    const select = parseSql('SELECT * FROM users WHERE age > -18')
    expect(select.where).toEqual({
      type: 'binary',
      op: '>',
      left: { type: 'identifier', name: 'age' },
      right: { type: 'unary', op: '-', argument: { type: 'literal', value: 18 } },
    })
  })

  it('should parse WHERE with AND', () => {
    const select = parseSql('SELECT * FROM users WHERE age > 18 AND city = "NYC"')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 18 },
      },
      right: {
        type: 'binary',
        op: '=',
        left: { type: 'identifier', name: 'city' },
        right: { type: 'identifier', name: 'NYC' },
      },
    })
  })

  it('should parse WHERE with OR', () => {
    const select = parseSql('SELECT * FROM users WHERE age < 18 OR age > 65')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'OR',
      left: {
        type: 'binary',
        op: '<',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 18 },
      },
      right: {
        type: 'binary',
        op: '>',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 65 },
      },
    })
  })

  it('should parse WHERE with NOT', () => {
    const select = parseSql('SELECT * FROM users WHERE NOT active')
    expect(select.where).toEqual({
      type: 'unary',
      op: 'NOT',
      argument: { type: 'identifier', name: 'active' },
    })
  })

  it('should parse WHERE with parentheses', () => {
    const select = parseSql('SELECT * FROM users WHERE (age > 18 AND age < 65)')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 18 },
      },
      right: {
        type: 'binary',
        op: '<',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 65 },
      },
    })
  })

  it('should parse WHERE with boolean literals', () => {
    const ast1 = parseSql('SELECT * FROM users WHERE active = TRUE')
    expect(ast1.where?.type).toBe('binary')
    if (ast1.where?.type === 'binary') {
      expect(ast1.where.right).toEqual({ type: 'literal', value: true })
    }

    const ast2 = parseSql('SELECT * FROM users WHERE deleted = FALSE')
    expect(ast2.where?.type).toBe('binary')
    if (ast2.where?.type === 'binary') {
      expect(ast2.where.right).toEqual({ type: 'literal', value: false })
    }
  })

  it('should parse WHERE with NULL', () => {
    const select = parseSql('SELECT * FROM users WHERE email = NULL')
    expect(select.where?.type).toBe('binary')
    if (select.where?.type === 'binary') {
      expect(select.where.right).toEqual({ type: 'literal', value: null })
    }
  })

  it('should parse WHERE with IS NULL', () => {
    const select = parseSql('SELECT * FROM users WHERE email IS NULL')
    expect(select.where).toEqual({
      type: 'unary',
      op: 'IS NULL',
      argument: { type: 'identifier', name: 'email' },
    })
  })

  it('should parse WHERE with IS NOT NULL', () => {
    const select = parseSql('SELECT * FROM users WHERE email IS NOT NULL')
    expect(select.where).toEqual({
      type: 'unary',
      op: 'IS NOT NULL',
      argument: { type: 'identifier', name: 'email' },
    })
  })

  it('should parse WHERE with IS NULL in complex expression', () => {
    const select = parseSql('SELECT * FROM users WHERE email IS NULL AND age > 18')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'unary',
        op: 'IS NULL',
        argument: { type: 'identifier', name: 'email' },
      },
      right: {
        type: 'binary',
        op: '>',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 18 },
      },
    })
  })

  it('should parse WHERE with IS NOT NULL in OR expression', () => {
    const select = parseSql('SELECT * FROM users WHERE email IS NOT NULL OR phone IS NOT NULL')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'OR',
      left: {
        type: 'unary',
        op: 'IS NOT NULL',
        argument: { type: 'identifier', name: 'email' },
      },
      right: {
        type: 'unary',
        op: 'IS NOT NULL',
        argument: { type: 'identifier', name: 'phone' },
      },
    })
  })

  it('should parse WHERE with LIKE', () => {
    const select = parseSql('SELECT * FROM users WHERE name LIKE \'John%\'')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'LIKE',
      left: { type: 'identifier', name: 'name' },
      right: { type: 'literal', value: 'John%' },
    })
  })

  it('should parse WHERE with BETWEEN', () => {
    const select = parseSql('SELECT * FROM users WHERE age BETWEEN 18 AND 65')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>=',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 18 },
      },
      right: {
        type: 'binary',
        op: '<=',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 65 },
      },
    })
  })

  it('should parse WHERE with NOT BETWEEN', () => {
    const select = parseSql('SELECT * FROM users WHERE age NOT BETWEEN 18 AND 65')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'OR',
      left: {
        type: 'binary',
        op: '<',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 18 },
      },
      right: {
        type: 'binary',
        op: '>',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 65 },
      },
    })
  })

  it('should parse WHERE with BETWEEN and strings', () => {
    const select = parseSql('SELECT * FROM users WHERE name BETWEEN \'A\' AND \'M\'')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>=',
        left: { type: 'identifier', name: 'name' },
        right: { type: 'literal', value: 'A' },
      },
      right: {
        type: 'binary',
        op: '<=',
        left: { type: 'identifier', name: 'name' },
        right: { type: 'literal', value: 'M' },
      },
    })
  })

  it('should parse WHERE with BETWEEN in complex expression', () => {
    const select = parseSql('SELECT * FROM users WHERE age BETWEEN 18 AND 65 AND city = \'NYC\'')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: 'AND',
        left: {
          type: 'binary',
          op: '>=',
          left: { type: 'identifier', name: 'age' },
          right: { type: 'literal', value: 18 },
        },
        right: {
          type: 'binary',
          op: '<=',
          left: { type: 'identifier', name: 'age' },
          right: { type: 'literal', value: 65 },
        },
      },
      right: {
        type: 'binary',
        op: '=',
        left: { type: 'identifier', name: 'city' },
        right: { type: 'literal', value: 'NYC' },
      },
    })
  })

  it('should parse WHERE with BETWEEN and qualified column names', () => {
    const select = parseSql('SELECT * FROM users WHERE users.age BETWEEN 18 AND 65')
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>=',
        left: { type: 'identifier', name: 'users.age' },
        right: { type: 'literal', value: 18 },
      },
      right: {
        type: 'binary',
        op: '<=',
        left: { type: 'identifier', name: 'users.age' },
        right: { type: 'literal', value: 65 },
      },
    })
  })

  it('should parse WHERE with IN subquery', () => {
    const select = parseSql('SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = 1)')
    expect(select.where).toEqual({
      type: 'in',
      expr: { type: 'identifier', name: 'user_id' },
      subquery: {
        distinct: false,
        columns: [{ kind: 'derived', expr: { type: 'identifier', name: 'id' } }],
        from: { kind: 'table', table: 'users' },
        joins: [],
        where: {
          type: 'binary',
          op: '=',
          left: { type: 'identifier', name: 'active' },
          right: { type: 'literal', value: 1 },
        },
        groupBy: [],
        orderBy: [],
      },
    })
  })

  it('should parse WHERE with IN list of values', () => {
    const select = parseSql('SELECT * FROM users WHERE name IN (\'Alice\', \'Bob\', \'Charlie\')')
    expect(select.where).toEqual({
      type: 'in valuelist',
      expr: { type: 'identifier', name: 'name' },
      values: [
        { type: 'literal', value: 'Alice' },
        { type: 'literal', value: 'Bob' },
        { type: 'literal', value: 'Charlie' },
      ],
    })
  })

  it('should parse WHERE with EXISTS subquery', () => {
    const select = parseSql('SELECT * FROM orders WHERE EXISTS (SELECT * FROM users WHERE users.id = orders.user_id)')
    expect(select.where).toEqual({
      type: 'exists',
      subquery: {
        distinct: false,
        columns: [{ kind: 'star' }],
        from: { kind: 'table', table: 'users' },
        joins: [],
        where: {
          type: 'binary',
          op: '=',
          left: { type: 'identifier', name: 'users.id' },
          right: { type: 'identifier', name: 'orders.user_id' },
        },
        groupBy: [],
        orderBy: [],
      },
    })
  })

  it('should parse WHERE with NOT EXISTS subquery', () => {
    const select = parseSql('SELECT * FROM orders WHERE NOT EXISTS (SELECT * FROM users WHERE users.id = orders.user_id)')
    expect(select.where).toEqual({
      type: 'not exists',
      subquery: {
        distinct: false,
        columns: [{ kind: 'star' }],
        from: { kind: 'table', table: 'users' },
        joins: [],
        where: {
          type: 'binary',
          op: '=',
          left: { type: 'identifier', name: 'users.id' },
          right: { type: 'identifier', name: 'orders.user_id' },
        },
        groupBy: [],
        orderBy: [],
      },
    })
  })
})
