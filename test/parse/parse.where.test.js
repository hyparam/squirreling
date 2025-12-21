import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql - WHERE clause', () => {
  it('should parse WHERE with equality', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE age = 25' })
    expect(select.where).toEqual({
      type: 'binary',
      op: '=',
      left: { type: 'identifier', name: 'age', positionStart: 26, positionEnd: 29 },
      right: { type: 'literal', value: 25, positionStart: 32, positionEnd: 34 },
      positionStart: 26,
      positionEnd: 34,
    })
  })

  it('should parse WHERE with string literal', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE name = \'John\'' })
    expect(select.where).toEqual({
      type: 'binary',
      op: '=',
      left: { type: 'identifier', name: 'name', positionStart: 26, positionEnd: 30 },
      right: {
        type: 'literal',
        value: 'John',
        positionStart: 33,
        positionEnd: 39,
      },
      positionStart: 26,
      positionEnd: 39,
    })
  })

  it('should parse WHERE with comparison operators', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE age > 18' })
    expect(select.where?.type).toBe('binary')
    if (select.where?.type === 'binary') {
      expect(select.where.op).toBe('>')
    }
  })

  it('should parse WHERE with negative numbers', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE age > -18' })
    expect(select.where).toEqual({
      type: 'binary',
      op: '>',
      left: {
        type: 'identifier',
        name: 'age',
        positionStart: 26,
        positionEnd: 29,
      },
      right: {
        type: 'literal',
        value: -18,
        positionStart: 32,
        positionEnd: 35,
      },
      positionStart: 26,
      positionEnd: 35,
    })
  })

  it('should parse WHERE with AND', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE age > 18 AND city = "NYC"' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>',
        left: {
          type: 'identifier',
          name: 'age',
          positionStart: 26,
          positionEnd: 29,
        },
        right: {
          type: 'literal',
          value: 18,
          positionStart: 32,
          positionEnd: 34,
        },
        positionStart: 26,
        positionEnd: 34,
      },
      right: {
        type: 'binary',
        op: '=',
        left: {
          type: 'identifier',
          name: 'city',
          positionStart: 39,
          positionEnd: 43,
        },
        right: {
          type: 'identifier',
          name: 'NYC',
          positionStart: 46,
          positionEnd: 51,
        },
        positionStart: 39,
        positionEnd: 51,
      },
      positionStart: 26,
      positionEnd: 51,
    })
  })

  it('should parse WHERE with OR', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE age < 18 OR age > 65' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'OR',
      left: {
        type: 'binary',
        op: '<',
        left: {
          type: 'identifier',
          name: 'age',
          positionStart: 26,
          positionEnd: 29,
        },
        right: {
          type: 'literal',
          value: 18,
          positionStart: 32,
          positionEnd: 34,
        },
        positionStart: 26,
        positionEnd: 34,
      },
      right: {
        type: 'binary',
        op: '>',
        left: {
          type: 'identifier',
          name: 'age',
          positionStart: 38,
          positionEnd: 41,
        },
        right: {
          type: 'literal',
          value: 65,
          positionStart: 44,
          positionEnd: 46,
        },
        positionStart: 38,
        positionEnd: 46,
      },
      positionStart: 26,
      positionEnd: 46,
    })
  })

  it('should parse WHERE with NOT', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE NOT active' })
    expect(select.where).toEqual({
      type: 'unary',
      op: 'NOT',
      argument: {
        type: 'identifier',
        name: 'active',
        positionStart: 30,
        positionEnd: 36,
      },
      positionStart: 26,
      positionEnd: 36,
    })
  })

  it('should parse WHERE with parentheses', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE (age > 18 AND age < 65)' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>',
        left: {
          type: 'identifier',
          name: 'age',
          positionStart: 27,
          positionEnd: 30,
        },
        right: {
          type: 'literal',
          value: 18,
          positionStart: 33,
          positionEnd: 35,
        },
        positionStart: 27,
        positionEnd: 35,
      },
      right: {
        type: 'binary',
        op: '<',
        left: {
          type: 'identifier',
          name: 'age',
          positionStart: 40,
          positionEnd: 43,
        },
        right: {
          type: 'literal',
          value: 65,
          positionStart: 46,
          positionEnd: 48,
        },
        positionStart: 40,
        positionEnd: 48,
      },
      positionStart: 27,
      positionEnd: 48,
    })
  })

  it('should parse WHERE with boolean literals', () => {
    const ast1 = parseSql({ query: 'SELECT * FROM users WHERE active = TRUE' })
    expect(ast1.where?.type).toBe('binary')
    if (ast1.where?.type === 'binary') {
      expect(ast1.where.right).toEqual({
        type: 'literal',
        value: true,
        positionStart: 35,
        positionEnd: 39,
      })
    }

    const ast2 = parseSql({ query: 'SELECT * FROM users WHERE deleted = FALSE' })
    expect(ast2.where?.type).toBe('binary')
    if (ast2.where?.type === 'binary') {
      expect(ast2.where.right).toEqual({
        type: 'literal',
        value: false,
        positionStart: 36,
        positionEnd: 41,
      })
    }
  })

  it('should parse WHERE with NULL', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE email = NULL' })
    expect(select.where?.type).toBe('binary')
    if (select.where?.type === 'binary') {
      expect(select.where.right).toEqual({
        type: 'literal',
        value: null,
        positionStart: 34,
        positionEnd: 38,
      })
    }
  })

  it('should parse WHERE with IS NULL', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE email IS NULL' })
    expect(select.where).toEqual({
      type: 'unary',
      op: 'IS NULL',
      argument: {
        type: 'identifier',
        name: 'email',
        positionStart: 26,
        positionEnd: 31,
      },
      positionStart: 26,
      positionEnd: 39,
    })
  })

  it('should parse WHERE with IS NOT NULL', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE email IS NOT NULL' })
    expect(select.where).toEqual({
      type: 'unary',
      op: 'IS NOT NULL',
      argument: {
        type: 'identifier',
        name: 'email',
        positionStart: 26,
        positionEnd: 31,
      },
      positionStart: 26,
      positionEnd: 43,
    })
  })

  it('should parse WHERE with IS NULL in complex expression', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE email IS NULL AND age > 18' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'unary',
        op: 'IS NULL',
        argument: {
          type: 'identifier',
          name: 'email',
          positionStart: 26,
          positionEnd: 31,
        },
        positionStart: 26,
        positionEnd: 39,
      },
      right: {
        type: 'binary',
        op: '>',
        left: {
          type: 'identifier',
          name: 'age',
          positionStart: 44,
          positionEnd: 47,
        },
        right: {
          type: 'literal',
          value: 18,
          positionStart: 50,
          positionEnd: 52,
        },
        positionStart: 44,
        positionEnd: 52,
      },
      positionStart: 26,
      positionEnd: 52,
    })
  })

  it('should parse WHERE with IS NOT NULL in OR expression', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE email IS NOT NULL OR phone IS NOT NULL' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'OR',
      left: {
        type: 'unary',
        op: 'IS NOT NULL',
        argument: {
          type: 'identifier',
          name: 'email',
          positionStart: 26,
          positionEnd: 31,
        },
        positionStart: 26,
        positionEnd: 43,
      },
      right: {
        type: 'unary',
        op: 'IS NOT NULL',
        argument: {
          type: 'identifier',
          name: 'phone',
          positionStart: 47,
          positionEnd: 52,
        },
        positionStart: 47,
        positionEnd: 64,
      },
      positionStart: 26,
      positionEnd: 64,
    })
  })

  it('should parse WHERE with LIKE', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE name LIKE \'John%\'' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'LIKE',
      left: {
        type: 'identifier',
        name: 'name',
        positionStart: 26,
        positionEnd: 30,
      },
      right: {
        type: 'literal',
        value: 'John%',
        positionStart: 36,
        positionEnd: 43,
      },
      positionStart: 26,
      positionEnd: 43,
    })
  })

  it('should parse WHERE with BETWEEN', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE age BETWEEN 18 AND 65' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>=',
        left: {
          type: 'identifier',
          name: 'age',
          positionStart: 26,
          positionEnd: 29,
        },
        right: {
          type: 'literal',
          value: 18,
          positionStart: 38,
          positionEnd: 40,
        },
        positionStart: 26,
        positionEnd: 40,
      },
      right: {
        type: 'binary',
        op: '<=',
        left: {
          type: 'identifier',
          name: 'age',
          positionStart: 26,
          positionEnd: 29,
        },
        right: {
          type: 'literal',
          value: 65,
          positionStart: 45,
          positionEnd: 47,
        },
        positionStart: 26,
        positionEnd: 47,
      },
      positionStart: 26,
      positionEnd: 47,
    })
  })

  it('should parse WHERE with NOT BETWEEN', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE age NOT BETWEEN 18 AND 65' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'OR',
      left: {
        type: 'binary',
        op: '<',
        left: {
          type: 'identifier',
          name: 'age',
          positionStart: 26,
          positionEnd: 29,
        },
        right: {
          type: 'literal',
          value: 18,
          positionStart: 42,
          positionEnd: 44,
        },
        positionStart: 26,
        positionEnd: 44,
      },
      right: {
        type: 'binary',
        op: '>',
        left: {
          type: 'identifier',
          name: 'age',
          positionStart: 26,
          positionEnd: 29,
        },
        right: {
          type: 'literal',
          value: 65,
          positionStart: 49,
          positionEnd: 51,
        },
        positionStart: 26,
        positionEnd: 51,
      },
      positionStart: 30,
      positionEnd: 51,
    })
  })

  it('should parse WHERE with BETWEEN and strings', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE name BETWEEN \'A\' AND \'M\'' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>=',
        left: {
          type: 'identifier',
          name: 'name',
          positionStart: 26,
          positionEnd: 30,
        },
        right: {
          type: 'literal',
          value: 'A',
          positionStart: 39,
          positionEnd: 42,
        },
        positionStart: 26,
        positionEnd: 42,
      },
      right: {
        type: 'binary',
        op: '<=',
        left: {
          type: 'identifier',
          name: 'name',
          positionStart: 26,
          positionEnd: 30,
        },
        right: {
          type: 'literal',
          value: 'M',
          positionStart: 47,
          positionEnd: 50,
        },
        positionStart: 26,
        positionEnd: 50,
      },
      positionStart: 26,
      positionEnd: 50,
    })
  })

  it('should parse WHERE with BETWEEN in complex expression', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE age BETWEEN 18 AND 65 AND city = \'NYC\'' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: 'AND',
        left: {
          type: 'binary',
          op: '>=',
          left: {
            type: 'identifier',
            name: 'age',
            positionStart: 26,
            positionEnd: 29,
          },
          right: {
            type: 'literal',
            value: 18,
            positionStart: 38,
            positionEnd: 40,
          },
          positionStart: 26,
          positionEnd: 40,
        },
        right: {
          type: 'binary',
          op: '<=',
          left: {
            type: 'identifier',
            name: 'age',
            positionStart: 26,
            positionEnd: 29,
          },
          right: {
            type: 'literal',
            value: 65,
            positionStart: 45,
            positionEnd: 47,
          },
          positionStart: 26,
          positionEnd: 47,
        },
        positionStart: 26,
        positionEnd: 47,
      },
      right: {
        type: 'binary',
        op: '=',
        left: {
          type: 'identifier',
          name: 'city',
          positionStart: 52,
          positionEnd: 56,
        },
        right: {
          type: 'literal',
          value: 'NYC',
          positionStart: 59,
          positionEnd: 64,
        },
        positionStart: 52,
        positionEnd: 64,
      },
      positionStart: 26,
      positionEnd: 64,
    })
  })

  it('should parse WHERE with BETWEEN and qualified column names', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE users.age BETWEEN 18 AND 65' })
    expect(select.where).toEqual({
      type: 'binary',
      op: 'AND',
      left: {
        type: 'binary',
        op: '>=',
        left: {
          type: 'identifier',
          name: 'users.age',
          positionStart: 26,
          positionEnd: 35,
        },
        right: {
          type: 'literal',
          value: 18,
          positionStart: 44,
          positionEnd: 46,
        },
        positionStart: 26,
        positionEnd: 46,
      },
      right: {
        type: 'binary',
        op: '<=',
        left: {
          type: 'identifier',
          name: 'users.age',
          positionStart: 26,
          positionEnd: 35,
        },
        right: {
          type: 'literal',
          value: 65,
          positionStart: 51,
          positionEnd: 53,
        },
        positionStart: 26,
        positionEnd: 53,
      },
      positionStart: 26,
      positionEnd: 53,
    })
  })

  it('should parse WHERE with IN subquery', () => {
    const select = parseSql({ query: 'SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = 1)' })
    expect(select.where).toEqual({
      type: 'in',
      expr: {
        type: 'identifier',
        name: 'user_id',
        positionStart: 27,
        positionEnd: 34,
      },
      subquery: {
        distinct: false,
        columns: [
          {
            kind: 'derived',
            expr: {
              type: 'identifier',
              name: 'id',
              positionStart: 46,
              positionEnd: 48,
            },
          },
        ],
        from: { kind: 'table', table: 'users' },
        joins: [],
        where: {
          type: 'binary',
          op: '=',
          left: {
            type: 'identifier',
            name: 'active',
            positionStart: 66,
            positionEnd: 72,
          },
          right: {
            type: 'literal',
            value: 1,
            positionStart: 75,
            positionEnd: 76,
          },
          positionStart: 66,
          positionEnd: 76,
        },
        groupBy: [],
        orderBy: [],
      },
      positionStart: 27,
      positionEnd: 77,
    })
  })

  it('should parse WHERE with IN list of values', () => {
    const select = parseSql({ query: 'SELECT * FROM users WHERE name IN (\'Alice\', \'Bob\', \'Charlie\')' })
    expect(select.where).toEqual({
      type: 'in valuelist',
      expr: {
        type: 'identifier',
        name: 'name',
        positionStart: 26,
        positionEnd: 30,
      },
      values: [
        {
          type: 'literal',
          value: 'Alice',
          positionStart: 35,
          positionEnd: 42,
        },
        {
          type: 'literal',
          value: 'Bob',
          positionStart: 44,
          positionEnd: 49,
        },
        {
          type: 'literal',
          value: 'Charlie',
          positionStart: 51,
          positionEnd: 60,
        },
      ],
      positionStart: 26,
      positionEnd: 61,
    })
  })

  it('should parse WHERE with EXISTS subquery', () => {
    const select = parseSql({ query: 'SELECT * FROM orders WHERE EXISTS (SELECT * FROM users WHERE users.id = orders.user_id)' })
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
          left: {
            type: 'identifier',
            name: 'users.id',
            positionStart: 61,
            positionEnd: 69,
          },
          right: {
            type: 'identifier',
            name: 'orders.user_id',
            positionStart: 72,
            positionEnd: 86,
          },
          positionStart: 61,
          positionEnd: 86,
        },
        groupBy: [],
        orderBy: [],
      },
      positionStart: 27,
      positionEnd: 87,
    })
  })

  it('should parse WHERE with NOT EXISTS subquery', () => {
    const select = parseSql({ query: 'SELECT * FROM orders WHERE NOT EXISTS (SELECT * FROM users WHERE users.id = orders.user_id)' })
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
          left: {
            type: 'identifier',
            name: 'users.id',
            positionStart: 65,
            positionEnd: 73,
          },
          right: {
            type: 'identifier',
            name: 'orders.user_id',
            positionStart: 76,
            positionEnd: 90,
          },
          positionStart: 65,
          positionEnd: 90,
        },
        groupBy: [],
        orderBy: [],
      },
      positionStart: 27,
      positionEnd: 91,
    })
  })
})
