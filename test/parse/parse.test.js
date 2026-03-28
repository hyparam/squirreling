import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql', () => {
  describe('basic SELECT queries', () => {
    it('should parse literal SELECT', () => {
      const select = parseSql({ query: 'SELECT 1 from users' })
      expect(select.columns).toEqual([
        { type: 'derived', expr: { type: 'literal', value: 1, positionStart: 7, positionEnd: 8 } },
      ])
    })

    it('should parse SELECT *', () => {
      const select = parseSql({ query: 'SELECT * FROM users' })
      expect(select).toEqual({
        type: 'select',
        distinct: false,
        columns: [{ type: 'star' }],
        from: { type: 'table', table: 'users', positionStart: 14, positionEnd: 19 },
        joins: [],
        groupBy: [],
        orderBy: [],
        positionStart: 0,
        positionEnd: 19,
      })
    })

    it('should parse SELECT with qualified asterisk', () => {
      const select = parseSql({ query: 'SELECT users.* FROM users' })
      expect(select).toEqual({
        type: 'select',
        distinct: false,
        columns: [{ type: 'star', table: 'users' }],
        from: { type: 'table', table: 'users', positionStart: 20, positionEnd: 25 },
        joins: [],
        groupBy: [],
        orderBy: [],
        positionStart: 0,
        positionEnd: 25,
      })
    })

    it('should parse SELECT * with additional columns', () => {
      const select = parseSql({ query: 'SELECT *, a + b FROM data' })
      expect(select.columns).toEqual([
        { type: 'star' },
        {
          type: 'derived',
          expr: {
            type: 'binary',
            op: '+',
            left: { type: 'identifier', name: 'a', positionStart: 10, positionEnd: 11 },
            right: { type: 'identifier', name: 'b', positionStart: 14, positionEnd: 15 },
            positionStart: 10,
            positionEnd: 15,
          },
        },
      ])
    })

    it('should parse SELECT with columns before and after asterisk', () => {
      const select = parseSql({ query: 'SELECT id, *, name FROM data' })
      expect(select.columns).toEqual([
        { type: 'derived', expr: { type: 'identifier', name: 'id', positionStart: 7, positionEnd: 9 } },
        { type: 'star' },
        { type: 'derived', expr: { type: 'identifier', name: 'name', positionStart: 14, positionEnd: 18 } },
      ])
    })

    it('should parse SELECT with qualified asterisk and additional columns', () => {
      const select = parseSql({ query: 'SELECT users.*, total FROM users' })
      expect(select.columns).toEqual([
        { type: 'star', table: 'users' },
        {
          type: 'derived',
          expr: {
            type: 'identifier',
            name: 'total',
            positionStart: 16,
            positionEnd: 21,
          },
        },
      ])
    })

    it('should parse SELECT with single column', () => {
      const select = parseSql({ query: 'SELECT name FROM users' })
      expect(select.columns).toEqual([
        { type: 'derived', expr: { type: 'identifier', name: 'name', positionStart: 7, positionEnd: 11 } },
      ])
    })

    it('should parse SELECT with multiple columns', () => {
      const select = parseSql({ query: 'SELECT name, age, email FROM users' })
      expect(select.columns).toEqual([
        { type: 'derived', expr: { type: 'identifier', name: 'name', positionStart: 7, positionEnd: 11 } },
        { type: 'derived', expr: { type: 'identifier', name: 'age', positionStart: 13, positionEnd: 16 } },
        { type: 'derived', expr: { type: 'identifier', name: 'email', positionStart: 18, positionEnd: 23 } },
      ])
    })

    it('should parse SELECT DISTINCT', () => {
      const select = parseSql({ query: 'SELECT DISTINCT city FROM users' })
      expect(select.distinct).toBe(true)
    })

    it('should handle trailing semicolon', () => {
      const select = parseSql({ query: 'SELECT * FROM users;' })
      expect(select.from).toEqual({ type: 'table', table: 'users', positionStart: 14, positionEnd: 19 })
    })

    it('should parse SELECT with negative number', () => {
      const select = parseSql({ query: 'SELECT -age as neg_age FROM users' })
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'unary',
            op: '-',
            argument: { type: 'identifier', name: 'age', positionStart: 8, positionEnd: 11 },
            positionStart: 7,
            positionEnd: 11,
          },
          alias: 'neg_age',
        },
      ])
    })
  })

  describe('column aliases', () => {
    it('should parse column alias with AS', () => {
      const select = parseSql({ query: 'SELECT name AS full_name FROM users' })
      expect(select.columns).toEqual([
        { type: 'derived', expr: { type: 'identifier', name: 'name', positionStart: 7, positionEnd: 11 }, alias: 'full_name' },
      ])
    })

    it('should parse column alias without AS', () => {
      const select = parseSql({ query: 'SELECT name FROM users' })
      expect(select.columns).toEqual([
        { type: 'derived', expr: { type: 'identifier', name: 'name', positionStart: 7, positionEnd: 11 } },
      ])
      expect(select.from).toEqual({ type: 'table', table: 'users', positionStart: 17, positionEnd: 22 })
    })
  })

  describe('quoted identifiers', () => {
    it('should parse column names with spaces using double quotes', () => {
      const select = parseSql({ query: 'SELECT "first name", "last name" FROM users' })
      expect(select.columns).toEqual([
        { type: 'derived', expr: { type: 'identifier', name: 'first name', positionStart: 7, positionEnd: 19 } },
        { type: 'derived', expr: { type: 'identifier', name: 'last name', positionStart: 21, positionEnd: 32 } },
      ])
    })

    it('should parse quoted table names with spaces', () => {
      const select = parseSql({ query: 'SELECT * FROM "user data"' })
      expect(select.from).toEqual({ type: 'table', table: 'user data', positionStart: 14, positionEnd: 25 })
    })

    it('should parse table alias', () => {
      const select = parseSql({ query: 'SELECT u.name FROM users u WHERE u.active = 1' })
      expect(select.from).toEqual({ type: 'table', table: 'users', alias: 'u', positionStart: 19, positionEnd: 26 })
    })

    it('should parse table alias with AS', () => {
      const select = parseSql({ query: 'SELECT u.name FROM users AS u' })
      expect(select.from).toEqual({ type: 'table', table: 'users', alias: 'u', positionStart: 19, positionEnd: 29 })
    })

    it('should parse quoted column with alias', () => {
      const select = parseSql({ query: 'SELECT "first name" AS fname FROM users' })
      expect(select.columns).toEqual([
        { type: 'derived', expr: { type: 'identifier', name: 'first name', positionStart: 7, positionEnd: 19 }, alias: 'fname' },
      ])
    })

    it('should parse mixed quoted and unquoted identifiers', () => {
      const select = parseSql({ query: 'SELECT id, "full name", email FROM users' })
      expect(select.columns).toEqual([
        { type: 'derived', expr: { type: 'identifier', name: 'id', positionStart: 7, positionEnd: 9 } },
        { type: 'derived', expr: { type: 'identifier', name: 'full name', positionStart: 11, positionEnd: 22 } },
        { type: 'derived', expr: { type: 'identifier', name: 'email', positionStart: 24, positionEnd: 29 } },
      ])
    })
  })

  describe('LIMIT and OFFSET', () => {
    it('should parse LIMIT', () => {
      const select = parseSql({ query: 'SELECT * FROM users LIMIT 10' })
      expect(select.limit).toBe(10)
      expect(select.offset).toBeUndefined()
    })

    it('should parse LIMIT with OFFSET', () => {
      const select = parseSql({ query: 'SELECT * FROM users LIMIT 10 OFFSET 20' })
      expect(select.limit).toBe(10)
      expect(select.offset).toBe(20)
    })

    it('should parse OFFSET without LIMIT', () => {
      const select = parseSql({ query: 'SELECT * FROM users OFFSET 20' })
      expect(select.limit).toBeUndefined()
      expect(select.offset).toBe(20)
    })
  })

  describe('complex queries', () => {
    it('should parse query with all clauses', () => {
      const select = parseSql({ query: `
        SELECT DISTINCT city, COUNT(*) AS total
        FROM users
        WHERE age > 18
        GROUP BY city
        ORDER BY total DESC
        LIMIT 5
        OFFSET 10
      ` })
      expect(select).toEqual({
        type: 'select',
        distinct: true,
        columns: [
          {
            type: 'derived',
            expr: {
              type: 'identifier',
              name: 'city',
              positionStart: 25,
              positionEnd: 29,
            },
          },
          {
            type: 'derived',
            expr: {
              type: 'function',
              funcName: 'COUNT',
              args: [{ type: 'star', positionStart: 37, positionEnd: 38 }],
              positionStart: 31,
              positionEnd: 39,
            },
            alias: 'total',
          },
        ],
        from: { type: 'table', table: 'users', positionStart: 62, positionEnd: 67 },
        joins: [],
        where: {
          type: 'binary',
          op: '>',
          left: {
            type: 'identifier',
            name: 'age',
            positionStart: 82,
            positionEnd: 85,
          },
          right: { type: 'literal', value: 18, positionStart: 88, positionEnd: 90 },
          positionStart: 82,
          positionEnd: 90,
        },
        groupBy: [
          {
            type: 'identifier',
            name: 'city',
            positionStart: 108,
            positionEnd: 112,
          },
        ],
        orderBy: [
          {
            expr: {
              type: 'identifier',
              name: 'total',
              positionStart: 130,
              positionEnd: 135,
            },
            direction: 'DESC',
          },
        ],
        limit: 5,
        offset: 10,
        positionStart: 9,
        positionEnd: 174,
      })
    })

    it('should parse query with complex WHERE expression', () => {
      const select = parseSql({ query: `
        SELECT * FROM users
        WHERE (age > 18 AND age < 65) OR status = 'admin'
      ` })
      expect(select.where?.type).toBe('binary')
      if (select.where?.type === 'binary') {
        expect(select.where.op).toBe('OR')
      }
    })

    it('should parse cast', () => {
      const select = parseSql({ query: 'SELECT CAST(age AS STRING) AS age_str FROM users' })
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'cast',
            expr: { type: 'identifier', name: 'age', positionStart: 12, positionEnd: 15 },
            toType: 'STRING',
            positionStart: 7,
            positionEnd: 26,
          },
          alias: 'age_str',
        },
      ])
    })

    it('should parse nested cast in aggregate', () => {
      const select = parseSql({ query: 'SELECT SUM(CAST(size AS BIGINT)) AS total_size FROM files' })
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'SUM',
            args: [{
              type: 'cast',
              expr: { type: 'identifier', name: 'size', positionStart: 16, positionEnd: 20 },
              toType: 'BIGINT',
              positionStart: 11,
              positionEnd: 31,
            }],
            positionStart: 7,
            positionEnd: 32,
          },
          alias: 'total_size',
        },
      ])
    })

    it('should parse subquery in FROM clause with AS', () => {
      const select = parseSql({ query: 'SELECT name FROM (SELECT * FROM users WHERE active = 1) AS u' })
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'identifier',
            name: 'name',
            positionStart: 7,
            positionEnd: 11,
          },
        },
      ])
      expect(select.from).toEqual({
        type: 'subquery',
        query: {
          type: 'select',
          distinct: false,
          columns: [{ type: 'star' }],
          from: { type: 'table', table: 'users', positionStart: 32, positionEnd: 37 },
          joins: [],
          where: {
            type: 'binary',
            op: '=',
            left: { type: 'identifier', name: 'active', positionStart: 44, positionEnd: 50 },
            right: { type: 'literal', value: 1, positionStart: 53, positionEnd: 54 },
            positionStart: 44,
            positionEnd: 54,
          },
          groupBy: [],
          orderBy: [],
          positionStart: 18,
          positionEnd: 54,
        },
        alias: 'u',
        positionStart: 17,
        positionEnd: 60,
      })
    })

    it('should parse subquery in FROM clause without AS', () => {
      const select = parseSql({ query: 'SELECT name FROM (SELECT * FROM users) u' })
      expect(select.from).toEqual({
        type: 'subquery',
        query: {
          type: 'select',
          distinct: false,
          columns: [{ type: 'star' }],
          from: { type: 'table', table: 'users', positionStart: 32, positionEnd: 37 },
          joins: [],
          groupBy: [],
          orderBy: [],
          positionStart: 18,
          positionEnd: 37,
        },
        alias: 'u',
        positionStart: 17,
        positionEnd: 40,
      })
    })
  })

  describe('CASE expressions', () => {
    it('should parse searched CASE expression', () => {
      const select = parseSql({ query: 'SELECT CASE WHEN age > 18 THEN \'adult\' ELSE \'minor\' END FROM users' })
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'case',
            whenClauses: [
              {
                condition: {
                  type: 'binary',
                  op: '>',
                  left: {
                    type: 'identifier',
                    name: 'age',
                    positionStart: 17,
                    positionEnd: 20,
                  },
                  right: {
                    type: 'literal',
                    value: 18,
                    positionStart: 23,
                    positionEnd: 25,
                  },
                  positionStart: 17,
                  positionEnd: 25,
                },
                positionStart: 17,
                positionEnd: 38,
                result: {
                  type: 'literal',
                  value: 'adult',
                  positionStart: 31,
                  positionEnd: 38,
                },
              },
            ],
            elseResult: {
              type: 'literal',
              value: 'minor',
              positionStart: 44,
              positionEnd: 51,
            },
            positionStart: 7,
            positionEnd: 55,
          },
        },
      ])
    })

    it('should parse simple CASE expression', () => {
      const select = parseSql({ query: 'SELECT CASE status WHEN 1 THEN \'active\' WHEN 0 THEN \'inactive\' END FROM users' })
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'case',
            caseExpr: {
              type: 'identifier',
              name: 'status',
              positionStart: 12,
              positionEnd: 18,
            },
            whenClauses: [
              {
                condition: {
                  type: 'literal',
                  value: 1,
                  positionStart: 24,
                  positionEnd: 25,
                },
                positionStart: 24,
                positionEnd: 39,
                result: {
                  type: 'literal',
                  value: 'active',
                  positionStart: 31,
                  positionEnd: 39,
                },
              },
              {
                condition: {
                  type: 'literal',
                  value: 0,
                  positionStart: 45,
                  positionEnd: 46,
                },
                positionStart: 45,
                positionEnd: 62,
                result: {
                  type: 'literal',
                  value: 'inactive',
                  positionStart: 52,
                  positionEnd: 62,
                },
              },
            ],
            positionStart: 7,
            positionEnd: 66,
          },
        },
      ])
    })

    it('should parse CASE expression with alias', () => {
      const select = parseSql({ query: 'SELECT CASE WHEN age >= 18 THEN \'adult\' ELSE \'minor\' END AS age_group FROM users' })
      expect(select.columns[0]).toEqual({
        type: 'derived',
        expr: {
          type: 'case',
          whenClauses: [
            {
              condition: {
                type: 'binary',
                op: '>=',
                left: {
                  type: 'identifier',
                  name: 'age',
                  positionStart: 17,
                  positionEnd: 20,
                },
                right: {
                  type: 'literal',
                  value: 18,
                  positionStart: 24,
                  positionEnd: 26,
                },
                positionStart: 17,
                positionEnd: 26,
              },
              positionStart: 17,
              positionEnd: 39,
              result: {
                type: 'literal',
                value: 'adult',
                positionStart: 32,
                positionEnd: 39,
              },
            },
          ],
          elseResult: {
            type: 'literal',
            value: 'minor',
            positionStart: 45,
            positionEnd: 52,
          },
          positionStart: 7,
          positionEnd: 56,
        },
        alias: 'age_group',
      })
    })
  })
})
