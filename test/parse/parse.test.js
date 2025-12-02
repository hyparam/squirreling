import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql', () => {
  describe('basic SELECT queries', () => {
    it('should parse literal SELECT', () => {
      const select = parseSql('SELECT 1 from users')
      expect(select.columns).toEqual([
        { kind: 'derived', expr: { type: 'literal', value: 1 } },
      ])
    })

    it('should parse SELECT *', () => {
      const select = parseSql('SELECT * FROM users')
      expect(select).toEqual({
        distinct: false,
        columns: [{ kind: 'star' }],
        from: { kind: 'table', table: 'users' },
        joins: [],
        groupBy: [],
        orderBy: [],
      })
    })

    it('should parse SELECT with qualified asterisk', () => {
      const select = parseSql('SELECT users.* FROM users')
      expect(select).toEqual({
        distinct: false,
        columns: [{ kind: 'star', table: 'users' }],
        from: { kind: 'table', table: 'users' },
        joins: [],
        groupBy: [],
        orderBy: [],
      })
    })

    it('should parse SELECT with single column', () => {
      const select = parseSql('SELECT name FROM users')
      expect(select.columns).toEqual([
        { kind: 'derived', expr: { type: 'identifier', name: 'name' } },
      ])
    })

    it('should parse SELECT with multiple columns', () => {
      const select = parseSql('SELECT name, age, email FROM users')
      expect(select.columns).toEqual([
        { kind: 'derived', expr: { type: 'identifier', name: 'name' } },
        { kind: 'derived', expr: { type: 'identifier', name: 'age' } },
        { kind: 'derived', expr: { type: 'identifier', name: 'email' } },
      ])
    })

    it('should parse SELECT DISTINCT', () => {
      const select = parseSql('SELECT DISTINCT city FROM users')
      expect(select.distinct).toBe(true)
    })

    it('should handle trailing semicolon', () => {
      const select = parseSql('SELECT * FROM users;')
      expect(select.from).toEqual({ kind: 'table', table: 'users' })
    })

    it('should parse SELECT with negative number', () => {
      const select = parseSql('SELECT -age as neg_age FROM users')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'unary',
            op: '-',
            argument: { type: 'identifier', name: 'age' },
          },
          alias: 'neg_age',
        },
      ])
    })
  })

  describe('column aliases', () => {
    it('should parse column alias with AS', () => {
      const select = parseSql('SELECT name AS full_name FROM users')
      expect(select.columns).toEqual([
        { kind: 'derived', expr: { type: 'identifier', name: 'name' }, alias: 'full_name' },
      ])
    })

    it('should parse column alias without AS', () => {
      const select = parseSql('SELECT name full_name FROM users')
      expect(select.columns).toEqual([
        { kind: 'derived', expr: { type: 'identifier', name: 'name' }, alias: 'full_name' },
      ])
    })

    it('should not treat FROM as implicit alias', () => {
      const select = parseSql('SELECT name FROM users')
      expect(select.columns[0].alias).toBeUndefined()
      expect(select.from).toEqual({ kind: 'table', table: 'users' })
    })
  })

  describe('quoted identifiers', () => {
    it('should parse column names with spaces using double quotes', () => {
      const select = parseSql('SELECT "first name", "last name" FROM users')
      expect(select.columns).toEqual([
        { kind: 'derived', expr: { type: 'identifier', name: 'first name' } },
        { kind: 'derived', expr: { type: 'identifier', name: 'last name' } },
      ])
    })

    it('should parse quoted table names with spaces', () => {
      const select = parseSql('SELECT * FROM "user data"')
      expect(select.from).toEqual({ kind: 'table', table: 'user data' })
    })

    it('should parse table alias', () => {
      const select = parseSql('SELECT u.name FROM users u WHERE u.active = 1')
      expect(select.from).toEqual({ kind: 'table', table: 'users', alias: 'u' })
    })

    it('should parse table alias with AS', () => {
      const select = parseSql('SELECT u.name FROM users AS u')
      expect(select.from).toEqual({ kind: 'table', table: 'users', alias: 'u' })
    })

    it('should parse quoted column with alias', () => {
      const select = parseSql('SELECT "first name" AS fname FROM users')
      expect(select.columns).toEqual([
        { kind: 'derived', expr: { type: 'identifier', name: 'first name' }, alias: 'fname' },
      ])
    })

    it('should parse mixed quoted and unquoted identifiers', () => {
      const select = parseSql('SELECT id, "full name", email FROM users')
      expect(select.columns).toEqual([
        { kind: 'derived', expr: { type: 'identifier', name: 'id' } },
        { kind: 'derived', expr: { type: 'identifier', name: 'full name' } },
        { kind: 'derived', expr: { type: 'identifier', name: 'email' } },
      ])
    })
  })

  describe('aggregate functions', () => {
    it('should parse COUNT(*)', () => {
      const select = parseSql('SELECT COUNT(*) FROM users')
      expect(select.columns).toEqual([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' } },
      ])
    })

    it('should parse COUNT with column', () => {
      const select = parseSql('SELECT COUNT(id) FROM users')
      expect(select.columns).toEqual([
        {
          kind: 'aggregate',
          func: 'COUNT',
          arg: {
            kind: 'expression',
            expr: { type: 'identifier', name: 'id' },
          },
        },
      ])
    })

    it('should parse SUM', () => {
      const select = parseSql('SELECT SUM(amount) FROM transactions')
      expect(select.columns).toEqual([
        {
          kind: 'aggregate',
          func: 'SUM',
          arg: { kind: 'expression', expr: { type: 'identifier', name: 'amount' } },
        },
      ])
    })

    it('should parse AVG', () => {
      const select = parseSql('SELECT AVG(score) FROM tests')
      expect(select.columns).toEqual([
        {
          kind: 'aggregate',
          func: 'AVG',
          arg: { kind: 'expression', expr: { type: 'identifier', name: 'score' } },
        },
      ])
    })

    it('should parse MIN and MAX', () => {
      const select = parseSql('SELECT MIN(price), MAX(price) FROM products')
      expect(select.columns).toEqual([
        {
          kind: 'aggregate',
          func: 'MIN',
          arg: { kind: 'expression', expr: { type: 'identifier', name: 'price' } },
        },
        {
          kind: 'aggregate',
          func: 'MAX',
          arg: { kind: 'expression', expr: { type: 'identifier', name: 'price' } },
        },
      ])
    })

    it('should parse aggregate with alias', () => {
      const select = parseSql('SELECT COUNT(*) AS total FROM users')
      expect(select.columns).toEqual([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' }, alias: 'total' },
      ])
    })
  })

  describe('GROUP BY clause', () => {
    it('should parse GROUP BY with single column', () => {
      const select = parseSql('SELECT city, COUNT(*) FROM users GROUP BY city')
      expect(select.groupBy).toEqual([{ type: 'identifier', name: 'city' }])
    })

    it('should parse GROUP BY with multiple columns', () => {
      const select = parseSql('SELECT city, state, COUNT(*) FROM users GROUP BY city, state')
      expect(select.groupBy).toEqual([
        { type: 'identifier', name: 'city' },
        { type: 'identifier', name: 'state' },
      ])
    })
  })

  describe('LIMIT and OFFSET', () => {
    it('should parse LIMIT', () => {
      const select = parseSql('SELECT * FROM users LIMIT 10')
      expect(select.limit).toBe(10)
      expect(select.offset).toBeUndefined()
    })

    it('should parse LIMIT with OFFSET', () => {
      const select = parseSql('SELECT * FROM users LIMIT 10 OFFSET 20')
      expect(select.limit).toBe(10)
      expect(select.offset).toBe(20)
    })

    it('should parse OFFSET without LIMIT', () => {
      const select = parseSql('SELECT * FROM users OFFSET 20')
      expect(select.limit).toBeUndefined()
      expect(select.offset).toBe(20)
    })
  })

  describe('complex queries', () => {
    it('should parse query with all clauses', () => {
      const select = parseSql(`
        SELECT DISTINCT city, COUNT(*) AS total
        FROM users
        WHERE age > 18
        GROUP BY city
        ORDER BY total DESC
        LIMIT 5
        OFFSET 10
      `)
      expect(select).toEqual({
        distinct: true,
        columns: [
          { kind: 'derived', expr: { type: 'identifier', name: 'city' } },
          { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' }, alias: 'total' },
        ],
        from: { kind: 'table', table: 'users' },
        joins: [],
        where: {
          type: 'binary',
          op: '>',
          left: { type: 'identifier', name: 'age' },
          right: { type: 'literal', value: 18 },
        },
        groupBy: [{ type: 'identifier', name: 'city' }],
        orderBy: [
          { expr: { type: 'identifier', name: 'total' }, direction: 'DESC' },
        ],
        limit: 5,
        offset: 10,
      })
    })

    it('should parse query with complex WHERE expression', () => {
      const select = parseSql(`
        SELECT * FROM users
        WHERE (age > 18 AND age < 65) OR status = 'admin'
      `)
      expect(select.where?.type).toBe('binary')
      if (select.where?.type === 'binary') {
        expect(select.where.op).toBe('OR')
      }
    })

    it('should parse cast', () => {
      const select = parseSql('SELECT CAST(age AS STRING) AS age_str FROM users')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'cast',
            expr: { type: 'identifier', name: 'age' },
            toType: 'STRING',
          },
          alias: 'age_str',
        },
      ])
    })

    it('should parse nested cast in aggregate', () => {
      const select = parseSql('SELECT SUM(CAST(size AS BIGINT)) AS total_size FROM files')
      expect(select.columns).toEqual([
        {
          kind: 'aggregate',
          func: 'SUM',
          arg: {
            kind: 'expression',
            expr: {
              type: 'cast',
              expr: { type: 'identifier', name: 'size' },
              toType: 'BIGINT',
            },
          },
          alias: 'total_size',
        },
      ])
    })

    it('should parse subquery in FROM clause', () => {
      const select = parseSql('SELECT name FROM (SELECT * FROM users WHERE active = 1) AS u')
      expect(select.columns).toEqual([
        { kind: 'derived', expr: { type: 'identifier', name: 'name' } },
      ])
      expect(select.from).toMatchObject({
        kind: 'subquery',
        query: {
          columns: [{ kind: 'star' }],
          from: { kind: 'table', table: 'users' },
          where: {
            type: 'binary',
            op: '=',
            left: { type: 'identifier', name: 'active' },
            right: { type: 'literal', value: 1 },
          },
        },
        alias: 'u',
      })
    })
  })

  describe('CASE expressions', () => {
    it('should parse searched CASE expression', () => {
      const select = parseSql('SELECT CASE WHEN age > 18 THEN \'adult\' ELSE \'minor\' END FROM users')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'case',
            whenClauses: [
              {
                condition: {
                  type: 'binary',
                  op: '>',
                  left: { type: 'identifier', name: 'age' },
                  right: { type: 'literal', value: 18 },
                },
                result: { type: 'literal', value: 'adult' },
              },
            ],
            elseResult: { type: 'literal', value: 'minor' },
          },
        },
      ])
    })

    it('should parse simple CASE expression', () => {
      const select = parseSql('SELECT CASE status WHEN 1 THEN \'active\' WHEN 0 THEN \'inactive\' END FROM users')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'case',
            caseExpr: { type: 'identifier', name: 'status' },
            whenClauses: [
              {
                condition: { type: 'literal', value: 1 },
                result: { type: 'literal', value: 'active' },
              },
              {
                condition: { type: 'literal', value: 0 },
                result: { type: 'literal', value: 'inactive' },
              },
            ],
          },
        },
      ])
    })

    it('should parse CASE expression with alias', () => {
      const select = parseSql('SELECT CASE WHEN age >= 18 THEN \'adult\' ELSE \'minor\' END AS age_group FROM users')
      expect(select.columns[0].alias).toBe('age_group')
      expect(select.columns[0].kind).toBe('derived')
    })
  })

})
