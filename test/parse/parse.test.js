import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql', () => {
  describe('basic SELECT queries', () => {
    it('should parse SELECT *', () => {
      const select = parseSql('SELECT * FROM users')
      expect(select).toEqual({
        distinct: false,
        columns: [{ kind: 'star', alias: undefined }],
        from: 'users',
        joins: [],
        where: undefined,
        groupBy: [],
        having: undefined,
        orderBy: [],
        limit: undefined,
        offset: undefined,
      })
    })

    it('should parse SELECT with qualified asterisk', () => {
      const select = parseSql('SELECT users.* FROM users')
      expect(select).toEqual({
        distinct: false,
        columns: [{ kind: 'star', table: 'users', alias: undefined }],
        from: 'users',
        joins: [],
        where: undefined,
        groupBy: [],
        having: undefined,
        orderBy: [],
        limit: undefined,
        offset: undefined,
      })
    })

    it('should parse SELECT with single column', () => {
      const select = parseSql('SELECT name FROM users')
      expect(select.columns).toEqual([
        { kind: 'column', column: 'name', alias: undefined },
      ])
    })

    it('should parse SELECT with multiple columns', () => {
      const select = parseSql('SELECT name, age, email FROM users')
      expect(select.columns).toEqual([
        { kind: 'column', column: 'name', alias: undefined },
        { kind: 'column', column: 'age', alias: undefined },
        { kind: 'column', column: 'email', alias: undefined },
      ])
    })

    it('should parse SELECT DISTINCT', () => {
      const select = parseSql('SELECT DISTINCT city FROM users')
      expect(select.distinct).toBe(true)
    })

    it('should handle trailing semicolon', () => {
      const select = parseSql('SELECT * FROM users;')
      expect(select.from).toBe('users')
    })

    it('should parse SELECT with negative number', () => {
      const select = parseSql('SELECT -age as neg_age FROM users')
      expect(select.columns).toEqual([
        {
          kind: 'operation',
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
        { kind: 'column', column: 'name', alias: 'full_name' },
      ])
    })

    it('should parse column alias without AS', () => {
      const select = parseSql('SELECT name full_name FROM users')
      expect(select.columns).toEqual([
        { kind: 'column', column: 'name', alias: 'full_name' },
      ])
    })

    it('should not treat FROM as implicit alias', () => {
      const select = parseSql('SELECT name FROM users')
      expect(select.columns[0].alias).toBeUndefined()
      expect(select.from).toBe('users')
    })
  })

  describe('quoted identifiers', () => {
    it('should parse column names with spaces using double quotes', () => {
      const select = parseSql('SELECT "first name", "last name" FROM users')
      expect(select.columns).toEqual([
        { kind: 'column', column: 'first name', alias: undefined },
        { kind: 'column', column: 'last name', alias: undefined },
      ])
    })

    it('should parse quoted table names with spaces', () => {
      const select = parseSql('SELECT * FROM "user data"')
      expect(select.from).toBe('user data')
    })

    it('should parse quoted column with alias', () => {
      const select = parseSql('SELECT "first name" AS fname FROM users')
      expect(select.columns).toEqual([
        { kind: 'column', column: 'first name', alias: 'fname' },
      ])
    })

    it('should parse mixed quoted and unquoted identifiers', () => {
      const select = parseSql('SELECT id, "full name", email FROM users')
      expect(select.columns).toEqual([
        { kind: 'column', column: 'id', alias: undefined },
        { kind: 'column', column: 'full name', alias: undefined },
        { kind: 'column', column: 'email', alias: undefined },
      ])
    })
  })

  describe('aggregate functions', () => {
    it('should parse COUNT(*)', () => {
      const select = parseSql('SELECT COUNT(*) FROM users')
      expect(select.columns).toEqual([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' }, alias: undefined },
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
          alias: undefined,
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
          alias: undefined,
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
          alias: undefined,
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
          alias: undefined,
        },
        {
          kind: 'aggregate',
          func: 'MAX',
          arg: { kind: 'expression', expr: { type: 'identifier', name: 'price' } },
          alias: undefined,
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

  describe('ORDER BY clause', () => {
    it('should parse ORDER BY with default ASC', () => {
      const select = parseSql('SELECT * FROM users ORDER BY name')
      expect(select.orderBy).toEqual([
        { expr: { type: 'identifier', name: 'name' }, direction: 'ASC' },
      ])
    })

    it('should parse ORDER BY with explicit ASC', () => {
      const select = parseSql('SELECT * FROM users ORDER BY name ASC')
      expect(select.orderBy).toEqual([
        { expr: { type: 'identifier', name: 'name' }, direction: 'ASC' },
      ])
    })

    it('should parse ORDER BY with DESC', () => {
      const select = parseSql('SELECT * FROM users ORDER BY age DESC')
      expect(select.orderBy).toEqual([
        { expr: { type: 'identifier', name: 'age' }, direction: 'DESC' },
      ])
    })

    it('should parse ORDER BY with multiple columns', () => {
      const select = parseSql('SELECT * FROM users ORDER BY city ASC, age DESC')
      expect(select.orderBy).toEqual([
        { expr: { type: 'identifier', name: 'city' }, direction: 'ASC' },
        { expr: { type: 'identifier', name: 'age' }, direction: 'DESC' },
      ])
    })

    it('should parse ORDER BY with CAST expression', () => {
      const select = parseSql('SELECT * FROM table ORDER BY CAST(size AS INTEGER)')
      expect(select.orderBy).toEqual([
        {
          expr: {
            type: 'cast',
            expr: { type: 'identifier', name: 'size' },
            toType: 'INTEGER',
          },
          direction: 'ASC',
        },
      ])
    })

    it('should parse ORDER BY with NULLS FIRST', () => {
      const select = parseSql('SELECT * FROM users ORDER BY name NULLS FIRST')
      expect(select.orderBy).toEqual([
        {
          expr: { type: 'identifier', name: 'name' },
          direction: 'ASC',
          nulls: 'FIRST',
        },
      ])
    })

    it('should parse ORDER BY with NULLS LAST', () => {
      const select = parseSql('SELECT * FROM users ORDER BY age DESC NULLS LAST')
      expect(select.orderBy).toEqual([
        {
          expr: { type: 'identifier', name: 'age' },
          direction: 'DESC',
          nulls: 'LAST',
        },
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
          { kind: 'column', column: 'city', alias: undefined },
          { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' }, alias: 'total' },
        ],
        from: 'users',
        joins: [],
        where: {
          type: 'binary',
          op: '>',
          left: { type: 'identifier', name: 'age' },
          right: { type: 'literal', value: 18 },
        },
        groupBy: [{ type: 'identifier', name: 'city' }],
        having: undefined,
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
          kind: 'operation',
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
        { kind: 'column', column: 'name', alias: undefined },
      ])
      expect(select.from).toMatchObject({
        kind: 'subquery',
        query: {
          columns: [{ kind: 'star' }],
          from: 'users',
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

})
