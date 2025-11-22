import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql', () => {
  describe('basic SELECT queries', () => {
    it('should parse SELECT *', () => {
      const select = parseSql('SELECT * FROM users')
      expect(select).toMatchObject({
        distinct: false,
        columns: [{ kind: 'star' }],
        from: 'users',
        where: undefined,
        groupBy: [],
        orderBy: [],
        limit: undefined,
        offset: undefined,
      })
    })

    it('should parse SELECT with single column', () => {
      const select = parseSql('SELECT name FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'column', column: 'name' },
      ])
    })

    it('should parse SELECT with multiple columns', () => {
      const select = parseSql('SELECT name, age, email FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'column', column: 'name' },
        { kind: 'column', column: 'age' },
        { kind: 'column', column: 'email' },
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
  })

  describe('column aliases', () => {
    it('should parse column alias with AS', () => {
      const select = parseSql('SELECT name AS full_name FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'column', column: 'name', alias: 'full_name' },
      ])
    })

    it('should parse column alias without AS', () => {
      const select = parseSql('SELECT name full_name FROM users')
      expect(select.columns).toMatchObject([
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
      expect(select.columns).toMatchObject([
        { kind: 'column', column: 'first name' },
        { kind: 'column', column: 'last name' },
      ])
    })

    it('should parse quoted table names with spaces', () => {
      const select = parseSql('SELECT * FROM "user data"')
      expect(select.from).toBe('user data')
    })

    it('should parse quoted column with alias', () => {
      const select = parseSql('SELECT "first name" AS fname FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'column', column: 'first name', alias: 'fname' },
      ])
    })

    it('should parse mixed quoted and unquoted identifiers', () => {
      const select = parseSql('SELECT id, "full name", email FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'column', column: 'id' },
        { kind: 'column', column: 'full name' },
        { kind: 'column', column: 'email' },
      ])
    })
  })

  describe('aggregate functions', () => {
    it('should parse COUNT(*)', () => {
      const select = parseSql('SELECT COUNT(*) FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' } },
      ])
    })

    it('should parse COUNT with column', () => {
      const select = parseSql('SELECT COUNT(id) FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'column', column: 'id' } },
      ])
    })

    it('should parse SUM', () => {
      const select = parseSql('SELECT SUM(amount) FROM transactions')
      expect(select.columns).toMatchObject([
        { kind: 'aggregate', func: 'SUM', arg: { kind: 'column', column: 'amount' } },
      ])
    })

    it('should parse AVG', () => {
      const select = parseSql('SELECT AVG(score) FROM tests')
      expect(select.columns).toMatchObject([
        { kind: 'aggregate', func: 'AVG', arg: { kind: 'column', column: 'score' } },
      ])
    })

    it('should parse MIN and MAX', () => {
      const select = parseSql('SELECT MIN(price), MAX(price) FROM products')
      expect(select.columns).toMatchObject([
        { kind: 'aggregate', func: 'MIN', arg: { kind: 'column', column: 'price' } },
        { kind: 'aggregate', func: 'MAX', arg: { kind: 'column', column: 'price' } },
      ])
    })

    it('should parse aggregate with alias', () => {
      const select = parseSql('SELECT COUNT(*) AS total FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' }, alias: 'total' },
      ])
    })
  })

  describe('string functions', () => {
    it('should parse UPPER function', () => {
      const select = parseSql('SELECT UPPER(name) FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'name' }] },
      ])
    })

    it('should parse LOWER function', () => {
      const select = parseSql('SELECT LOWER(email) FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'function', func: 'LOWER', args: [{ type: 'identifier', name: 'email' }] },
      ])
    })

    it('should parse LENGTH function', () => {
      const select = parseSql('SELECT LENGTH(name) FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'function', func: 'LENGTH', args: [{ type: 'identifier', name: 'name' }] },
      ])
    })

    it('should parse TRIM function', () => {
      const select = parseSql('SELECT TRIM(name) FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'function', func: 'TRIM', args: [{ type: 'identifier', name: 'name' }] },
      ])
    })

    it('should parse CONCAT function with two arguments', () => {
      const select = parseSql('SELECT CONCAT(first_name, last_name) FROM users')
      expect(select.columns).toMatchObject([
        {
          kind: 'function',
          func: 'CONCAT',
          args: [
            { type: 'identifier', name: 'first_name' },
            { type: 'identifier', name: 'last_name' },
          ],
        },
      ])
    })

    it('should parse CONCAT function with string literals', () => {
      const select = parseSql('SELECT CONCAT(first_name, \' \', last_name) FROM users')
      expect(select.columns).toMatchObject([
        {
          kind: 'function',
          func: 'CONCAT',
          args: [
            { type: 'identifier', name: 'first_name' },
            { type: 'literal', value: ' ' },
            { type: 'identifier', name: 'last_name' },
          ],
        },
      ])
    })

    it('should parse SUBSTRING function with three arguments', () => {
      const select = parseSql('SELECT SUBSTRING(name, 1, 3) FROM users')
      expect(select.columns).toMatchObject([
        {
          kind: 'function',
          func: 'SUBSTRING',
          args: [
            { type: 'identifier', name: 'name' },
            { type: 'literal', value: 1 },
            { type: 'literal', value: 3 },
          ],
        },
      ])
    })

    it('should parse string function with alias using AS', () => {
      const select = parseSql('SELECT UPPER(name) AS upper_name FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'name' }], alias: 'upper_name' },
      ])
    })

    it('should parse string function with implicit alias', () => {
      const select = parseSql('SELECT LOWER(email) user_email FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'function', func: 'LOWER', args: [{ type: 'identifier', name: 'email' }], alias: 'user_email' },
      ])
    })

    it('should parse multiple string functions', () => {
      const select = parseSql('SELECT UPPER(first_name), LOWER(last_name), LENGTH(email) FROM users')
      expect(select.columns).toHaveLength(3)
      expect(select.columns).toMatchObject([
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'first_name' }] },
        { kind: 'function', func: 'LOWER', args: [{ type: 'identifier', name: 'last_name' }] },
        { kind: 'function', func: 'LENGTH', args: [{ type: 'identifier', name: 'email' }] },
      ])
    })

    it('should parse string function with qualified column name', () => {
      const select = parseSql('SELECT UPPER(users.name) FROM users')
      expect(select.columns).toMatchObject([
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'users.name' }] },
      ])
    })

    it('should parse mix of string functions and regular columns', () => {
      const select = parseSql('SELECT id, UPPER(name), email FROM users')
      expect(select.columns).toHaveLength(3)
      expect(select.columns).toMatchObject([
        { kind: 'column', column: 'id' },
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'name' }] },
        { kind: 'column', column: 'email' },
      ])
    })

    it('should parse string functions with aggregate functions', () => {
      const select = parseSql('SELECT UPPER(city), COUNT(*) FROM users GROUP BY city')
      expect(select.columns).toMatchObject([
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'city' }] },
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' } },
      ])
    })
  })

  describe('WHERE clause', () => {
    it('should parse WHERE with equality', () => {
      const select = parseSql('SELECT * FROM users WHERE age = 25')
      expect(select.where).toMatchObject({
        type: 'binary',
        op: '=',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 25 },
      })
    })

    it('should parse WHERE with string literal', () => {
      const select = parseSql('SELECT * FROM users WHERE name = \'John\'')
      expect(select.where).toMatchObject({
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
      expect(select.where).toMatchObject({
        type: 'binary',
        op: '>',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'unary', op: '-', argument: { type: 'literal', value: 18 } },
      })
    })

    it('should parse WHERE with AND', () => {
      const select = parseSql('SELECT * FROM users WHERE age > 18 AND city = "NYC"')
      expect(select.where).toMatchObject({
        type: 'binary',
        op: 'AND',
      })
    })

    it('should parse WHERE with OR', () => {
      const select = parseSql('SELECT * FROM users WHERE age < 18 OR age > 65')
      expect(select.where).toMatchObject({
        type: 'binary',
        op: 'OR',
      })
    })

    it('should parse WHERE with NOT', () => {
      const select = parseSql('SELECT * FROM users WHERE NOT active')
      expect(select.where).toMatchObject({
        type: 'unary',
        op: 'NOT',
        argument: { type: 'identifier', name: 'active' },
      })
    })

    it('should parse WHERE with parentheses', () => {
      const select = parseSql('SELECT * FROM users WHERE (age > 18 AND age < 65)')
      expect(select.where).toMatchObject({
        type: 'binary',
        op: 'AND',
      })
    })

    it('should parse WHERE with boolean literals', () => {
      const ast1 = parseSql('SELECT * FROM users WHERE active = TRUE')
      expect(ast1.where?.type).toBe('binary')
      if (ast1.where?.type === 'binary') {
        expect(ast1.where.right).toMatchObject({ type: 'literal', value: true })
      }

      const ast2 = parseSql('SELECT * FROM users WHERE deleted = FALSE')
      expect(ast2.where?.type).toBe('binary')
      if (ast2.where?.type === 'binary') {
        expect(ast2.where.right).toMatchObject({ type: 'literal', value: false })
      }
    })

    it('should parse WHERE with NULL', () => {
      const select = parseSql('SELECT * FROM users WHERE email = NULL')
      expect(select.where?.type).toBe('binary')
      if (select.where?.type === 'binary') {
        expect(select.where.right).toMatchObject({ type: 'literal', value: null })
      }
    })

    it('should parse WHERE with IS NULL', () => {
      const select = parseSql('SELECT * FROM users WHERE email IS NULL')
      expect(select.where).toMatchObject({
        type: 'unary',
        op: 'IS NULL',
        argument: { type: 'identifier', name: 'email' },
      })
    })

    it('should parse WHERE with IS NOT NULL', () => {
      const select = parseSql('SELECT * FROM users WHERE email IS NOT NULL')
      expect(select.where).toMatchObject({
        type: 'unary',
        op: 'IS NOT NULL',
        argument: { type: 'identifier', name: 'email' },
      })
    })

    it('should parse WHERE with IS NULL in complex expression', () => {
      const select = parseSql('SELECT * FROM users WHERE email IS NULL AND age > 18')
      expect(select.where).toMatchObject({
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
      expect(select.where).toMatchObject({
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
      expect(select.where).toMatchObject({
        type: 'binary',
        op: 'LIKE',
        left: { type: 'identifier', name: 'name' },
        right: { type: 'literal', value: 'John%' },
      })
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
      expect(select.orderBy).toMatchObject([
        { expr: { type: 'identifier', name: 'name' }, direction: 'ASC' },
      ])
    })

    it('should parse ORDER BY with explicit ASC', () => {
      const select = parseSql('SELECT * FROM users ORDER BY name ASC')
      expect(select.orderBy).toMatchObject([
        { expr: { type: 'identifier', name: 'name' }, direction: 'ASC' },
      ])
    })

    it('should parse ORDER BY with DESC', () => {
      const select = parseSql('SELECT * FROM users ORDER BY age DESC')
      expect(select.orderBy).toMatchObject([
        { expr: { type: 'identifier', name: 'age' }, direction: 'DESC' },
      ])
    })

    it('should parse ORDER BY with multiple columns', () => {
      const select = parseSql('SELECT * FROM users ORDER BY city ASC, age DESC')
      expect(select.orderBy).toMatchObject([
        { expr: { type: 'identifier', name: 'city' }, direction: 'ASC' },
        { expr: { type: 'identifier', name: 'age' }, direction: 'DESC' },
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
      expect(select).toMatchObject({
        distinct: true,
        from: 'users',
        groupBy: [{ type: 'identifier', name: 'city' }],
        limit: 5,
        offset: 10,
      })
      expect(select.columns).toHaveLength(2)
      expect(select.orderBy).toHaveLength(1)
      expect(select.where).toBeTruthy()
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
  })

  describe('JOIN queries', () => {
    it('should parse simple INNER JOIN', () => {
      const select = parseSql('SELECT * FROM users JOIN orders ON users.id = orders.user_id')
      expect(select.from).toBe('users')
      expect(select.joins).toHaveLength(1)
      expect(select.joins[0]).toMatchObject({
        type: 'INNER',
        table: 'orders',
      })
      expect(select.joins[0].on).toBeTruthy()
    })

    it('should parse explicit INNER JOIN', () => {
      const select = parseSql('SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id')
      expect(select.joins[0].type).toBe('INNER')
    })

    it('should parse LEFT JOIN', () => {
      const select = parseSql('SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id')
      expect(select.joins[0]).toMatchObject({
        type: 'LEFT',
        table: 'orders',
      })
    })

    it('should parse LEFT OUTER JOIN', () => {
      const select = parseSql('SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id')
      expect(select.joins[0].type).toBe('LEFT')
    })

    it('should parse RIGHT JOIN', () => {
      const select = parseSql('SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id')
      expect(select.joins[0].type).toBe('RIGHT')
    })

    it('should parse FULL JOIN', () => {
      const select = parseSql('SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id')
      expect(select.joins[0].type).toBe('FULL')
    })

    it('should parse multiple JOINs', () => {
      const select = parseSql('SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id')
      expect(select.joins).toHaveLength(2)
      expect(select.joins[0].table).toBe('orders')
      expect(select.joins[1].table).toBe('products')
    })

    it('should parse JOIN with WHERE clause', () => {
      const select = parseSql('SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE orders.total > 100')
      expect(select.joins).toHaveLength(1)
      expect(select.where).toBeTruthy()
    })

    it('should parse qualified column names in WHERE', () => {
      const select = parseSql('SELECT * FROM users WHERE users.age > 18')
      expect(select.where).toMatchObject({
        type: 'binary',
        op: '>',
        left: { type: 'identifier', name: 'users.age' },
        right: { type: 'literal', value: 18 },
      })
    })
  })
})
