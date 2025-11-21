import { describe, expect, it } from 'vitest'
import { parseSql } from '../src/parse.js'

describe('parseSql', () => {
  describe('basic SELECT queries', () => {
    it('should parse SELECT *', () => {
      const ast = parseSql('SELECT * FROM users')
      expect(ast).toMatchObject({
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
      const ast = parseSql('SELECT name FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'column', column: 'name' },
      ])
    })

    it('should parse SELECT with multiple columns', () => {
      const ast = parseSql('SELECT name, age, email FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'column', column: 'name' },
        { kind: 'column', column: 'age' },
        { kind: 'column', column: 'email' },
      ])
    })

    it('should parse SELECT DISTINCT', () => {
      const ast = parseSql('SELECT DISTINCT city FROM users')
      expect(ast.distinct).toBe(true)
    })

    it('should handle trailing semicolon', () => {
      const ast = parseSql('SELECT * FROM users;')
      expect(ast.from).toBe('users')
    })
  })

  describe('column aliases', () => {
    it('should parse column alias with AS', () => {
      const ast = parseSql('SELECT name AS full_name FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'column', column: 'name', alias: 'full_name' },
      ])
    })

    it('should parse column alias without AS', () => {
      const ast = parseSql('SELECT name full_name FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'column', column: 'name', alias: 'full_name' },
      ])
    })

    it('should not treat FROM as implicit alias', () => {
      const ast = parseSql('SELECT name FROM users')
      expect(ast.columns[0].alias).toBeUndefined()
      expect(ast.from).toBe('users')
    })
  })

  describe('aggregate functions', () => {
    it('should parse COUNT(*)', () => {
      const ast = parseSql('SELECT COUNT(*) FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' } },
      ])
    })

    it('should parse COUNT with column', () => {
      const ast = parseSql('SELECT COUNT(id) FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'column', column: 'id' } },
      ])
    })

    it('should parse SUM', () => {
      const ast = parseSql('SELECT SUM(amount) FROM transactions')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'SUM', arg: { kind: 'column', column: 'amount' } },
      ])
    })

    it('should parse AVG', () => {
      const ast = parseSql('SELECT AVG(score) FROM tests')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'AVG', arg: { kind: 'column', column: 'score' } },
      ])
    })

    it('should parse MIN and MAX', () => {
      const ast = parseSql('SELECT MIN(price), MAX(price) FROM products')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'MIN', arg: { kind: 'column', column: 'price' } },
        { kind: 'aggregate', func: 'MAX', arg: { kind: 'column', column: 'price' } },
      ])
    })

    it('should parse aggregate with alias', () => {
      const ast = parseSql('SELECT COUNT(*) AS total FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' }, alias: 'total' },
      ])
    })
  })

  describe('string functions', () => {
    it('should parse UPPER function', () => {
      const ast = parseSql('SELECT UPPER(name) FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'name' }] },
      ])
    })

    it('should parse LOWER function', () => {
      const ast = parseSql('SELECT LOWER(email) FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'function', func: 'LOWER', args: [{ type: 'identifier', name: 'email' }] },
      ])
    })

    it('should parse LENGTH function', () => {
      const ast = parseSql('SELECT LENGTH(name) FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'function', func: 'LENGTH', args: [{ type: 'identifier', name: 'name' }] },
      ])
    })

    it('should parse TRIM function', () => {
      const ast = parseSql('SELECT TRIM(name) FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'function', func: 'TRIM', args: [{ type: 'identifier', name: 'name' }] },
      ])
    })

    it('should parse CONCAT function with two arguments', () => {
      const ast = parseSql('SELECT CONCAT(first_name, last_name) FROM users')
      expect(ast.columns).toMatchObject([
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
      const ast = parseSql('SELECT CONCAT(first_name, \' \', last_name) FROM users')
      expect(ast.columns).toMatchObject([
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
      const ast = parseSql('SELECT SUBSTRING(name, 1, 3) FROM users')
      expect(ast.columns).toMatchObject([
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
      const ast = parseSql('SELECT UPPER(name) AS upper_name FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'name' }], alias: 'upper_name' },
      ])
    })

    it('should parse string function with implicit alias', () => {
      const ast = parseSql('SELECT LOWER(email) user_email FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'function', func: 'LOWER', args: [{ type: 'identifier', name: 'email' }], alias: 'user_email' },
      ])
    })

    it('should parse multiple string functions', () => {
      const ast = parseSql('SELECT UPPER(first_name), LOWER(last_name), LENGTH(email) FROM users')
      expect(ast.columns).toHaveLength(3)
      expect(ast.columns).toMatchObject([
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'first_name' }] },
        { kind: 'function', func: 'LOWER', args: [{ type: 'identifier', name: 'last_name' }] },
        { kind: 'function', func: 'LENGTH', args: [{ type: 'identifier', name: 'email' }] },
      ])
    })

    it('should parse string function with qualified column name', () => {
      const ast = parseSql('SELECT UPPER(users.name) FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'users.name' }] },
      ])
    })

    it('should parse mix of string functions and regular columns', () => {
      const ast = parseSql('SELECT id, UPPER(name), email FROM users')
      expect(ast.columns).toHaveLength(3)
      expect(ast.columns).toMatchObject([
        { kind: 'column', column: 'id' },
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'name' }] },
        { kind: 'column', column: 'email' },
      ])
    })

    it('should parse string functions with aggregate functions', () => {
      const ast = parseSql('SELECT UPPER(city), COUNT(*) FROM users GROUP BY city')
      expect(ast.columns).toMatchObject([
        { kind: 'function', func: 'UPPER', args: [{ type: 'identifier', name: 'city' }] },
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' } },
      ])
    })
  })

  describe('WHERE clause', () => {
    it('should parse WHERE with equality', () => {
      const ast = parseSql('SELECT * FROM users WHERE age = 25')
      expect(ast.where).toMatchObject({
        type: 'binary',
        op: '=',
        left: { type: 'identifier', name: 'age' },
        right: { type: 'literal', value: 25 },
      })
    })

    it('should parse WHERE with string literal', () => {
      const ast = parseSql('SELECT * FROM users WHERE name = \'John\'')
      expect(ast.where).toMatchObject({
        type: 'binary',
        op: '=',
        left: { type: 'identifier', name: 'name' },
        right: { type: 'literal', value: 'John' },
      })
    })

    it('should parse WHERE with comparison operators', () => {
      const ast = parseSql('SELECT * FROM users WHERE age > 18')
      expect(ast.where?.type).toBe('binary')
      if (ast.where?.type === 'binary') {
        expect(ast.where.op).toBe('>')
      }
    })

    it('should parse WHERE with AND', () => {
      const ast = parseSql('SELECT * FROM users WHERE age > 18 AND city = "NYC"')
      expect(ast.where).toMatchObject({
        type: 'binary',
        op: 'AND',
      })
    })

    it('should parse WHERE with OR', () => {
      const ast = parseSql('SELECT * FROM users WHERE age < 18 OR age > 65')
      expect(ast.where).toMatchObject({
        type: 'binary',
        op: 'OR',
      })
    })

    it('should parse WHERE with NOT', () => {
      const ast = parseSql('SELECT * FROM users WHERE NOT active')
      expect(ast.where).toMatchObject({
        type: 'unary',
        op: 'NOT',
        argument: { type: 'identifier', name: 'active' },
      })
    })

    it('should parse WHERE with parentheses', () => {
      const ast = parseSql('SELECT * FROM users WHERE (age > 18 AND age < 65)')
      expect(ast.where).toMatchObject({
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
      const ast = parseSql('SELECT * FROM users WHERE email = NULL')
      expect(ast.where?.type).toBe('binary')
      if (ast.where?.type === 'binary') {
        expect(ast.where.right).toMatchObject({ type: 'literal', value: null })
      }
    })

    it('should parse WHERE with IS NULL', () => {
      const ast = parseSql('SELECT * FROM users WHERE email IS NULL')
      expect(ast.where).toMatchObject({
        type: 'unary',
        op: 'IS NULL',
        argument: { type: 'identifier', name: 'email' },
      })
    })

    it('should parse WHERE with IS NOT NULL', () => {
      const ast = parseSql('SELECT * FROM users WHERE email IS NOT NULL')
      expect(ast.where).toMatchObject({
        type: 'unary',
        op: 'IS NOT NULL',
        argument: { type: 'identifier', name: 'email' },
      })
    })

    it('should parse WHERE with IS NULL in complex expression', () => {
      const ast = parseSql('SELECT * FROM users WHERE email IS NULL AND age > 18')
      expect(ast.where).toMatchObject({
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
      const ast = parseSql('SELECT * FROM users WHERE email IS NOT NULL OR phone IS NOT NULL')
      expect(ast.where).toMatchObject({
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
      const ast = parseSql('SELECT * FROM users WHERE name LIKE \'John%\'')
      expect(ast.where).toMatchObject({
        type: 'binary',
        op: 'LIKE',
        left: { type: 'identifier', name: 'name' },
        right: { type: 'literal', value: 'John%' },
      })
    })
  })

  describe('GROUP BY clause', () => {
    it('should parse GROUP BY with single column', () => {
      const ast = parseSql('SELECT city, COUNT(*) FROM users GROUP BY city')
      expect(ast.groupBy).toEqual([{ type: 'identifier', name: 'city' }])
    })

    it('should parse GROUP BY with multiple columns', () => {
      const ast = parseSql('SELECT city, state, COUNT(*) FROM users GROUP BY city, state')
      expect(ast.groupBy).toEqual([
        { type: 'identifier', name: 'city' },
        { type: 'identifier', name: 'state' },
      ])
    })
  })

  describe('ORDER BY clause', () => {
    it('should parse ORDER BY with default ASC', () => {
      const ast = parseSql('SELECT * FROM users ORDER BY name')
      expect(ast.orderBy).toMatchObject([
        { expr: { type: 'identifier', name: 'name' }, direction: 'ASC' },
      ])
    })

    it('should parse ORDER BY with explicit ASC', () => {
      const ast = parseSql('SELECT * FROM users ORDER BY name ASC')
      expect(ast.orderBy).toMatchObject([
        { expr: { type: 'identifier', name: 'name' }, direction: 'ASC' },
      ])
    })

    it('should parse ORDER BY with DESC', () => {
      const ast = parseSql('SELECT * FROM users ORDER BY age DESC')
      expect(ast.orderBy).toMatchObject([
        { expr: { type: 'identifier', name: 'age' }, direction: 'DESC' },
      ])
    })

    it('should parse ORDER BY with multiple columns', () => {
      const ast = parseSql('SELECT * FROM users ORDER BY city ASC, age DESC')
      expect(ast.orderBy).toMatchObject([
        { expr: { type: 'identifier', name: 'city' }, direction: 'ASC' },
        { expr: { type: 'identifier', name: 'age' }, direction: 'DESC' },
      ])
    })
  })

  describe('LIMIT and OFFSET', () => {
    it('should parse LIMIT', () => {
      const ast = parseSql('SELECT * FROM users LIMIT 10')
      expect(ast.limit).toBe(10)
      expect(ast.offset).toBeUndefined()
    })

    it('should parse LIMIT with OFFSET', () => {
      const ast = parseSql('SELECT * FROM users LIMIT 10 OFFSET 20')
      expect(ast.limit).toBe(10)
      expect(ast.offset).toBe(20)
    })

    it('should parse OFFSET without LIMIT', () => {
      const ast = parseSql('SELECT * FROM users OFFSET 20')
      expect(ast.limit).toBeUndefined()
      expect(ast.offset).toBe(20)
    })
  })

  describe('complex queries', () => {
    it('should parse query with all clauses', () => {
      const ast = parseSql(`
        SELECT DISTINCT city, COUNT(*) AS total
        FROM users
        WHERE age > 18
        GROUP BY city
        ORDER BY total DESC
        LIMIT 5
        OFFSET 10
      `)
      expect(ast).toMatchObject({
        distinct: true,
        from: 'users',
        groupBy: [{ type: 'identifier', name: 'city' }],
        limit: 5,
        offset: 10,
      })
      expect(ast.columns).toHaveLength(2)
      expect(ast.orderBy).toHaveLength(1)
      expect(ast.where).toBeTruthy()
    })

    it('should parse query with complex WHERE expression', () => {
      const ast = parseSql(`
        SELECT * FROM users
        WHERE (age > 18 AND age < 65) OR status = 'admin'
      `)
      expect(ast.where?.type).toBe('binary')
      if (ast.where?.type === 'binary') {
        expect(ast.where.op).toBe('OR')
      }
    })
  })

  describe('JOIN queries', () => {
    it('should parse simple INNER JOIN', () => {
      const ast = parseSql('SELECT * FROM users JOIN orders ON users.id = orders.user_id')
      expect(ast.from).toBe('users')
      expect(ast.joins).toHaveLength(1)
      expect(ast.joins[0]).toMatchObject({
        type: 'INNER',
        table: 'orders',
      })
      expect(ast.joins[0].on).toBeTruthy()
    })

    it('should parse explicit INNER JOIN', () => {
      const ast = parseSql('SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id')
      expect(ast.joins[0].type).toBe('INNER')
    })

    it('should parse LEFT JOIN', () => {
      const ast = parseSql('SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id')
      expect(ast.joins[0]).toMatchObject({
        type: 'LEFT',
        table: 'orders',
      })
    })

    it('should parse LEFT OUTER JOIN', () => {
      const ast = parseSql('SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id')
      expect(ast.joins[0].type).toBe('LEFT')
    })

    it('should parse RIGHT JOIN', () => {
      const ast = parseSql('SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id')
      expect(ast.joins[0].type).toBe('RIGHT')
    })

    it('should parse FULL JOIN', () => {
      const ast = parseSql('SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id')
      expect(ast.joins[0].type).toBe('FULL')
    })

    it('should parse multiple JOINs', () => {
      const ast = parseSql('SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id')
      expect(ast.joins).toHaveLength(2)
      expect(ast.joins[0].table).toBe('orders')
      expect(ast.joins[1].table).toBe('products')
    })

    it('should parse JOIN with WHERE clause', () => {
      const ast = parseSql('SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE orders.total > 100')
      expect(ast.joins).toHaveLength(1)
      expect(ast.where).toBeTruthy()
    })

    it('should parse qualified column names in WHERE', () => {
      const ast = parseSql('SELECT * FROM users WHERE users.age > 18')
      expect(ast.where).toMatchObject({
        type: 'binary',
        op: '>',
        left: { type: 'identifier', name: 'users.age' },
        right: { type: 'literal', value: 18 },
      })
    })
  })

  describe('error cases', () => {
    it('should throw error on missing FROM', () => {
      expect(() => parseSql('SELECT name')).toThrow()
    })

    it('should throw error on unexpected token', () => {
      expect(() => parseSql('SELECT * FROM users WHERE')).toThrow()
    })

    it('should throw error on invalid LIMIT', () => {
      expect(() => parseSql('SELECT * FROM users LIMIT abc')).toThrow('Expected numeric LIMIT')
    })

    it('should throw error on invalid OFFSET', () => {
      expect(() => parseSql('SELECT * FROM users OFFSET xyz')).toThrow('Expected numeric OFFSET')
    })

    it('should throw error on unexpected tokens after query', () => {
      expect(() => parseSql('SELECT * FROM users; SELECT')).toThrow('Unexpected tokens after end of query')
    })

    it('should throw error on missing closing paren', () => {
      expect(() => parseSql('SELECT * FROM users WHERE (age > 18')).toThrow()
    })
  })
})
