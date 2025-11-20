import { describe, expect, it } from 'vitest'
import { tokenize, parseSql } from '../src/parse.js'

describe('tokenize', () => {
  it('should tokenize simple SELECT query', () => {
    const tokens = tokenize('SELECT name FROM users')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'identifier', value: 'name' },
      { type: 'keyword', value: 'FROM' },
      { type: 'identifier', value: 'users' },
      { type: 'eof' }
    ])
  })

  it('should tokenize numbers', () => {
    const tokens = tokenize('123 45.67 1.2e10 3E-5')
    expect(tokens[0]).toMatchObject({ type: 'number', value: '123', numericValue: 123 })
    expect(tokens[1]).toMatchObject({ type: 'number', value: '45.67', numericValue: 45.67 })
    expect(tokens[2]).toMatchObject({ type: 'number', value: '1.2e10', numericValue: 1.2e10 })
    expect(tokens[3]).toMatchObject({ type: 'number', value: '3E-5', numericValue: 3e-5 })
  })

  it('should tokenize string literals with single quotes', () => {
    const tokens = tokenize("'hello world'")
    expect(tokens[0]).toMatchObject({ type: 'string', value: 'hello world' })
  })

  it('should tokenize string literals with double quotes', () => {
    const tokens = tokenize('"test string"')
    expect(tokens[0]).toMatchObject({ type: 'string', value: 'test string' })
  })

  it('should handle escaped quotes in strings', () => {
    const tokens = tokenize("'can''t'")
    expect(tokens[0]).toMatchObject({ type: 'string', value: "can't" })
  })

  it('should tokenize operators', () => {
    const tokens = tokenize('= != <> < > <= >= + - * / %')
    expect(tokens.map(t => t.value)).toEqual(['=', '!=', '<>', '<', '>', '<=', '>=', '+', '-', '*', '/', '%', ''])
  })

  it('should skip whitespace', () => {
    const tokens = tokenize('  SELECT  \t\n  name  ')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'identifier', value: 'name' },
      { type: 'eof' }
    ])
  })

  it('should skip line comments', () => {
    const tokens = tokenize('SELECT -- this is a comment\nname')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'identifier', value: 'name' },
      { type: 'eof' }
    ])
  })

  it('should skip block comments', () => {
    const tokens = tokenize('SELECT /* comment */ name')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'identifier', value: 'name' },
      { type: 'eof' }
    ])
  })

  it('should tokenize special characters', () => {
    const tokens = tokenize('SELECT * FROM table, column ( ) ;')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'operator', value: '*' },
      { type: 'keyword', value: 'FROM' },
      { type: 'identifier', value: 'table' },
      { type: 'comma', value: ',' },
      { type: 'identifier', value: 'column' },
      { type: 'paren', value: '(' },
      { type: 'paren', value: ')' },
      { type: 'semicolon', value: ';' },
      { type: 'eof' }
    ])
  })

  it('should tokenize dot notation', () => {
    const tokens = tokenize('table.column')
    expect(tokens).toMatchObject([
      { type: 'identifier', value: 'table' },
      { type: 'dot', value: '.' },
      { type: 'identifier', value: 'column' },
      { type: 'eof' }
    ])
  })

  it('should throw error on unexpected character', () => {
    expect(() => tokenize('@invalid')).toThrow('Unexpected character')
  })
})

describe('parseSql', () => {
  describe('basic SELECT queries', () => {
    it('should parse SELECT *', () => {
      const ast = parseSql('SELECT * FROM users')
      expect(ast).toMatchObject({
        distinct: false,
        columns: [{ kind: 'star' }],
        from: 'users',
        where: null,
        groupBy: [],
        orderBy: [],
        limit: null,
        offset: null
      })
    })

    it('should parse SELECT with single column', () => {
      const ast = parseSql('SELECT name FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'column', column: 'name' }
      ])
    })

    it('should parse SELECT with multiple columns', () => {
      const ast = parseSql('SELECT name, age, email FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'column', column: 'name' },
        { kind: 'column', column: 'age' },
        { kind: 'column', column: 'email' }
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
        { kind: 'column', column: 'name', alias: 'full_name' }
      ])
    })

    it('should parse column alias without AS', () => {
      const ast = parseSql('SELECT name full_name FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'column', column: 'name', alias: 'full_name' }
      ])
    })

    it('should not treat FROM as implicit alias', () => {
      const ast = parseSql('SELECT name FROM users')
      expect(ast.columns[0].alias).toBeNull()
      expect(ast.from).toBe('users')
    })
  })

  describe('aggregate functions', () => {
    it('should parse COUNT(*)', () => {
      const ast = parseSql('SELECT COUNT(*) FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' } }
      ])
    })

    it('should parse COUNT with column', () => {
      const ast = parseSql('SELECT COUNT(id) FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'column', column: 'id' } }
      ])
    })

    it('should parse SUM', () => {
      const ast = parseSql('SELECT SUM(amount) FROM transactions')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'SUM', arg: { kind: 'column', column: 'amount' } }
      ])
    })

    it('should parse AVG', () => {
      const ast = parseSql('SELECT AVG(score) FROM tests')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'AVG', arg: { kind: 'column', column: 'score' } }
      ])
    })

    it('should parse MIN and MAX', () => {
      const ast = parseSql('SELECT MIN(price), MAX(price) FROM products')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'MIN', arg: { kind: 'column', column: 'price' } },
        { kind: 'aggregate', func: 'MAX', arg: { kind: 'column', column: 'price' } }
      ])
    })

    it('should parse aggregate with alias', () => {
      const ast = parseSql('SELECT COUNT(*) AS total FROM users')
      expect(ast.columns).toMatchObject([
        { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' }, alias: 'total' }
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
        right: { type: 'literal', value: 25 }
      })
    })

    it('should parse WHERE with string literal', () => {
      const ast = parseSql("SELECT * FROM users WHERE name = 'John'")
      expect(ast.where).toMatchObject({
        type: 'binary',
        op: '=',
        left: { type: 'identifier', name: 'name' },
        right: { type: 'literal', value: 'John' }
      })
    })

    it('should parse WHERE with comparison operators', () => {
      const ast = parseSql('SELECT * FROM users WHERE age > 18')
      expect(ast.where?.op).toBe('>')
    })

    it('should parse WHERE with AND', () => {
      const ast = parseSql('SELECT * FROM users WHERE age > 18 AND city = "NYC"')
      expect(ast.where).toMatchObject({
        type: 'binary',
        op: 'AND'
      })
    })

    it('should parse WHERE with OR', () => {
      const ast = parseSql('SELECT * FROM users WHERE age < 18 OR age > 65')
      expect(ast.where).toMatchObject({
        type: 'binary',
        op: 'OR'
      })
    })

    it('should parse WHERE with NOT', () => {
      const ast = parseSql('SELECT * FROM users WHERE NOT active')
      expect(ast.where).toMatchObject({
        type: 'unary',
        op: 'NOT',
        argument: { type: 'identifier', name: 'active' }
      })
    })

    it('should parse WHERE with parentheses', () => {
      const ast = parseSql('SELECT * FROM users WHERE (age > 18 AND age < 65)')
      expect(ast.where).toMatchObject({
        type: 'binary',
        op: 'AND'
      })
    })

    it('should parse WHERE with boolean literals', () => {
      const ast1 = parseSql('SELECT * FROM users WHERE active = TRUE')
      expect(ast1.where?.right).toMatchObject({ type: 'literal', value: true })

      const ast2 = parseSql('SELECT * FROM users WHERE deleted = FALSE')
      expect(ast2.where?.right).toMatchObject({ type: 'literal', value: false })
    })

    it('should parse WHERE with NULL', () => {
      const ast = parseSql('SELECT * FROM users WHERE email = NULL')
      expect(ast.where?.right).toMatchObject({ type: 'literal', value: null })
    })
  })

  describe('GROUP BY clause', () => {
    it('should parse GROUP BY with single column', () => {
      const ast = parseSql('SELECT city, COUNT(*) FROM users GROUP BY city')
      expect(ast.groupBy).toEqual(['city'])
    })

    it('should parse GROUP BY with multiple columns', () => {
      const ast = parseSql('SELECT city, state, COUNT(*) FROM users GROUP BY city, state')
      expect(ast.groupBy).toEqual(['city', 'state'])
    })
  })

  describe('ORDER BY clause', () => {
    it('should parse ORDER BY with default ASC', () => {
      const ast = parseSql('SELECT * FROM users ORDER BY name')
      expect(ast.orderBy).toMatchObject([
        { expr: 'name', direction: 'ASC' }
      ])
    })

    it('should parse ORDER BY with explicit ASC', () => {
      const ast = parseSql('SELECT * FROM users ORDER BY name ASC')
      expect(ast.orderBy).toMatchObject([
        { expr: 'name', direction: 'ASC' }
      ])
    })

    it('should parse ORDER BY with DESC', () => {
      const ast = parseSql('SELECT * FROM users ORDER BY age DESC')
      expect(ast.orderBy).toMatchObject([
        { expr: 'age', direction: 'DESC' }
      ])
    })

    it('should parse ORDER BY with multiple columns', () => {
      const ast = parseSql('SELECT * FROM users ORDER BY city ASC, age DESC')
      expect(ast.orderBy).toMatchObject([
        { expr: 'city', direction: 'ASC' },
        { expr: 'age', direction: 'DESC' }
      ])
    })
  })

  describe('LIMIT and OFFSET', () => {
    it('should parse LIMIT', () => {
      const ast = parseSql('SELECT * FROM users LIMIT 10')
      expect(ast.limit).toBe(10)
      expect(ast.offset).toBeNull()
    })

    it('should parse LIMIT with OFFSET', () => {
      const ast = parseSql('SELECT * FROM users LIMIT 10 OFFSET 20')
      expect(ast.limit).toBe(10)
      expect(ast.offset).toBe(20)
    })

    it('should parse OFFSET without LIMIT', () => {
      const ast = parseSql('SELECT * FROM users OFFSET 20')
      expect(ast.limit).toBeNull()
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
        groupBy: ['city'],
        limit: 5,
        offset: 10
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
      expect(ast.where?.op).toBe('OR')
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
