import { describe, expect, it } from 'vitest'
import { tokenize } from '../../src/parse/tokenize.js'

describe('tokenize', () => {
  it('should tokenize simple SELECT query', () => {
    const tokens = tokenize('SELECT name FROM users')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'identifier', value: 'name' },
      { type: 'keyword', value: 'FROM' },
      { type: 'identifier', value: 'users' },
      { type: 'eof' },
    ])
  })

  it('should tokenize numbers', () => {
    const tokens = tokenize('123 45.67 1.2e10 3E-5')
    expect(tokens[0]).toMatchObject({ type: 'number', value: '123', numericValue: 123 })
    expect(tokens[1]).toMatchObject({ type: 'number', value: '45.67', numericValue: 45.67 })
    expect(tokens[2]).toMatchObject({ type: 'number', value: '1.2e10', numericValue: 1.2e10 })
    expect(tokens[3]).toMatchObject({ type: 'number', value: '3E-5', numericValue: 3e-5 })
  })

  it('should tokenize negative numbers and expressions', () => {
    expect(tokenize('-42')).toMatchObject([
      { type: 'operator', value: '-' },
      { type: 'number', value: '42', numericValue: 42 },
      { type: 'eof' },
    ])
    expect(tokenize('x - 42')).toMatchObject([
      { type: 'identifier', value: 'x' },
      { type: 'operator', value: '-' },
      { type: 'number', value: '42', numericValue: 42 },
      { type: 'eof' },
    ])
    expect(tokenize('- x')).toMatchObject([
      { type: 'operator', value: '-' },
      { type: 'identifier', value: 'x' },
      { type: 'eof' },
    ])
  })

  it('should tokenize string literals with single quotes', () => {
    const tokens = tokenize('\'hello world\'')
    expect(tokens[0]).toMatchObject({ type: 'string', value: 'hello world' })
  })

  it('should tokenize identifier with double quotes', () => {
    const tokens = tokenize('"test string"')
    expect(tokens[0]).toMatchObject({ type: 'identifier', value: 'test string' })
  })

  it('should handle escaped single quotes in string literals', () => {
    const tokens = tokenize('\'can\'\'t\'')
    expect(tokens[0]).toMatchObject({ type: 'string', value: 'can\'t' })
  })

  it('should handle double quotes in string literals', () => {
    const tokens = tokenize('\'first "middle" "" last\'')
    expect(tokens[0]).toMatchObject({ type: 'string', value: 'first "middle" "" last' })
  })

  it('should handle escaped double quotes in identifiers', () => {
    const tokens = tokenize('"double""quote"')
    expect(tokens[0]).toMatchObject({ type: 'identifier', value: 'double"quote' })
  })

  it('should handle single quotes in identifiers', () => {
    const tokens = tokenize('"it\'s an \'\' identifier"')
    expect(tokens[0]).toMatchObject({ type: 'identifier', value: 'it\'s an \'\' identifier' })
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
      { type: 'eof' },
    ])
  })

  it('should skip line comments', () => {
    const tokens = tokenize('SELECT -- this is a comment\nname')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'identifier', value: 'name' },
      { type: 'eof' },
    ])
  })

  it('should skip block comments', () => {
    const tokens = tokenize('SELECT /* comment */ name')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'identifier', value: 'name' },
      { type: 'eof' },
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
      { type: 'eof' },
    ])
  })

  it('should tokenize dot notation', () => {
    const tokens = tokenize('table.column')
    expect(tokens).toMatchObject([
      { type: 'identifier', value: 'table' },
      { type: 'dot', value: '.' },
      { type: 'identifier', value: 'column' },
      { type: 'eof' },
    ])
  })

  it('should handle special characters in string literals', () => {
    const tokens = tokenize('\'line1\nline2\tend\'')
    expect(tokens[0]).toMatchObject({ type: 'string', value: 'line1\nline2\tend' })
  })

  it('should tokenize CAST', () => {
    const tokens = tokenize('CAST(value AS INTEGER)')
    expect(tokens).toMatchObject([
      { type: 'identifier', value: 'CAST' },
      { type: 'paren', value: '(' },
      { type: 'identifier', value: 'value' },
      { type: 'keyword', value: 'AS' },
      { type: 'identifier', value: 'INTEGER' },
      { type: 'paren', value: ')' },
      { type: 'eof' },
    ])
  })

  it('should tokenize HAVING clause', () => {
    const tokens = tokenize('SELECT city, COUNT(*) FROM users GROUP BY city HAVING COUNT(*) > 5')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'identifier', value: 'city' },
      { type: 'comma', value: ',' },
      { type: 'identifier', value: 'COUNT' },
      { type: 'paren', value: '(' },
      { type: 'operator', value: '*' },
      { type: 'paren', value: ')' },
      { type: 'keyword', value: 'FROM' },
      { type: 'identifier', value: 'users' },
      { type: 'keyword', value: 'GROUP' },
      { type: 'keyword', value: 'BY' },
      { type: 'identifier', value: 'city' },
      { type: 'keyword', value: 'HAVING' },
      { type: 'identifier', value: 'COUNT' },
      { type: 'paren', value: '(' },
      { type: 'operator', value: '*' },
      { type: 'paren', value: ')' },
      { type: 'operator', value: '>' },
      { type: 'number', value: '5', numericValue: 5 },
      { type: 'eof' },
    ])
  })

  it('should tokenize BETWEEN clause', () => {
    const tokens = tokenize('SELECT * FROM users WHERE age BETWEEN 18 AND 65')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'operator', value: '*' },
      { type: 'keyword', value: 'FROM' },
      { type: 'identifier', value: 'users' },
      { type: 'keyword', value: 'WHERE' },
      { type: 'identifier', value: 'age' },
      { type: 'keyword', value: 'BETWEEN' },
      { type: 'number', value: '18', numericValue: 18 },
      { type: 'keyword', value: 'AND' },
      { type: 'number', value: '65', numericValue: 65 },
      { type: 'eof' },
    ])
  })

  it('should throw error on unterminated string literal', () => {
    expect(() => tokenize('\'unterminated string')).toThrow('Unterminated string literal starting at position 0')
  })

  it('should throw error on unterminated identifier', () => {
    expect(() => tokenize('"unterminated identifier')).toThrow('Unterminated identifier starting at position 0')
  })

  it('should throw on backticks', () => {
    expect(() => tokenize('`backtick`')).toThrow('Expected SELECT but found "`" at position 0')
    expect(() => tokenize('SELECT `backtick` FROM table')).toThrow('Unexpected character "`" at position 7')
  })

  it('should throw error on invalid number', () => {
    expect(() => tokenize('12.34n')).toThrow('Invalid number at position 0: 12.34n')
  })

  it('should throw error on unexpected character', () => {
    expect(() => tokenize('@invalid')).toThrow('Expected SELECT but found "@" at position 0')
    expect(() => tokenize(' #invalid')).toThrow('Expected SELECT but found "#" at position 1')
  })

  it('should tokenize subquery in FROM clause', () => {
    const tokens = tokenize('SELECT name FROM (SELECT * FROM users WHERE active = 1) AS u')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'identifier', value: 'name' },
      { type: 'keyword', value: 'FROM' },
      { type: 'paren', value: '(' },
      { type: 'keyword', value: 'SELECT' },
      { type: 'operator', value: '*' },
      { type: 'keyword', value: 'FROM' },
      { type: 'identifier', value: 'users' },
      { type: 'keyword', value: 'WHERE' },
      { type: 'identifier', value: 'active' },
      { type: 'operator', value: '=' },
      { type: 'number', value: '1', numericValue: 1 },
      { type: 'paren', value: ')' },
      { type: 'keyword', value: 'AS' },
      { type: 'identifier', value: 'u' },
      { type: 'eof' },
    ])
  })

  it('should tokenize IN with subquery in WHERE clause', () => {
    const tokens = tokenize('SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = 1)')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'operator', value: '*' },
      { type: 'keyword', value: 'FROM' },
      { type: 'identifier', value: 'orders' },
      { type: 'keyword', value: 'WHERE' },
      { type: 'identifier', value: 'user_id' },
      { type: 'keyword', value: 'IN' },
      { type: 'paren', value: '(' },
      { type: 'keyword', value: 'SELECT' },
      { type: 'identifier', value: 'id' },
      { type: 'keyword', value: 'FROM' },
      { type: 'identifier', value: 'users' },
      { type: 'keyword', value: 'WHERE' },
      { type: 'identifier', value: 'active' },
      { type: 'operator', value: '=' },
      { type: 'number', value: '1', numericValue: 1 },
      { type: 'paren', value: ')' },
      { type: 'eof' },
    ])
  })

  it('should tokenize EXISTS with subquery in WHERE clause', () => {
    const tokens = tokenize('SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM users WHERE users.id = orders.user_id)')
    expect(tokens).toMatchObject([
      { type: 'keyword', value: 'SELECT' },
      { type: 'operator', value: '*' },
      { type: 'keyword', value: 'FROM' },
      { type: 'identifier', value: 'orders' },
      { type: 'keyword', value: 'WHERE' },
      { type: 'keyword', value: 'EXISTS' },
      { type: 'paren', value: '(' },
      { type: 'keyword', value: 'SELECT' },
      { type: 'number', value: '1', numericValue: 1 },
      { type: 'keyword', value: 'FROM' },
      { type: 'identifier', value: 'users' },
      { type: 'keyword', value: 'WHERE' },
      { type: 'identifier', value: 'users' },
      { type: 'dot', value: '.' },
      { type: 'identifier', value: 'id' },
      { type: 'operator', value: '=' },
      { type: 'identifier', value: 'orders' },
      { type: 'dot', value: '.' },
      { type: 'identifier', value: 'user_id' },
      { type: 'paren', value: ')' },
      { type: 'eof' },
    ])
  })
})
