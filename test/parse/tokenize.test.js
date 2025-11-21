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

  it('should tokenize string literals with single quotes', () => {
    const tokens = tokenize('\'hello world\'')
    expect(tokens[0]).toMatchObject({ type: 'string', value: 'hello world' })
  })

  it('should tokenize string literals with double quotes', () => {
    const tokens = tokenize('"test string"')
    expect(tokens[0]).toMatchObject({ type: 'string', value: 'test string' })
  })

  it('should handle escaped quotes in strings', () => {
    const tokens = tokenize('\'can\'\'t\'')
    expect(tokens[0]).toMatchObject({ type: 'string', value: 'can\'t' })
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

  it('should throw error on unexpected character', () => {
    expect(() => tokenize('@invalid')).toThrow('Unexpected character')
  })
})
