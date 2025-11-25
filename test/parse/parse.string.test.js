import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql - string functions', () => {
  it('should parse UPPER function', () => {
    const select = parseSql('SELECT UPPER(name) FROM users')
    expect(select.columns).toEqual([
      { kind: 'derived', expr: { type: 'function', name: 'UPPER', args: [{ type: 'identifier', name: 'name' }] }, alias: undefined },
    ])
  })

  it('should parse LOWER function', () => {
    const select = parseSql('SELECT LOWER(email) FROM users')
    expect(select.columns).toEqual([
      { kind: 'derived', expr: { type: 'function', name: 'LOWER', args: [{ type: 'identifier', name: 'email' }] }, alias: undefined },
    ])
  })

  it('should parse LENGTH function', () => {
    const select = parseSql('SELECT LENGTH(name) FROM users')
    expect(select.columns).toEqual([
      { kind: 'derived', expr: { type: 'function', name: 'LENGTH', args: [{ type: 'identifier', name: 'name' }] }, alias: undefined },
    ])
  })

  it('should parse TRIM function', () => {
    const select = parseSql('SELECT TRIM(name) FROM users')
    expect(select.columns).toEqual([
      { kind: 'derived', expr: { type: 'function', name: 'TRIM', args: [{ type: 'identifier', name: 'name' }] }, alias: undefined },
    ])
  })

  it('should parse CONCAT function with two arguments', () => {
    const select = parseSql('SELECT CONCAT(first_name, last_name) FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'CONCAT',
          args: [
            { type: 'identifier', name: 'first_name' },
            { type: 'identifier', name: 'last_name' },
          ],
        },
        alias: undefined,
      },
    ])
  })

  it('should parse CONCAT function with string literals', () => {
    const select = parseSql('SELECT CONCAT(first_name, \' \', last_name) FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'CONCAT',
          args: [
            { type: 'identifier', name: 'first_name' },
            { type: 'literal', value: ' ' },
            { type: 'identifier', name: 'last_name' },
          ],
        },
        alias: undefined,
      },
    ])
  })

  it('should parse SUBSTRING function with three arguments', () => {
    const select = parseSql('SELECT SUBSTRING(name, 1, 3) FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'SUBSTRING',
          args: [
            { type: 'identifier', name: 'name' },
            { type: 'literal', value: 1 },
            { type: 'literal', value: 3 },
          ],
        },
        alias: undefined,
      },
    ])
  })

  it('should parse string function with alias using AS', () => {
    const select = parseSql('SELECT UPPER(name) AS upper_name FROM users')
    expect(select.columns).toEqual([
      { kind: 'derived', expr: { type: 'function', name: 'UPPER', args: [{ type: 'identifier', name: 'name' }] }, alias: 'upper_name' },
    ])
  })

  it('should parse string function with implicit alias', () => {
    const select = parseSql('SELECT LOWER(email) user_email FROM users')
    expect(select.columns).toEqual([
      { kind: 'derived', expr: { type: 'function', name: 'LOWER', args: [{ type: 'identifier', name: 'email' }] }, alias: 'user_email' },
    ])
  })

  it('should parse multiple string functions', () => {
    const select = parseSql('SELECT UPPER(first_name), LOWER(last_name), LENGTH(email) FROM users')
    expect(select.columns).toEqual([
      { kind: 'derived', expr: { type: 'function', name: 'UPPER', args: [{ type: 'identifier', name: 'first_name' }] }, alias: undefined },
      { kind: 'derived', expr: { type: 'function', name: 'LOWER', args: [{ type: 'identifier', name: 'last_name' }] }, alias: undefined },
      { kind: 'derived', expr: { type: 'function', name: 'LENGTH', args: [{ type: 'identifier', name: 'email' }] }, alias: undefined },
    ])
  })

  it('should parse string function with qualified column name', () => {
    const select = parseSql('SELECT UPPER(users.name) FROM users')
    expect(select.columns).toEqual([
      { kind: 'derived', expr: { type: 'function', name: 'UPPER', args: [{ type: 'identifier', name: 'users.name' }] }, alias: undefined },
    ])
  })

  it('should parse mix of string functions and regular columns', () => {
    const select = parseSql('SELECT id, UPPER(name), email FROM users')
    expect(select.columns).toEqual([
      { kind: 'derived', expr: { type: 'identifier', name: 'id' }, alias: undefined },
      { kind: 'derived', expr: { type: 'function', name: 'UPPER', args: [{ type: 'identifier', name: 'name' }] }, alias: undefined },
      { kind: 'derived', expr: { type: 'identifier', name: 'email' }, alias: undefined },
    ])
  })

  it('should parse string functions with aggregate functions', () => {
    const select = parseSql('SELECT UPPER(city), COUNT(*) FROM users GROUP BY city')
    expect(select.columns).toEqual([
      { kind: 'derived', expr: { type: 'function', name: 'UPPER', args: [{ type: 'identifier', name: 'city' }] }, alias: undefined },
      { kind: 'aggregate', func: 'COUNT', arg: { kind: 'star' }, alias: undefined },
    ])
  })
})
