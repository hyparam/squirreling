import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql - string functions', () => {
  it('should parse UPPER function', () => {
    const select = parseSql('SELECT UPPER(name) FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'UPPER',
          args: [
            {
              type: 'identifier',
              name: 'name',
              positionStart: 13,
              positionEnd: 17,
            },
          ],
          positionStart: 7,
          positionEnd: 18,
        },
        alias: undefined,
      },
    ])
  })

  it('should parse LOWER function', () => {
    const select = parseSql('SELECT LOWER(email) FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'LOWER',
          args: [
            {
              type: 'identifier',
              name: 'email',
              positionStart: 13,
              positionEnd: 18,
            },
          ],
          positionStart: 7,
          positionEnd: 19,
        },
        alias: undefined,
      },
    ])
  })

  it('should parse LENGTH function', () => {
    const select = parseSql('SELECT LENGTH(name) FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'LENGTH',
          args: [
            {
              type: 'identifier',
              name: 'name',
              positionStart: 14,
              positionEnd: 18,
            },
          ],
          positionStart: 7,
          positionEnd: 19,
        },
        alias: undefined,
      },
    ])
  })

  it('should parse TRIM function', () => {
    const select = parseSql('SELECT TRIM(name) FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'TRIM',
          args: [
            {
              type: 'identifier',
              name: 'name',
              positionStart: 12,
              positionEnd: 16,
            },
          ],
          positionStart: 7,
          positionEnd: 17,
        },
        alias: undefined,
      },
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
            {
              type: 'identifier',
              name: 'first_name',
              positionStart: 14,
              positionEnd: 24,
            },
            {
              type: 'identifier',
              name: 'last_name',
              positionStart: 26,
              positionEnd: 35,
            },
          ],
          positionStart: 7,
          positionEnd: 36,
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
            {
              type: 'identifier',
              name: 'first_name',
              positionStart: 14,
              positionEnd: 24,
            },
            { type: 'literal', value: ' ', positionStart: 26, positionEnd: 29 },
            {
              type: 'identifier',
              name: 'last_name',
              positionStart: 31,
              positionEnd: 40,
            },
          ],
          positionStart: 7,
          positionEnd: 41,
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
            {
              type: 'identifier',
              name: 'name',
              positionStart: 17,
              positionEnd: 21,
            },
            { type: 'literal', value: 1, positionStart: 23, positionEnd: 24 },
            { type: 'literal', value: 3, positionStart: 26, positionEnd: 27 },
          ],
          positionStart: 7,
          positionEnd: 28,
        },
        alias: undefined,
      },
    ])
  })

  it('should parse string function with alias using AS', () => {
    const select = parseSql('SELECT UPPER(name) AS upper_name FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'UPPER',
          args: [
            {
              type: 'identifier',
              name: 'name',
              positionStart: 13,
              positionEnd: 17,
            },
          ],
          positionStart: 7,
          positionEnd: 18,
        },
        alias: 'upper_name',
      },
    ])
  })

  it('should parse string function with implicit alias', () => {
    const select = parseSql('SELECT LOWER(email) user_email FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'LOWER',
          args: [
            {
              type: 'identifier',
              name: 'email',
              positionStart: 13,
              positionEnd: 18,
            },
          ],
          positionStart: 7,
          positionEnd: 19,
        },
        alias: 'user_email',
      },
    ])
  })

  it('should parse multiple string functions', () => {
    const select = parseSql('SELECT UPPER(first_name), LOWER(last_name), LENGTH(email) FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'UPPER',
          args: [
            {
              type: 'identifier',
              name: 'first_name',
              positionStart: 13,
              positionEnd: 23,
            },
          ],
          positionStart: 7,
          positionEnd: 24,
        },
        alias: undefined,
      },
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'LOWER',
          args: [
            {
              type: 'identifier',
              name: 'last_name',
              positionStart: 32,
              positionEnd: 41,
            },
          ],
          positionStart: 26,
          positionEnd: 42,
        },
        alias: undefined,
      },
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'LENGTH',
          args: [
            {
              type: 'identifier',
              name: 'email',
              positionStart: 51,
              positionEnd: 56,
            },
          ],
          positionStart: 44,
          positionEnd: 57,
        },
        alias: undefined,
      },
    ])
  })

  it('should parse string function with qualified column name', () => {
    const select = parseSql('SELECT UPPER(users.name) FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'UPPER',
          args: [
            {
              type: 'identifier',
              name: 'users.name',
              positionStart: 13,
              positionEnd: 23,
            },
          ],
          positionStart: 7,
          positionEnd: 24,
        },
        alias: undefined,
      },
    ])
  })

  it('should parse mix of string functions and regular columns', () => {
    const select = parseSql('SELECT id, UPPER(name), email FROM users')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: { type: 'identifier', name: 'id', positionStart: 7, positionEnd: 9 },
        alias: undefined,
      },
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'UPPER',
          args: [
            {
              type: 'identifier',
              name: 'name',
              positionStart: 17,
              positionEnd: 21,
            },
          ],
          positionStart: 11,
          positionEnd: 22,
        },
        alias: undefined,
      },
      {
        kind: 'derived',
        expr: {
          type: 'identifier',
          name: 'email',
          positionStart: 24,
          positionEnd: 29,
        },
        alias: undefined,
      },
    ])
  })

  it('should parse string functions with aggregate functions', () => {
    const select = parseSql('SELECT UPPER(city), COUNT(*) FROM users GROUP BY city')
    expect(select.columns).toEqual([
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'UPPER',
          args: [
            {
              type: 'identifier',
              name: 'city',
              positionStart: 13,
              positionEnd: 17,
            },
          ],
          positionStart: 7,
          positionEnd: 18,
        },
        alias: undefined,
      },
      {
        kind: 'derived',
        expr: {
          type: 'function',
          name: 'COUNT',
          args: [{ type: 'identifier', name: '*', positionStart: 26, positionEnd: 27 }],
          positionStart: 20,
          positionEnd: 28,
        },
      },
    ])
  })
})
