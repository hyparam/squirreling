import { describe, expect, it } from 'vitest'
import { parseSelect } from '../helpers.js'

describe('parseSql - string functions', () => {
  it('should parse UPPER function', () => {
    const select = parseSelect('SELECT UPPER(name) FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'UPPER',
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
      },
    ])
  })

  it('should parse LOWER function', () => {
    const select = parseSelect('SELECT LOWER(email) FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'LOWER',
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
      },
    ])
  })

  it('should parse LENGTH function', () => {
    const select = parseSelect('SELECT LENGTH(name) FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'LENGTH',
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
      },
    ])
  })

  it('should parse TRIM function', () => {
    const select = parseSelect('SELECT TRIM(name) FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'TRIM',
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
      },
    ])
  })

  it('should parse CONCAT function with two arguments', () => {
    const select = parseSelect('SELECT CONCAT(first_name, last_name) FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'CONCAT',
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
      },
    ])
  })

  it('should parse CONCAT function with string literals', () => {
    const select = parseSelect('SELECT CONCAT(first_name, \' \', last_name) FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'CONCAT',
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
      },
    ])
  })

  it('should parse SUBSTRING function with three arguments', () => {
    const select = parseSelect('SELECT SUBSTRING(name, 1, 3) FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'SUBSTRING',
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
      },
    ])
  })

  it('should parse string function with alias using AS', () => {
    const select = parseSelect('SELECT UPPER(name) AS upper_name FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'UPPER',
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
    const select = parseSelect('SELECT LOWER(email) user_email FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'LOWER',
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
    const select = parseSelect('SELECT UPPER(first_name), LOWER(last_name), LENGTH(email) FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'UPPER',
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
      },
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'LOWER',
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
      },
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'LENGTH',
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
      },
    ])
  })

  it('should parse string function with qualified column name', () => {
    const select = parseSelect('SELECT UPPER(users.name) FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'UPPER',
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
      },
    ])
  })

  it('should parse mix of string functions and regular columns', () => {
    const select = parseSelect('SELECT id, UPPER(name), email FROM users')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: { type: 'identifier', name: 'id', positionStart: 7, positionEnd: 9 },
      },
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'UPPER',
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
      },
      {
        type: 'derived',
        expr: {
          type: 'identifier',
          name: 'email',
          positionStart: 24,
          positionEnd: 29,
        },
      },
    ])
  })

  it('should parse string functions with aggregate functions', () => {
    const select = parseSelect('SELECT UPPER(city), COUNT(*) FROM users GROUP BY city')
    expect(select.columns).toEqual([
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'UPPER',
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
      },
      {
        type: 'derived',
        expr: {
          type: 'function',
          funcName: 'COUNT',
          args: [{ type: 'star', positionStart: 26, positionEnd: 27 }],
          positionStart: 20,
          positionEnd: 28,
        },
      },
    ])
  })
})
