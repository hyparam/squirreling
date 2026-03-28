import { describe, expect, it } from 'vitest'
import { parseSelect } from '../helpers.js'

describe('ORDER BY clause', () => {
  it('should parse ORDER BY with default ASC', () => {
    const select = parseSelect('SELECT * FROM users ORDER BY name')
    expect(select.orderBy).toEqual([
      {
        expr: {
          type: 'identifier',
          name: 'name',
          positionStart: 29,
          positionEnd: 33,
        },
        direction: 'ASC',
      },
    ])
  })

  it('should parse ORDER BY with explicit ASC', () => {
    const select = parseSelect('SELECT * FROM users ORDER BY name ASC')
    expect(select.orderBy).toEqual([
      {
        expr: {
          type: 'identifier',
          name: 'name',
          positionStart: 29,
          positionEnd: 33,
        },
        direction: 'ASC',
      },
    ])
  })

  it('should parse ORDER BY with DESC', () => {
    const select = parseSelect('SELECT * FROM users ORDER BY age DESC')
    expect(select.orderBy).toEqual([
      {
        expr: {
          type: 'identifier',
          name: 'age',
          positionStart: 29,
          positionEnd: 32,
        },
        direction: 'DESC',
      },
    ])
  })

  it('should parse ORDER BY with multiple columns', () => {
    const select = parseSelect('SELECT * FROM users ORDER BY city ASC, age DESC')
    expect(select.orderBy).toEqual([
      {
        expr: {
          type: 'identifier',
          name: 'city',
          positionStart: 29,
          positionEnd: 33,
        },
        direction: 'ASC',
      },
      {
        expr: {
          type: 'identifier',
          name: 'age',
          positionStart: 39,
          positionEnd: 42,
        },
        direction: 'DESC',
      },
    ])
  })

  it('should parse ORDER BY with CAST expression', () => {
    const select = parseSelect('SELECT * FROM table ORDER BY CAST(size AS INTEGER)')
    expect(select.orderBy).toEqual([
      {
        expr: {
          type: 'cast',
          expr: {
            type: 'identifier',
            name: 'size',
            positionStart: 34,
            positionEnd: 38,
          },
          toType: 'INTEGER',
          positionStart: 29,
          positionEnd: 50,
        },
        direction: 'ASC',
      },
    ])
  })

  it('should parse ORDER BY with NULLS FIRST', () => {
    const select = parseSelect('SELECT * FROM users ORDER BY name NULLS FIRST')
    expect(select.orderBy).toEqual([
      {
        expr: {
          type: 'identifier',
          name: 'name',
          positionStart: 29,
          positionEnd: 33,
        },
        direction: 'ASC',
        nulls: 'FIRST',
      },
    ])
  })

  it('should parse ORDER BY with NULLS LAST', () => {
    const select = parseSelect('SELECT * FROM users ORDER BY age DESC NULLS LAST')
    expect(select.orderBy).toEqual([
      {
        expr: {
          type: 'identifier',
          name: 'age',
          positionStart: 29,
          positionEnd: 32,
        },
        direction: 'DESC',
        nulls: 'LAST',
      },
    ])
  })

  it('should parse ORDER BY RANDOM()', () => {
    const select = parseSelect('SELECT * FROM users ORDER BY RANDOM()')
    expect(select.orderBy).toEqual([
      {
        expr: {
          type: 'function',
          funcName: 'RANDOM',
          args: [],
          positionStart: 29,
          positionEnd: 37,
        },
        direction: 'ASC',
      },
    ])
  })

  it('should parse ORDER BY RAND()', () => {
    const select = parseSelect('SELECT * FROM users ORDER BY RAND()')
    expect(select.orderBy).toEqual([
      {
        expr: {
          type: 'function',
          funcName: 'RAND',
          args: [],
          positionStart: 29,
          positionEnd: 35,
        },
        direction: 'ASC',
      },
    ])
  })
})
