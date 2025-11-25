import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

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
