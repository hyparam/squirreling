import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql', () => {
  describe('aggregate functions', () => {
    it('should parse COUNT(*)', () => {
      const select = parseSql({ query: 'SELECT COUNT(*) FROM users' })
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'function',
            name: 'COUNT',
            args: [{ type: 'star', positionStart: 13, positionEnd: 14 }],
            positionStart: 7,
            positionEnd: 15,
          },
        },
      ])
    })

    it('should parse COUNT with column', () => {
      const select = parseSql({ query: 'SELECT COUNT(id) FROM users' })
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'function',
            name: 'COUNT',
            args: [{ type: 'identifier', name: 'id', positionStart: 13, positionEnd: 15 }],
            positionStart: 7,
            positionEnd: 16,
          },
        },
      ])
    })

    it('should parse SUM', () => {
      const select = parseSql({ query: 'SELECT SUM(amount) FROM transactions' })
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'function',
            name: 'SUM',
            args: [{ type: 'identifier', name: 'amount', positionStart: 11, positionEnd: 17 }],
            positionStart: 7,
            positionEnd: 18,
          },
        },
      ])
    })

    it('should parse AVG', () => {
      const select = parseSql({ query: 'SELECT AVG(score) FROM tests' })
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'function',
            name: 'AVG',
            args: [{ type: 'identifier', name: 'score', positionStart: 11, positionEnd: 16 }],
            positionStart: 7,
            positionEnd: 17,
          },
        },
      ])
    })

    it('should parse MIN and MAX', () => {
      const select = parseSql({ query: 'SELECT MIN(price), MAX(price) FROM products' })
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'function',
            name: 'MIN',
            args: [{ type: 'identifier', name: 'price', positionStart: 11, positionEnd: 16 }],
            positionStart: 7,
            positionEnd: 17,
          },
        },
        {
          kind: 'derived',
          expr: {
            type: 'function',
            name: 'MAX',
            args: [{ type: 'identifier', name: 'price', positionStart: 23, positionEnd: 28 }],
            positionStart: 19,
            positionEnd: 29,
          },
        },
      ])
    })

    it('should parse aggregate with alias', () => {
      const select = parseSql({ query: 'SELECT COUNT(*) AS total FROM users' })
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'function',
            name: 'COUNT',
            args: [{ type: 'star', positionStart: 13, positionEnd: 14 }],
            positionStart: 7,
            positionEnd: 15,
          },
          alias: 'total',
        },
      ])
    })
  })
})
