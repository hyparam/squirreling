import { describe, expect, it } from 'vitest'
import { parseSelect } from '../helpers.js'

describe('parseSql', () => {
  describe('aggregate functions', () => {
    it('should parse COUNT(*)', () => {
      const select = parseSelect('SELECT COUNT(*) FROM users')
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'COUNT',
            args: [{ type: 'star', positionStart: 13, positionEnd: 14 }],
            positionStart: 7,
            positionEnd: 15,
          },
        },
      ])
    })

    it('should parse COUNT with column', () => {
      const select = parseSelect('SELECT COUNT(id) FROM users')
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'COUNT',
            args: [{ type: 'identifier', name: 'id', positionStart: 13, positionEnd: 15 }],
            positionStart: 7,
            positionEnd: 16,
          },
        },
      ])
    })

    it('should parse SUM', () => {
      const select = parseSelect('SELECT SUM(amount) FROM transactions')
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'SUM',
            args: [{ type: 'identifier', name: 'amount', positionStart: 11, positionEnd: 17 }],
            positionStart: 7,
            positionEnd: 18,
          },
        },
      ])
    })

    it('should parse AVG', () => {
      const select = parseSelect('SELECT AVG(score) FROM tests')
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'AVG',
            args: [{ type: 'identifier', name: 'score', positionStart: 11, positionEnd: 16 }],
            positionStart: 7,
            positionEnd: 17,
          },
        },
      ])
    })

    it('should parse MIN and MAX', () => {
      const select = parseSelect('SELECT MIN(price), MAX(price) FROM products')
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'MIN',
            args: [{ type: 'identifier', name: 'price', positionStart: 11, positionEnd: 16 }],
            positionStart: 7,
            positionEnd: 17,
          },
        },
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'MAX',
            args: [{ type: 'identifier', name: 'price', positionStart: 23, positionEnd: 28 }],
            positionStart: 19,
            positionEnd: 29,
          },
        },
      ])
    })

    it('should parse aggregate with alias', () => {
      const select = parseSelect('SELECT COUNT(*) AS total FROM users')
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'COUNT',
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
