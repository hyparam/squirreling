import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql', () => {
  describe('GROUP BY clause', () => {
    it('should parse GROUP BY with single column', () => {
      const select = parseSql({ query: 'SELECT city, COUNT(*) FROM users GROUP BY city' })
      expect(select.groupBy).toEqual([
        { type: 'identifier', name: 'city', positionStart: 42, positionEnd: 46 },
      ])
    })

    it('should parse GROUP BY with multiple columns', () => {
      const select = parseSql({ query: 'SELECT city, state, COUNT(*) FROM users GROUP BY city, state' })
      expect(select.groupBy).toEqual([
        { type: 'identifier', name: 'city', positionStart: 49, positionEnd: 53 },
        { type: 'identifier', name: 'state', positionStart: 55, positionEnd: 60 },
      ])
    })

    it('should parse nested function in aggregate', () => {
      const select = parseSql({ query: 'SELECT MAX(LENGTH(problem)) AS max_problem_len FROM table' })
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'function',
            name: 'MAX',
            args: [{
              type: 'function',
              name: 'LENGTH',
              args: [{ type: 'identifier', name: 'problem', positionStart: 18, positionEnd: 25 }],
              positionStart: 11,
              positionEnd: 26,
            }],
            positionStart: 7,
            positionEnd: 27,
          },
          alias: 'max_problem_len',
        },
      ])
    })

    it('should parse COUNT(DISTINCT ...)', () => {
      const select = parseSql({ query: 'SELECT COUNT(DISTINCT problem_id) AS n_unique_problems FROM table' })
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'function',
            name: 'COUNT',
            args: [{ type: 'identifier', name: 'problem_id', positionStart: 22, positionEnd: 32 }],
            distinct: true,
            positionStart: 7,
            positionEnd: 33,
          },
          alias: 'n_unique_problems',
        },
      ])
    })

    it('should parse COUNT(ALL ...)', () => {
      const select = parseSql({ query: 'SELECT COUNT(ALL problem_id) FROM table' })
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'function',
            name: 'COUNT',
            args: [{ type: 'identifier', name: 'problem_id', positionStart: 17, positionEnd: 27 }],
            positionStart: 7,
            positionEnd: 28,
          },
        },
      ])
    })
  })
})
