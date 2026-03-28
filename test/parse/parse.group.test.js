import { describe, expect, it } from 'vitest'
import { parseSelect } from '../helpers.js'

describe('parseSql', () => {
  describe('GROUP BY clause', () => {
    it('should parse GROUP BY with single column', () => {
      const select = parseSelect('SELECT city, COUNT(*) FROM users GROUP BY city')
      expect(select.groupBy).toEqual([
        { type: 'identifier', name: 'city', positionStart: 42, positionEnd: 46 },
      ])
    })

    it('should parse GROUP BY with multiple columns', () => {
      const select = parseSelect('SELECT city, state, COUNT(*) FROM users GROUP BY city, state')
      expect(select.groupBy).toEqual([
        { type: 'identifier', name: 'city', positionStart: 49, positionEnd: 53 },
        { type: 'identifier', name: 'state', positionStart: 55, positionEnd: 60 },
      ])
    })

    it('should parse nested function in aggregate', () => {
      const select = parseSelect('SELECT MAX(LENGTH(problem)) AS max_problem_len FROM table')
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'MAX',
            args: [{
              type: 'function',
              funcName: 'LENGTH',
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
      const select = parseSelect('SELECT COUNT(DISTINCT problem_id) AS n_unique_problems FROM table')
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'COUNT',
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
      const select = parseSelect('SELECT COUNT(ALL problem_id) FROM table')
      expect(select.columns).toEqual([
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'COUNT',
            args: [{ type: 'identifier', name: 'problem_id', positionStart: 17, positionEnd: 27 }],
            positionStart: 7,
            positionEnd: 28,
          },
        },
      ])
    })
  })
})
