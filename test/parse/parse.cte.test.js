import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql - CTE (WITH clause)', () => {
  describe('basic CTEs', () => {
    it('should parse simple CTE', () => {
      const select = parseSql({
        query: 'WITH cte AS (SELECT * FROM users) SELECT * FROM cte',
      })
      expect(select.with).toBeDefined()
      expect(select.with?.ctes).toHaveLength(1)
      expect(select.with?.ctes[0].name).toBe('cte')
      expect(select.with?.ctes[0].select.from).toEqual({ type: 'table', table: 'users', positionStart: 27, positionEnd: 32 })
      expect(select.from).toEqual({ type: 'table', table: 'cte', positionStart: 48, positionEnd: 51 })
    })

    it('should parse CTE with column selection', () => {
      const select = parseSql({
        query: 'WITH active AS (SELECT id, name FROM users WHERE active = TRUE) SELECT name FROM active',
      })
      expect(select.with?.ctes).toHaveLength(1)
      expect(select.with?.ctes[0].name).toBe('active')
      expect(select.with?.ctes[0].select.columns).toHaveLength(2)
      expect(select.with?.ctes[0].select.where).toBeDefined()
    })

    it('should parse CTE with alias', () => {
      const select = parseSql({
        query: 'WITH cte AS (SELECT * FROM users) SELECT * FROM cte AS t',
      })
      expect(select.from).toEqual({ type: 'table', table: 'cte', alias: 't', positionStart: 48, positionEnd: 56 })
    })
  })

  describe('multiple CTEs', () => {
    it('should parse multiple CTEs', () => {
      const select = parseSql({
        query: `
          WITH
            cte1 AS (SELECT id FROM users),
            cte2 AS (SELECT id FROM orders)
          SELECT * FROM cte1
        `,
      })
      expect(select.with?.ctes).toHaveLength(2)
      expect(select.with?.ctes[0].name).toBe('cte1')
      expect(select.with?.ctes[1].name).toBe('cte2')
      expect(select.with?.ctes[0].select.from).toEqual({
        type: 'table',
        table: 'users',
        positionStart: 52,
        positionEnd: 57,
      })
      expect(select.with?.ctes[1].select.from).toEqual({
        type: 'table',
        table: 'orders',
        positionStart: 96,
        positionEnd: 102,
      })
    })

    it('should parse CTE referencing another CTE', () => {
      const select = parseSql({
        query: `
          WITH
            base AS (SELECT id, name FROM users),
            filtered AS (SELECT * FROM base WHERE id > 1)
          SELECT * FROM filtered
        `,
      })
      expect(select.with?.ctes).toHaveLength(2)
      expect(select.with?.ctes[0].name).toBe('base')
      expect(select.with?.ctes[1].name).toBe('filtered')
      expect(select.with?.ctes[1].select.from.type).toBe('table')
      expect(select.with?.ctes[1].select.from.type === 'table' && select.with?.ctes[1].select.from.table).toBe('base')
    })
  })

  describe('CTE with complex queries', () => {
    it('should parse CTE with GROUP BY', () => {
      const select = parseSql({
        query: `
          WITH totals AS (
            SELECT user_id, SUM(amount) AS total
            FROM orders
            GROUP BY user_id
          )
          SELECT * FROM totals
        `,
      })
      expect(select.with?.ctes[0].select.groupBy).toHaveLength(1)
    })

    it('should parse CTE with HAVING', () => {
      const select = parseSql({
        query: `
          WITH big_spenders AS (
            SELECT user_id, SUM(amount) AS total
            FROM orders
            GROUP BY user_id
            HAVING SUM(amount) > 100
          )
          SELECT * FROM big_spenders
        `,
      })
      expect(select.with?.ctes[0].select.having).toBeDefined()
    })

    it('should parse CTE with ORDER BY and LIMIT', () => {
      const select = parseSql({
        query: `
          WITH top_users AS (
            SELECT * FROM users
            ORDER BY score DESC
            LIMIT 10
          )
          SELECT * FROM top_users
        `,
      })
      expect(select.with?.ctes[0].select.orderBy).toHaveLength(1)
      expect(select.with?.ctes[0].select.limit).toBe(10)
    })

    it('should parse CTE with JOIN in main query', () => {
      const select = parseSql({
        query: `
          WITH active AS (SELECT id, name FROM users WHERE active = TRUE)
          SELECT active.name, orders.amount
          FROM active
          JOIN orders ON active.id = orders.user_id
        `,
      })
      expect(select.from.type).toBe('table')
      expect(select.from.type === 'table' && select.from.table).toBe('active')
      expect(select.joins).toHaveLength(1)
      expect(select.joins[0].table).toBe('orders')
    })
  })

  describe('error cases', () => {
    it('should throw error for duplicate CTE names', () => {
      expect(() => {
        parseSql({
          query: 'WITH cte AS (SELECT 1 FROM a), cte AS (SELECT 2 FROM b) SELECT * FROM cte',
        })
      }).toThrow('CTE "cte" is defined more than once at position 0')
    })

    it('should throw error for duplicate CTE names (case-insensitive)', () => {
      expect(() => {
        parseSql({
          query: 'WITH Cte AS (SELECT 1 FROM a), CTE AS (SELECT 2 FROM b) SELECT * FROM cte',
        })
      }).toThrow('CTE "CTE" is defined more than once at position 0')
    })
  })

  describe('edge cases', () => {
    it('should parse query without WITH clause', () => {
      const select = parseSql({
        query: 'SELECT * FROM users',
      })
      expect(select.with).toBeUndefined()
    })

    it('should preserve CTE name case', () => {
      const select = parseSql({
        query: 'WITH MyTable AS (SELECT * FROM users) SELECT * FROM MyTable',
      })
      expect(select.with?.ctes[0].name).toBe('MyTable')
    })
  })
})
