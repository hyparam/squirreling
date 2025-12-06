import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/** @type {null} */
const NULL = null

describe('INTERVAL date arithmetic', () => {
  const dummy = [{ id: 1 }]
  const orders = [
    { id: 1, order_date: '2025-01-15' },
    { id: 2, order_date: '2025-06-20' },
  ]
  const events = [
    { id: 1, created_at: '2025-01-15T10:30:00.000Z' },
  ]

  describe('date + INTERVAL', () => {
    it('should add days to a date', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT order_date + INTERVAL 7 DAY AS next_week FROM orders WHERE id = 1',
      }))
      expect(result[0].next_week).toBe('2025-01-22')
    })

    it('should add months to a date', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT order_date + INTERVAL 1 MONTH AS next_month FROM orders WHERE id = 1',
      }))
      expect(result[0].next_month).toBe('2025-02-15')
    })

    it('should add years to a date', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT order_date + INTERVAL 1 YEAR AS next_year FROM orders WHERE id = 1',
      }))
      expect(result[0].next_year).toBe('2026-01-15')
    })
  })

  describe('date - INTERVAL', () => {
    it('should subtract days from a date', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT order_date - INTERVAL 7 DAY AS last_week FROM orders WHERE id = 1',
      }))
      expect(result[0].last_week).toBe('2025-01-08')
    })

    it('should subtract months from a date', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT order_date - INTERVAL 1 MONTH AS last_month FROM orders WHERE id = 1',
      }))
      expect(result[0].last_month).toBe('2024-12-15')
    })
  })

  describe('timestamp + INTERVAL', () => {
    it('should add hours to a timestamp', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT created_at + INTERVAL 2 HOUR AS later FROM events',
      }))
      expect(result[0].later).toBe('2025-01-15T12:30:00.000Z')
    })

    it('should add minutes to a timestamp', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT created_at + INTERVAL 30 MINUTE AS later FROM events',
      }))
      expect(result[0].later).toBe('2025-01-15T11:00:00.000Z')
    })

    it('should add seconds to a timestamp', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT created_at + INTERVAL 45 SECOND AS later FROM events',
      }))
      expect(result[0].later).toBe('2025-01-15T10:30:45.000Z')
    })

    it('should subtract time from a timestamp', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT created_at - INTERVAL 30 MINUTE AS earlier FROM events',
      }))
      expect(result[0].earlier).toBe('2025-01-15T10:00:00.000Z')
    })
  })

  describe('CURRENT_DATE/TIMESTAMP + INTERVAL', () => {
    it('should add interval to CURRENT_DATE', async () => {
      const result = await collect(executeSql({
        tables: { dummy },
        query: 'SELECT CURRENT_DATE + INTERVAL 1 DAY AS tomorrow FROM dummy',
      }))
      expect(result[0].tomorrow).toMatch(/^\d{4}-\d{2}-\d{2}$/)
    })

    it('should subtract interval from CURRENT_TIMESTAMP', async () => {
      const result = await collect(executeSql({
        tables: { dummy },
        query: 'SELECT CURRENT_TIMESTAMP - INTERVAL 1 HOUR AS hour_ago FROM dummy',
      }))
      expect(result[0].hour_ago).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
    })
  })

  describe('standard SQL syntax', () => {
    it('should work with quoted interval values', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT order_date + INTERVAL \'7\' DAY AS next_week FROM orders WHERE id = 1',
      }))
      expect(result[0].next_week).toBe('2025-01-22')
    })
  })

  describe('WHERE clause with intervals', () => {
    it('should filter using interval arithmetic', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT * FROM orders WHERE order_date > \'2025-01-01\' + INTERVAL 14 DAY',
      }))
      expect(result.length).toBe(1)
      expect(result[0].id).toBe(2)
    })
  })

  describe('edge cases', () => {
    it('should handle null dates', async () => {
      const data = [{ id: 1, date: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT date + INTERVAL 1 DAY AS result FROM data',
      }))
      expect(result[0].result).toBe(null)
    })

    it('should handle negative interval values', async () => {
      const result = await collect(executeSql({
        tables: { orders },
        query: 'SELECT order_date + INTERVAL -7 DAY AS last_week FROM orders WHERE id = 1',
      }))
      expect(result[0].last_week).toBe('2025-01-08')
    })
  })
})
