import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/** @type {null} */
const NULL = null

describe('date/time functions', () => {
  const dummy = [{ id: 1 }]

  describe('CURRENT_DATE', () => {
    it('should return date in YYYY-MM-DD format', async () => {
      const result = await collect(executeSql({
        tables: { dummy },
        query: 'SELECT CURRENT_DATE AS d FROM dummy',
      }))
      expect(result[0].d).toMatch(/^\d{4}-\d{2}-\d{2}$/)
    })

    it('should work with parentheses', async () => {
      const result = await collect(executeSql({
        tables: { dummy },
        query: 'SELECT CURRENT_DATE() AS d FROM dummy',
      }))
      expect(result[0].d).toMatch(/^\d{4}-\d{2}-\d{2}$/)
    })
  })

  describe('CURRENT_TIME', () => {
    it('should return time in HH:MM:SS.sss format', async () => {
      const result = await collect(executeSql({
        tables: { dummy },
        query: 'SELECT CURRENT_TIME AS t FROM dummy',
      }))
      expect(result[0].t).toMatch(/^\d{2}:\d{2}:\d{2}\.\d{3}$/)
    })

    it('should work with parentheses', async () => {
      const result = await collect(executeSql({
        tables: { dummy },
        query: 'SELECT CURRENT_TIME() AS t FROM dummy',
      }))
      expect(result[0].t).toMatch(/^\d{2}:\d{2}:\d{2}\.\d{3}$/)
    })
  })

  describe('CURRENT_TIMESTAMP', () => {
    it('should return ISO 8601 timestamp', async () => {
      const result = await collect(executeSql({
        tables: { dummy },
        query: 'SELECT CURRENT_TIMESTAMP AS ts FROM dummy',
      }))
      expect(result[0].ts).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
    })

    it('should work with parentheses', async () => {
      const result = await collect(executeSql({
        tables: { dummy },
        query: 'SELECT CURRENT_TIMESTAMP() AS ts FROM dummy',
      }))
      expect(result[0].ts).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
    })
  })

  describe('DATE_TRUNC', () => {
    const events = [
      { ts: '2024-07-15T14:30:45.123Z' },
    ]

    it('should truncate to year', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_TRUNC(\'year\', ts) AS t FROM events',
      }))
      expect(result[0].t).toBe('2024-01-01T00:00:00.000Z')
    })

    it('should truncate to month', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_TRUNC(\'month\', ts) AS t FROM events',
      }))
      expect(result[0].t).toBe('2024-07-01T00:00:00.000Z')
    })

    it('should truncate to day', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_TRUNC(\'day\', ts) AS t FROM events',
      }))
      expect(result[0].t).toBe('2024-07-15T00:00:00.000Z')
    })

    it('should truncate to hour', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_TRUNC(\'hour\', ts) AS t FROM events',
      }))
      expect(result[0].t).toBe('2024-07-15T14:00:00.000Z')
    })

    it('should truncate to minute', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_TRUNC(\'minute\', ts) AS t FROM events',
      }))
      expect(result[0].t).toBe('2024-07-15T14:30:00.000Z')
    })

    it('should truncate to second', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_TRUNC(\'second\', ts) AS t FROM events',
      }))
      expect(result[0].t).toBe('2024-07-15T14:30:45.000Z')
    })

    it('should return date string for date-only input', async () => {
      const dates = [{ d: '2024-07-15' }]
      const result = await collect(executeSql({
        tables: { dates },
        query: 'SELECT DATE_TRUNC(\'month\', d) AS t FROM dates',
      }))
      expect(result[0].t).toBe('2024-07-01')
    })

    it('should return null for null input', async () => {
      const data = [{ ts: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT DATE_TRUNC(\'month\', ts) AS t FROM data',
      }))
      expect(result[0].t).toBe(null)
    })

    it('should be case-insensitive for precision', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_TRUNC(\'MONTH\', ts) AS t FROM events',
      }))
      expect(result[0].t).toBe('2024-07-01T00:00:00.000Z')
    })

    it('should throw for wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_TRUNC(\'month\') AS t FROM events',
      }))).rejects.toThrow()
    })

    it('should work with GROUP BY', async () => {
      const logs = [
        { ts: '2024-01-10T08:00:00.000Z', val: 1 },
        { ts: '2024-01-20T12:00:00.000Z', val: 2 },
        { ts: '2024-02-05T09:00:00.000Z', val: 3 },
        { ts: '2024-02-15T15:00:00.000Z', val: 4 },
        { ts: '2024-03-01T10:00:00.000Z', val: 5 },
      ]
      const result = await collect(executeSql({
        tables: { logs },
        query: 'SELECT DATE_TRUNC(\'month\', ts) AS month, SUM(val) AS total FROM logs GROUP BY DATE_TRUNC(\'month\', ts) ORDER BY month',
      }))
      expect(result).toEqual([
        { month: '2024-01-01T00:00:00.000Z', total: 3 },
        { month: '2024-02-01T00:00:00.000Z', total: 7 },
        { month: '2024-03-01T00:00:00.000Z', total: 5 },
      ])
    })
  })
})
