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

  describe('EXTRACT', () => {
    const events = [
      { ts: '2024-07-15T14:30:45.123Z' },
    ]

    it('should extract year', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT EXTRACT(YEAR FROM ts) AS y FROM events',
      }))
      expect(result[0].y).toBe(2024)
    })

    it('should extract month', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT EXTRACT(MONTH FROM ts) AS m FROM events',
      }))
      expect(result[0].m).toBe(7)
    })

    it('should extract day', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT EXTRACT(DAY FROM ts) AS d FROM events',
      }))
      expect(result[0].d).toBe(15)
    })

    it('should extract hour', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT EXTRACT(HOUR FROM ts) AS h FROM events',
      }))
      expect(result[0].h).toBe(14)
    })

    it('should extract minute', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT EXTRACT(MINUTE FROM ts) AS m FROM events',
      }))
      expect(result[0].m).toBe(30)
    })

    it('should extract second', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT EXTRACT(SECOND FROM ts) AS s FROM events',
      }))
      expect(result[0].s).toBe(45)
    })

    it('should extract dow (day of week)', async () => {
      // 2024-07-15 is a Monday = 1
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT EXTRACT(DOW FROM ts) AS dow FROM events',
      }))
      expect(result[0].dow).toBe(1)
    })

    it('should extract epoch', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT EXTRACT(EPOCH FROM ts) AS e FROM events',
      }))
      expect(result[0].e).toBe(new Date('2024-07-15T14:30:45.123Z').getTime() / 1000)
    })

    it('should work with date-only input', async () => {
      const dates = [{ d: '2024-07-15' }]
      const result = await collect(executeSql({
        tables: { dates },
        query: 'SELECT EXTRACT(MONTH FROM d) AS m FROM dates',
      }))
      expect(result[0].m).toBe(7)
    })

    it('should return null for null input', async () => {
      const data = [{ ts: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT EXTRACT(YEAR FROM ts) AS y FROM data',
      }))
      expect(result[0].y).toBe(null)
    })

    it('should work in expressions', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT EXTRACT(YEAR FROM ts) * 100 + EXTRACT(MONTH FROM ts) AS ym FROM events',
      }))
      expect(result[0].ym).toBe(202407)
    })

    it('should work in WHERE clause', async () => {
      const logs = [
        { ts: '2024-01-15T00:00:00.000Z', val: 1 },
        { ts: '2024-07-15T00:00:00.000Z', val: 2 },
        { ts: '2024-12-15T00:00:00.000Z', val: 3 },
      ]
      const result = await collect(executeSql({
        tables: { logs },
        query: 'SELECT val FROM logs WHERE EXTRACT(MONTH FROM ts) = 7',
      }))
      expect(result).toEqual([{ val: 2 }])
    })

    it('should accept lowercase field names', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT extract(dow FROM ts) AS m FROM events',
      }))
      expect(result[0].m).toBe(1)
    })
  })

  describe('DATE_PART', () => {
    const events = [
      { ts: '2024-07-15T14:30:45.123Z' },
    ]

    it('should extract year', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_PART(\'year\', ts) AS y FROM events',
      }))
      expect(result[0].y).toBe(2024)
    })

    it('should extract month', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_PART(\'month\', ts) AS m FROM events',
      }))
      expect(result[0].m).toBe(7)
    })

    it('should extract dow', async () => {
      const result = await collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_PART(\'dow\', ts) AS dow FROM events',
      }))
      expect(result[0].dow).toBe(1)
    })

    it('should return null for null input', async () => {
      const data = [{ ts: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT DATE_PART(\'year\', ts) AS y FROM data',
      }))
      expect(result[0].y).toBe(null)
    })

    it('should throw for wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { events },
        query: 'SELECT DATE_PART(\'year\') AS y FROM events',
      }))).rejects.toThrow()
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
