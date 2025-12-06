import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

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
})
