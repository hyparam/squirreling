import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'
import { trackingSource } from './trackingSource.js'

describe('window functions', () => {
  const sales = [
    { id: 1, region: 'east', amount: 100 },
    { id: 2, region: 'east', amount: 200 },
    { id: 3, region: 'east', amount: 150 },
    { id: 4, region: 'west', amount: 300 },
    { id: 5, region: 'west', amount: 50 },
  ]

  describe('ROW_NUMBER', () => {
    it('should number rows within partitions ordered by amount DESC', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, region: 'east', amount: 100, rn: 3 },
        { id: 2, region: 'east', amount: 200, rn: 1 },
        { id: 3, region: 'east', amount: 150, rn: 2 },
        { id: 4, region: 'west', amount: 300, rn: 1 },
        { id: 5, region: 'west', amount: 50, rn: 2 },
      ])
    })

    it('should number all rows globally with no PARTITION BY', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, ROW_NUMBER() OVER (ORDER BY amount) AS rn FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, rn: 2 },
        { id: 2, rn: 4 },
        { id: 3, rn: 3 },
        { id: 4, rn: 5 },
        { id: 5, rn: 1 },
      ])
    })

    it('should preserve input order within partition when ORDER BY is omitted', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, region, ROW_NUMBER() OVER (PARTITION BY region) AS rn FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, region: 'east', rn: 1 },
        { id: 2, region: 'east', rn: 2 },
        { id: 3, region: 'east', rn: 3 },
        { id: 4, region: 'west', rn: 1 },
        { id: 5, region: 'west', rn: 2 },
      ])
    })

    it('should assign sequential numbers with empty OVER()', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, ROW_NUMBER() OVER () AS rn FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, rn: 1 },
        { id: 2, rn: 2 },
        { id: 3, rn: 3 },
        { id: 4, rn: 4 },
        { id: 5, rn: 5 },
      ])
    })

    it('should default to row_number as column alias without AS', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, row_number: 1 },
        { id: 2, row_number: 3 },
        { id: 3, row_number: 2 },
        { id: 4, row_number: 2 },
        { id: 5, row_number: 1 },
      ])
    })

    it('should combine with WHERE before window evaluation', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn FROM sales WHERE region = \'east\'',
      }))
      expect(result).toEqual([
        { id: 1, rn: 1 },
        { id: 2, rn: 3 },
        { id: 3, rn: 2 },
      ])
    })

    it('should allow outer ORDER BY on the window alias', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM sales ORDER BY region, rn',
      }))
      expect(result).toEqual([
        { id: 2, rn: 1 },
        { id: 3, rn: 2 },
        { id: 1, rn: 3 },
        { id: 4, rn: 1 },
        { id: 5, rn: 2 },
      ])
    })

    it('should place NULL partitions together', async () => {
      const data = [
        { id: 1, g: 'a' },
        { id: 2, g: null },
        { id: 3, g: 'a' },
        { id: 4, g: null },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT id, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn FROM data',
      }))
      expect(result).toEqual([
        { id: 1, g: 'a', rn: 1 },
        { id: 2, g: null, rn: 1 },
        { id: 3, g: 'a', rn: 2 },
        { id: 4, g: null, rn: 2 },
      ])
    })

    it('should throw on ROW_NUMBER without OVER', () => {
      expect(() => executeSql({
        tables: { sales },
        query: 'SELECT ROW_NUMBER() FROM sales',
      })).toThrow('ROW_NUMBER() requires an OVER clause at position 7')
    })

    it('should throw on ROW_NUMBER with arguments', () => {
      expect(() => executeSql({
        tables: { sales },
        query: 'SELECT ROW_NUMBER(id) OVER (ORDER BY id) FROM sales',
      })).toThrow('ROW_NUMBER() function requires 0 arguments, got 1')
    })

    it('should throw when used in WHERE', () => {
      expect(() => executeSql({
        tables: { sales },
        query: 'SELECT * FROM sales WHERE ROW_NUMBER() OVER (ORDER BY id) > 1',
      })).toThrow('Window function ROW_NUMBER is not allowed in WHERE clause')
    })

    it('should stream OVER () without buffering and push LIMIT down to scan', async () => {
      const data = Array.from({ length: 1000 }, (_, i) => ({ id: i + 1 }))
      const { source, getRowCount } = trackingSource(data)
      const result = await collect(executeSql({
        tables: { data: source },
        query: 'SELECT id, ROW_NUMBER() OVER () AS rn FROM data LIMIT 3',
      }))
      expect(result).toEqual([
        { id: 1, rn: 1 },
        { id: 2, rn: 2 },
        { id: 3, rn: 3 },
      ])
      expect(getRowCount()).toBe(3)
    })

    it('should throw when combined with GROUP BY', () => {
      expect(() => executeSql({
        tables: { sales },
        query: 'SELECT region, ROW_NUMBER() OVER (ORDER BY region) AS rn FROM sales GROUP BY region',
      })).toThrow('Window functions are not supported in queries with aggregation')
    })
  })

  describe('LAG', () => {
    it('should return previous row value within partition ordered by amount', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, region, amount, LAG(amount) OVER (PARTITION BY region ORDER BY amount) AS prev FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, region: 'east', amount: 100, prev: null },
        { id: 2, region: 'east', amount: 200, prev: 150 },
        { id: 3, region: 'east', amount: 150, prev: 100 },
        { id: 4, region: 'west', amount: 300, prev: 50 },
        { id: 5, region: 'west', amount: 50, prev: null },
      ])
    })

    it('should support an explicit offset', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, LAG(amount, 2) OVER (ORDER BY id) AS prev FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, prev: null },
        { id: 2, prev: null },
        { id: 3, prev: 100 },
        { id: 4, prev: 200 },
        { id: 5, prev: 150 },
      ])
    })

    it('should return the default value when offset falls outside the partition', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, LAG(amount, 1, 0) OVER (PARTITION BY region ORDER BY amount) AS prev FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, prev: 0 },
        { id: 2, prev: 150 },
        { id: 3, prev: 100 },
        { id: 4, prev: 50 },
        { id: 5, prev: 0 },
      ])
    })

    it('should default to lag as column alias without AS', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, LAG(amount) OVER (ORDER BY id) FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, lag: null },
        { id: 2, lag: 100 },
        { id: 3, lag: 200 },
        { id: 4, lag: 150 },
        { id: 5, lag: 300 },
      ])
    })

    it('should throw on LAG without OVER', () => {
      expect(() => executeSql({
        tables: { sales },
        query: 'SELECT LAG(amount) FROM sales',
      })).toThrow('LAG() requires an OVER clause at position 7')
    })

    it('should throw on LAG with too many arguments', () => {
      expect(() => executeSql({
        tables: { sales },
        query: 'SELECT LAG(amount, 1, 0, 1) OVER (ORDER BY id) FROM sales',
      })).toThrow('LAG(value[, offset[, default]]) function requires 1-3 arguments, got 4')
    })

    it('should throw on LAG with no arguments', () => {
      expect(() => executeSql({
        tables: { sales },
        query: 'SELECT LAG() OVER (ORDER BY id) FROM sales',
      })).toThrow('LAG(value[, offset[, default]]) function requires 1-3 arguments, got 0')
    })
  })

  describe('LEAD', () => {
    it('should return next row value within partition ordered by amount', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, region, amount, LEAD(amount) OVER (PARTITION BY region ORDER BY amount) AS next FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, region: 'east', amount: 100, next: 150 },
        { id: 2, region: 'east', amount: 200, next: null },
        { id: 3, region: 'east', amount: 150, next: 200 },
        { id: 4, region: 'west', amount: 300, next: null },
        { id: 5, region: 'west', amount: 50, next: 300 },
      ])
    })

    it('should support an explicit offset and default', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, LEAD(amount, 2, -1) OVER (ORDER BY id) AS next FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, next: 150 },
        { id: 2, next: 300 },
        { id: 3, next: 50 },
        { id: 4, next: -1 },
        { id: 5, next: -1 },
      ])
    })

    it('should default to lead as column alias without AS', async () => {
      const result = await collect(executeSql({
        tables: { sales },
        query: 'SELECT id, LEAD(amount) OVER (ORDER BY id) FROM sales',
      }))
      expect(result).toEqual([
        { id: 1, lead: 200 },
        { id: 2, lead: 150 },
        { id: 3, lead: 300 },
        { id: 4, lead: 50 },
        { id: 5, lead: null },
      ])
    })

    it('should throw on LEAD without OVER', () => {
      expect(() => executeSql({
        tables: { sales },
        query: 'SELECT LEAD(amount) FROM sales',
      })).toThrow('LEAD() requires an OVER clause at position 7')
    })
  })
})
