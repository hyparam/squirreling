import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

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

    it('should throw when combined with GROUP BY', () => {
      expect(() => executeSql({
        tables: { sales },
        query: 'SELECT region, ROW_NUMBER() OVER (ORDER BY region) AS rn FROM sales GROUP BY region',
      })).toThrow('Window functions are not supported in queries with aggregation')
    })
  })
})
