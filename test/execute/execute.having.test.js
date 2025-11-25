import { describe, expect, it } from 'vitest'
import { executeSql } from '../../src/execute/execute.js'

describe('executeSql - HAVING clause', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
    { id: 6, name: 'Frank', age: 22, city: 'Chicago', active: true },
  ]

  it('should filter groups with HAVING COUNT(*)', () => {
    const result = executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) > 2',
    })
    expect(result).toHaveLength(1)
    expect(result[0]).toEqual({ city: 'NYC', cnt: 3 })
  })

  it('should filter groups with HAVING on aggregate comparison', () => {
    const result = executeSql({
      tables: { users },
      query: 'SELECT city, AVG(age) AS avg_age FROM users GROUP BY city HAVING AVG(age) > 28',
    })
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.avg_age).toBeGreaterThan(28)
    }
  })

  it('should filter groups with HAVING using column reference', () => {
    const result = executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING city = \'NYC\'',
    })
    expect(result).toHaveLength(1)
    expect(result[0]).toEqual({ city: 'NYC', cnt: 3 })
  })

  it('should handle HAVING with multiple conditions using AND', () => {
    const result = executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt, AVG(age) AS avg_age FROM users GROUP BY city HAVING COUNT(*) >= 2 AND AVG(age) > 25',
    })
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.cnt).toBeGreaterThanOrEqual(2)
      expect(row.avg_age).toBeGreaterThan(25)
    }
  })

  it('should handle HAVING with OR condition', () => {
    const result = executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) > 2 OR city = \'Chicago\'',
    })
    expect(result.length).toBeGreaterThan(0)
    const cities = result.map(r => r.city)
    expect(cities).toContain('NYC')
    expect(cities).toContain('Chicago')
  })

  it('should combine WHERE, GROUP BY, and HAVING', () => {
    const result = executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users WHERE age > 25 GROUP BY city HAVING COUNT(*) >= 2',
    })
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.cnt).toBeGreaterThanOrEqual(2)
    }
  })

  it('should handle HAVING with SUM aggregate', () => {
    const sales = [
      { region: 'North', product: 'A', amount: 100 },
      { region: 'North', product: 'A', amount: 150 },
      { region: 'South', product: 'A', amount: 50 },
      { region: 'North', product: 'B', amount: 80 },
    ]
    const result = executeSql({
      tables: { sales },
      query: 'SELECT region, SUM(amount) AS total FROM sales GROUP BY region HAVING SUM(amount) > 200',
    })
    expect(result).toHaveLength(1)
    expect(result[0]).toEqual({ region: 'North', total: 330 })
  })

  it('should handle HAVING with MIN and MAX', () => {
    const result = executeSql({
      tables: { users },
      query: 'SELECT city, MIN(age) AS min_age, MAX(age) AS max_age FROM users GROUP BY city HAVING MAX(age) > 30',
    })
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.max_age).toBeGreaterThan(30)
    }
  })

  it('should handle complex query with WHERE, GROUP BY, HAVING, ORDER BY, and LIMIT', () => {
    const result = executeSql({
      tables: { users },
      query: `SELECT city, COUNT(*) AS cnt, AVG(age) AS avg_age
       FROM users
       WHERE active = TRUE
       GROUP BY city
       HAVING COUNT(*) >= 2
       ORDER BY cnt DESC
       LIMIT 2`,
    })
    expect(result.length).toBeLessThanOrEqual(2)
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.cnt).toBeGreaterThanOrEqual(2)
    }
  })

  it('should return empty result when no groups satisfy HAVING', () => {
    const result = executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) > 10',
    })
    expect(result).toEqual([])
  })

  it('should handle HAVING with inequality operators', () => {
    const result1 = executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) <= 2',
    })
    expect(result1.length).toBeGreaterThan(0)

    const result2 = executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) != 3',
    })
    expect(result2.length).toBeGreaterThan(0)
  })

  it('should handle HAVING with COUNT on specific column', () => {
    const data = [
      { city: 'NYC', name: 'Alice' },
      { city: 'NYC', name: 'Bob' },
      { city: 'NYC', name: null },
      { city: 'LA', name: 'Charlie' },
    ]
    const result = executeSql({
      tables: { data },
      query: 'SELECT city, COUNT(name) AS cnt FROM data GROUP BY city HAVING COUNT(name) >= 2',
    })
    expect(result).toHaveLength(1)
    expect(result[0]).toEqual({ city: 'NYC', cnt: 2 })
  })
})
