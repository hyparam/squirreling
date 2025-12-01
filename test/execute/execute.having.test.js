import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('executeSql - HAVING clause', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
    { id: 6, name: 'Frank', age: 22, city: 'Chicago', active: true },
  ]

  it('should filter groups with HAVING COUNT(*)', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) > 2',
    }))
    expect(result).toHaveLength(1)
    expect(result[0]).toEqual({ city: 'NYC', cnt: 3 })
  })

  it('should filter groups with HAVING on aggregate comparison', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, AVG(age) AS avg_age FROM users GROUP BY city HAVING AVG(age) > 28',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.avg_age).toBeGreaterThan(28)
    }
  })

  it('should filter groups with HAVING using column reference', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING city = \'NYC\'',
    }))
    expect(result).toHaveLength(1)
    expect(result[0]).toEqual({ city: 'NYC', cnt: 3 })
  })

  it('should handle HAVING with multiple conditions using AND', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt, AVG(age) AS avg_age FROM users GROUP BY city HAVING COUNT(*) >= 2 AND AVG(age) > 25',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.cnt).toBeGreaterThanOrEqual(2)
      expect(row.avg_age).toBeGreaterThan(25)
    }
  })

  it('should handle HAVING with OR condition', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) > 2 OR city = \'Chicago\'',
    }))
    expect(result.length).toBeGreaterThan(0)
    const cities = result.map(r => r.city)
    expect(cities).toContain('NYC')
    expect(cities).toContain('Chicago')
  })

  it('should combine WHERE, GROUP BY, and HAVING', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users WHERE age > 25 GROUP BY city HAVING COUNT(*) >= 2',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.cnt).toBeGreaterThanOrEqual(2)
    }
  })

  it('should handle HAVING with SUM aggregate', async () => {
    const sales = [
      { region: 'North', product: 'A', amount: 100 },
      { region: 'North', product: 'A', amount: 150 },
      { region: 'South', product: 'A', amount: 50 },
      { region: 'North', product: 'B', amount: 80 },
    ]
    const result = await collect(executeSql({
      tables: { sales },
      query: 'SELECT region, SUM(amount) AS total FROM sales GROUP BY region HAVING SUM(amount) > 200',
    }))
    expect(result).toHaveLength(1)
    expect(result[0]).toEqual({ region: 'North', total: 330 })
  })

  it('should handle HAVING with MIN and MAX', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, MIN(age) AS min_age, MAX(age) AS max_age FROM users GROUP BY city HAVING MAX(age) > 30',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.max_age).toBeGreaterThan(30)
    }
  })

  it('should handle complex query with WHERE, GROUP BY, HAVING, ORDER BY, and LIMIT', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: `SELECT city, COUNT(*) AS cnt, AVG(age) AS avg_age
       FROM users
       WHERE active = TRUE
       GROUP BY city
       HAVING COUNT(*) >= 2
       ORDER BY cnt DESC
       LIMIT 2`,
    }))
    expect(result.length).toBeLessThanOrEqual(2)
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.cnt).toBeGreaterThanOrEqual(2)
    }
  })

  it('should return empty result when no groups satisfy HAVING', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) > 10',
    }))
    expect(result).toEqual([])
  })

  it('should handle HAVING with inequality operators', async () => {
    const result1 = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) <= 2',
    }))
    expect(result1.length).toBeGreaterThan(0)

    const result2 = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) != 3',
    }))
    expect(result2.length).toBeGreaterThan(0)
  })

  it('should handle HAVING with COUNT on specific column', async () => {
    const data = [
      { city: 'NYC', name: 'Alice' },
      { city: 'NYC', name: 'Bob' },
      { city: 'NYC', name: null },
      { city: 'LA', name: 'Charlie' },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT city, COUNT(name) AS cnt FROM data GROUP BY city HAVING COUNT(name) >= 2',
    }))
    expect(result).toHaveLength(1)
    expect(result[0]).toEqual({ city: 'NYC', cnt: 2 })
  })

  it('should handle HAVING with >= operator', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, AVG(age) AS avg_age FROM users GROUP BY city HAVING AVG(age) >= 30',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.avg_age).toBeGreaterThanOrEqual(30)
    }
  })

  it('should handle HAVING with LIKE operator', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING city LIKE \'N%\'',
    }))
    expect(result).toHaveLength(1)
    expect(result[0].city).toBe('NYC')
  })

  it('should handle HAVING with LIKE pattern matching', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING city LIKE \'%A\'',
    }))
    expect(result.length).toBeGreaterThan(0)
    expect(result.some(r => r.city === 'LA')).toBe(true)
  })

  it('should handle HAVING with BETWEEN operator', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, AVG(age) AS avg_age FROM users GROUP BY city HAVING AVG(age) BETWEEN 25 AND 30',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.avg_age).toBeGreaterThanOrEqual(25)
      expect(row.avg_age).toBeLessThanOrEqual(30)
    }
  })

  it('should handle HAVING with NOT BETWEEN operator', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING COUNT(*) NOT BETWEEN 2 AND 2',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.cnt).not.toBe(2)
    }
  })

  it('should handle HAVING with BETWEEN and NULL values', async () => {
    const data = [
      { category: 'A', value: 10 },
      { category: 'A', value: null },
      { category: 'B', value: 5 },
      { category: 'B', value: 15 },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT category, MIN(value) AS min_val FROM data GROUP BY category HAVING MIN(value) BETWEEN 5 AND 10',
    }))
    expect(result.length).toBeGreaterThan(0)
  })

  it('should handle HAVING with MIN aggregate function', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, MIN(age) AS min_age FROM users GROUP BY city HAVING MIN(age) > 23',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.min_age).toBeGreaterThan(23)
    }
  })

  it('should handle HAVING with IS NULL', async () => {
    const data = [
      { category: 'A', value: 10 },
      { category: 'A', value: null },
      { category: 'B', value: null },
      { category: 'B', value: null },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT category, MIN(value) AS min_val FROM data GROUP BY category HAVING MIN(value) IS NULL',
    }))
    expect(result.length).toBeGreaterThan(0)
    expect(result.some(r => r.min_val === null)).toBe(true)
  })

  it('should handle HAVING with IS NOT NULL', async () => {
    const data = [
      { category: 'A', value: 10 },
      { category: 'A', value: 20 },
      { category: 'B', value: null },
      { category: 'B', value: null },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT category, MIN(value) AS min_val FROM data GROUP BY category HAVING MIN(value) IS NOT NULL',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.min_val).not.toBeNull()
    }
  })

  it('should handle HAVING with NOT operator', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING NOT (COUNT(*) > 2)',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(row.cnt).toBeLessThanOrEqual(2)
    }
  })

  it('should handle NULL comparisons in HAVING with comparison operators', async () => {
    const data = [
      { category: 'A', value: 10 },
      { category: 'A', value: null },
      { category: 'B', value: null },
      { category: 'B', value: null },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT category, MIN(value) AS min_val FROM data GROUP BY category HAVING MIN(value) < 20',
    }))
    // NULL values should be excluded from < comparison
    expect(result.some(r => r.min_val !== null)).toBe(true)
  })

  it('should handle NULL equality comparisons in HAVING', async () => {
    const data = [
      { category: 'A', value: 10 },
      { category: 'A', value: null },
      { category: 'B', value: null },
      { category: 'B', value: null },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT category, MIN(value) AS min_val FROM data GROUP BY category HAVING MIN(value) = 10',
    }))
    // Only category A with min value 10 should match
    expect(result).toHaveLength(1)
    expect(result[0].category).toBe('A')
  })

  it('should handle BETWEEN with NULL bounds', async () => {
    const data = [
      { category: 'A', value: 10 },
      { category: 'A', value: 20 },
      { category: 'B', value: 5 },
      { category: 'C', value: null },
    ]
    // This tests the NULL handling in BETWEEN (line 116-117)
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT category, MAX(value) AS max_val FROM data GROUP BY category HAVING MAX(value) BETWEEN 10 AND 25',
    }))
    expect(result.length).toBeGreaterThan(0)
    // Category C with NULL should be excluded
    expect(result.every(r => r.max_val !== null)).toBe(true)
  })

  it('should handle aggregate function as direct boolean in HAVING', async () => {
    const data = [
      { category: 'A', value: 0 },
      { category: 'B', value: 5 },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT category, COUNT(*) AS cnt FROM data GROUP BY category HAVING COUNT(*)',
    }))
    // All groups with non-zero count should pass
    expect(result.length).toBeGreaterThan(0)
  })

  it('should handle IN operator in HAVING', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) AS cnt FROM users GROUP BY city HAVING city IN (\'NYC\', \'LA\')',
    }))
    expect(result.length).toBeGreaterThan(0)
    for (const row of result) {
      expect(['NYC', 'LA']).toContain(row.city)
    }
  })
})
