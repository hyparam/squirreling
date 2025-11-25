import { describe, expect, it } from 'vitest'
import { executeSql } from '../../src/execute/execute.js'

describe('executeSql - GROUP BY', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
  ]

  it('should group by single column', () => {
    const result = executeSql({ tables: { users }, query: 'SELECT city, COUNT(*) AS count FROM users GROUP BY city' })
    expect(result).toHaveLength(2)
    const nycGroup = result.find(r => r.city === 'NYC')
    const laGroup = result.find(r => r.city === 'LA')
    expect(nycGroup?.count).toBe(3)
    expect(laGroup?.count).toBe(2)
  })

  it('should group by multiple columns', () => {
    const result = executeSql({ tables: { users }, query: 'SELECT city, age, COUNT(*) AS count FROM users GROUP BY city, age' })
    expect(result.length).toBeGreaterThan(2)
    const nycAge30 = result.find(r => r.city === 'NYC' && r.age === 30)
    expect(nycAge30?.count).toBe(2)
  })

  it('should handle aggregates with GROUP BY', () => {
    const result = executeSql({ tables: { users }, query: 'SELECT city, AVG(age) AS avg_age FROM users GROUP BY city' })
    expect(result).toHaveLength(2)
    const nycGroup = result.find(r => r.city === 'NYC')
    expect(nycGroup?.avg_age).toBeCloseTo(31.67, 1)
  })

  it('should select non-grouped column (takes first value)', () => {
    const result = executeSql({ tables: { users }, query: 'SELECT city, name, COUNT(*) AS count FROM users GROUP BY city' })
    expect(result).toHaveLength(2)
    expect(result.every(r => r.name)).toBe(true)
  })

  it('should handle empty groups with SELECT *', () => {
    const data = [
      { id: 1, city: 'NYC', age: 30 },
      { id: 2, city: 'LA', age: 25 },
    ]
    // Filter creates empty result, then GROUP BY
    const result = executeSql({ tables: { data }, query: 'SELECT * FROM data WHERE age > 100 GROUP BY city' })
    expect(result).toEqual([])
  })

  it('should group by multiple columns with ORDER BY', () => {
    const result = executeSql({ tables: { users }, query: `
      SELECT city, active, COUNT(*) AS count
      FROM users
      GROUP BY city, active
      ORDER BY city, active DESC
    ` })
    expect(result.length).toBeGreaterThan(0)
    expect(result.every(r => r.count > 0)).toBe(true)
  })

  it('should aggregate with multiple group columns', () => {
    const sales = [
      { region: 'North', product: 'A', amount: 100 },
      { region: 'North', product: 'B', amount: 150 },
      { region: 'South', product: 'A', amount: 200 },
      { region: 'South', product: 'B', amount: 120 },
      { region: 'North', product: 'A', amount: 80 },
    ]
    const result = executeSql({ tables: { sales }, query: `
      SELECT region, product, SUM(amount) AS total, COUNT(*) AS sales_count
      FROM sales
      GROUP BY region, product
      ORDER BY region, product
    ` })
    const northA = result.find(r => r.region === 'North' && r.product === 'A')
    expect(northA?.total).toBe(180)
    expect(northA?.sales_count).toBe(2)
  })
})
