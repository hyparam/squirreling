import { describe, expect, it } from 'vitest'
import { executeSql } from '../src/execute.js'

describe('executeSql - GROUP BY', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
  ]

  it('should group by single column', () => {
    const result = executeSql(users, 'SELECT city, COUNT(*) AS count FROM users GROUP BY city')
    expect(result).toHaveLength(2)
    const nycGroup = result.find(r => r.city === 'NYC')
    const laGroup = result.find(r => r.city === 'LA')
    expect(nycGroup?.count).toBe(3)
    expect(laGroup?.count).toBe(2)
  })

  it('should group by multiple columns', () => {
    const result = executeSql(users, 'SELECT city, age, COUNT(*) AS count FROM users GROUP BY city, age')
    expect(result.length).toBeGreaterThan(2)
    const nycAge30 = result.find(r => r.city === 'NYC' && r.age === 30)
    expect(nycAge30?.count).toBe(2)
  })

  it('should handle aggregates with GROUP BY', () => {
    const result = executeSql(users, 'SELECT city, AVG(age) AS avg_age FROM users GROUP BY city')
    expect(result).toHaveLength(2)
    const nycGroup = result.find(r => r.city === 'NYC')
    expect(nycGroup?.avg_age).toBeCloseTo(31.67, 1)
  })

  it('should select non-grouped column (takes first value)', () => {
    const result = executeSql(users, 'SELECT city, name, COUNT(*) AS count FROM users GROUP BY city')
    expect(result).toHaveLength(2)
    expect(result.every(r => r.name)).toBe(true)
  })

  it('should handle empty groups with SELECT *', () => {
    const data = [
      { id: 1, city: 'NYC', age: 30 },
      { id: 2, city: 'LA', age: 25 },
    ]
    // Filter creates empty result, then GROUP BY
    const result = executeSql(data, 'SELECT * FROM users WHERE age > 100 GROUP BY city')
    expect(result).toEqual([])
  })
})
