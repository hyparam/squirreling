import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('ORDER BY', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
  ]

  it('should sort ascending by default', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users ORDER BY age' }))
    expect(result[0].age).toBe(25)
    expect(result[result.length - 1].age).toBe(35)
  })

  it('should sort descending', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users ORDER BY age DESC' }))
    expect(result[0].age).toBe(35)
    expect(result[result.length - 1].age).toBe(25)
  })

  it('should sort by multiple columns', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users ORDER BY age ASC, name DESC' }))
    expect(result[0].name).toBe('Bob') // age 25
    const age30s = result.filter(r => r.age === 30)
    expect(age30s[0].name).toBe('Eve') // DESC order
  })

  it('should handle null/undefined values in sorting', async () => {
    const data = [
      { id: 1, value: 10 },
      { id: 2, value: null },
      { id: 3, value: 5 },
    ]
    const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data ORDER BY value' }))
    expect(result[0].value).toBe(null) // null comes first
    expect(result[1].value).toBe(5)
  })

  it('should handle string sorting', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users ORDER BY name' }))
    expect(result[0].name).toBe('Alice')
    expect(result[result.length - 1].name).toBe('Eve')
  })

  it('should handle ORDER BY RANDOM()', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users ORDER BY RANDOM()' }))
    expect(result).toHaveLength(5)
    expect(result.map(r => r.id).sort()).toEqual([1, 2, 3, 4, 5])
  })

  it('should handle ORDER BY RAND()', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT * FROM users ORDER BY RAND()' }))
    expect(result).toHaveLength(5)
    expect(result.map(r => r.id).sort()).toEqual([1, 2, 3, 4, 5])
  })

  it('should handle CAST in ORDER BY clause', async () => {
    const data = [
      { path: '/file1.txt', size: '100' },
      { path: '/file2.txt', size: '50' },
      { path: '/file3.txt', size: '200' },
      { path: '/file4.txt', size: '75' },
      { path: '/file5.txt', size: '150' },
    ]
    const result = await collect(executeSql({ tables: { table: data }, query: 'SELECT path, size FROM table ORDER BY CAST(size AS INTEGER) DESC LIMIT 5' }))
    expect(result).toHaveLength(5)
    expect(result[0].path).toBe('/file3.txt') // size 200
    expect(result[1].path).toBe('/file5.txt') // size 150
    expect(result[2].path).toBe('/file1.txt') // size 100
    expect(result[3].path).toBe('/file4.txt') // size 75
    expect(result[4].path).toBe('/file2.txt') // size 50
  })

  it('should sort by column not included in SELECT', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT name FROM users ORDER BY age' }))
    // Expected order by age: Bob (25), Diana (28), Alice (30), Eve (30), Charlie (35)
    expect(result.map(r => r.name)).toEqual(['Bob', 'Diana', 'Alice', 'Eve', 'Charlie'])
  })

  describe('ORDER BY with GROUP BY', () => {
    it('should sort by GROUP BY column', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT city, COUNT(*) as cnt FROM users GROUP BY city ORDER BY city' }))
      expect(result[0].city).toBe('LA')
      expect(result[1].city).toBe('NYC')
    })

    it('should sort by GROUP BY column DESC', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT city, COUNT(*) as cnt FROM users GROUP BY city ORDER BY city DESC' }))
      expect(result[0].city).toBe('NYC')
      expect(result[1].city).toBe('LA')
    })

    it('should sort by aggregate alias', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT city, COUNT(*) as cnt FROM users GROUP BY city ORDER BY cnt DESC' }))
      // NYC has 3 users, LA has 2
      expect(result[0].city).toBe('NYC')
      expect(result[0].cnt).toBe(3)
    })

    // it('should sort by aggregate expression', async () => {
    //   const result = await collect(executeSql({ tables: { users }, query: 'SELECT city, COUNT(*) as cnt FROM users GROUP BY city ORDER BY COUNT(*) DESC' }))
    //   expect(result[0].city).toBe('NYC')
    //   expect(result[0].cnt).toBe(3)
    // })
  })

  describe('NULLS FIRST and NULLS LAST', () => {
    it('should handle NULLS FIRST with ASC', async () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 5 },
        { id: 4, value: null },
        { id: 5, value: 20 },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data ORDER BY value ASC NULLS FIRST' }))
      expect(result[0].value).toBe(null)
      expect(result[1].value).toBe(null)
      expect(result[2].value).toBe(5)
      expect(result[3].value).toBe(10)
      expect(result[4].value).toBe(20)
    })

    it('should handle NULLS LAST with ASC', async () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 5 },
        { id: 4, value: null },
        { id: 5, value: 20 },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data ORDER BY value ASC NULLS LAST' }))
      expect(result[0].value).toBe(5)
      expect(result[1].value).toBe(10)
      expect(result[2].value).toBe(20)
      expect(result[3].value).toBe(null)
      expect(result[4].value).toBe(null)
    })

    it('should handle NULLS FIRST with DESC', async () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 5 },
        { id: 4, value: null },
        { id: 5, value: 20 },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data ORDER BY value DESC NULLS FIRST' }))
      expect(result[0].value).toBe(null)
      expect(result[1].value).toBe(null)
      expect(result[2].value).toBe(20)
      expect(result[3].value).toBe(10)
      expect(result[4].value).toBe(5)
    })

    it('should handle NULLS LAST with DESC', async () => {
      const data = [
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 5 },
        { id: 4, value: null },
        { id: 5, value: 20 },
      ]
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT * FROM data ORDER BY value DESC NULLS LAST' }))
      expect(result[0].value).toBe(20)
      expect(result[1].value).toBe(10)
      expect(result[2].value).toBe(5)
      expect(result[3].value).toBe(null)
      expect(result[4].value).toBe(null)
    })
  })
})
