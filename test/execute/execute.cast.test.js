import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('CAST calls', () => {
  const users = [
    { id: 1, name: 'Alice', age: '30', city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: '25', city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: '35', city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: '28', city: 'LA', active: true },
    { id: 5, name: 'Eve', age: '5', city: 'NYC', active: true },
  ]

  it('should handle CAST to INTEGER in SELECT', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT id, CAST(age AS INTEGER) as age_int FROM users',
    }))
    expect(result).toHaveLength(5)
    expect(result[0].age_int).toBe(30)
    expect(result[1].age_int).toBe(25)
    expect(result[2].age_int).toBe(35)
  })

  it('should handle CAST in WHERE clause', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT * FROM users WHERE CAST(age AS INTEGER) > \'28\'',
    }))
    expect(result).toHaveLength(2)
    // Without CAST, Eve (age '5') would be included
    expect(result.map(r => r.id).sort()).toEqual([1, 3])
  })

  it('should handle CAST in HAVING clause', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT city, COUNT(*) as total FROM users GROUP BY city HAVING total > CAST(\'1\' AS INTEGER)',
    }))
    expect(result).toHaveLength(2)
    expect(result.map(r => r.city).sort()).toEqual(['LA', 'NYC'])
  })

  it('should handle CAST object to STRING as JSON', async () => {
    // bigint serialization test
    const data = [
      { id: 1, info: { id: 1n, name: 'Alice', age: 30 } },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT CAST(info AS STRING) as info_str FROM data',
    }))
    expect(result).toHaveLength(1)
    expect(result[0].info_str).toBe('{"id":1,"name":"Alice","age":30}')
  })
})
