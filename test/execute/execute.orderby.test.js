import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * @import { UserDefinedFunction } from '../../src/index.js'
 */

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

  it('should support positional reference to derived column', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT name, age FROM users ORDER BY 2 DESC' }))
    expect(result.map(r => r.age)).toEqual([35, 30, 30, 28, 25])
  })

  it('should support positional reference to aliased expression', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT name, age * 2 AS doubled FROM users ORDER BY 2 ASC' }))
    expect(result[0].doubled).toBe(50)
    expect(result[4].doubled).toBe(70)
  })

  it('should support multiple positional references', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT city, age, name FROM users ORDER BY 1 ASC, 2 DESC' }))
    expect(result[0]).toEqual({ city: 'LA', age: 28, name: 'Diana' })
    expect(result[1]).toEqual({ city: 'LA', age: 25, name: 'Bob' })
    expect(result[2]).toEqual({ city: 'NYC', age: 35, name: 'Charlie' })
  })

  it('should throw for out-of-range ORDER BY position', () => {
    expect(() => executeSql({ tables: { users }, query: 'SELECT name FROM users ORDER BY 5' })).toThrow(/position 5 is out of range/)
  })

  it('should throw for ORDER BY positional reference to *', () => {
    expect(() => executeSql({ tables: { users }, query: 'SELECT * FROM users ORDER BY 1' })).toThrow(/refers to \* which is not supported/)
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

  it('should sort by SELECT alias', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT id AS user_id, name FROM users ORDER BY user_id DESC' }))
    // Expected order by id DESC: 5, 4, 3, 2, 1
    expect(result.map(r => r.user_id)).toEqual([5, 4, 3, 2, 1])
  })

  it('should sort by SELECT alias with expression', async () => {
    const result = await collect(executeSql({ tables: { users }, query: 'SELECT age * 2 AS double_age, name FROM users ORDER BY double_age DESC' }))
    // Expected order by age DESC: Charlie (70), Alice (60), Eve (60), Diana (56), Bob (50)
    expect(result[0].name).toBe('Charlie')
    expect(result[0].double_age).toBe(70)
    expect(result[result.length - 1].name).toBe('Bob')
    expect(result[result.length - 1].double_age).toBe(50)
  })

  it('should sort by expression alias', async () => {
    const data = [
      { x: 3 },
      { x: 1 },
      { x: 2 },
    ]
    const result = await collect(executeSql({ tables: { data }, query: 'SELECT x + 1 AS y FROM data ORDER BY y' }))
    expect(result).toEqual([{ y: 2 }, { y: 3 }, { y: 4 }])
  })

  it('should sort by alias used inside ORDER BY expression', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT id AS uid, name FROM users ORDER BY uid * -1 DESC',
    }))
    // uid * -1 DESC means smallest uid first
    expect(result.map(r => r.uid)).toEqual([1, 2, 3, 4, 5])
  })

  it('should sort by alias used inside ORDER BY function', async () => {
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT id AS uid, name FROM users ORDER BY ABS(uid - 3)',
    }))
    // Distance from 3: id=3 (0), id=2,4 (1), id=1,5 (2)
    expect(result[0].uid).toBe(3)
  })

  it('should sort column names with spaces', async () => {
    const users = [
      { id: 1, 'full name': 'Charlie', score: 85 },
      { id: 2, 'full name': 'Alice', score: 95 },
      { id: 3, 'full name': 'Bob', score: 90 },
    ]
    const result = await collect(executeSql({
      tables: { users },
      query: 'SELECT "full name", score FROM users ORDER BY "full name"',
    }))
    expect(result).toHaveLength(3)
    expect(result[0]['full name']).toBe('Alice')
    expect(result[1]['full name']).toBe('Bob')
    expect(result[2]['full name']).toBe('Charlie')
  })

  it('should sort mixed types', async () => {
    const data = [
      { id: 1, value: 10 },
      { id: 2, value: '5' },
      { id: 3, value: 20 },
      { id: 4, value: 15n },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT * FROM data ORDER BY value',
    }))
    // Should coerce types
    expect(result[0].value).toBe('5')
    expect(result[1].value).toBe(10)
    expect(result[2].value).toBe(15n)
    expect(result[3].value).toBe(20)
  })

  it('should sort bigint values correctly', async () => {
    const data = [
      { id: 1, value: 10n },
      { id: 2, value: 5n },
      { id: 3, value: 20n },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT * FROM data ORDER BY value DESC',
    }))
    expect(result[0].value).toBe(20n)
    expect(result[1].value).toBe(10n)
    expect(result[2].value).toBe(5n)
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

    it('should sort many groups without overflowing the call stack', async () => {
      const data = Array.from({ length: 200000 }, (_, i) => ({ g: i }))
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT g, COUNT(*) AS cnt FROM data GROUP BY g ORDER BY cnt DESC LIMIT 5',
      }))
      expect(result).toHaveLength(5)
      expect(result.map(r => r.cnt)).toEqual([1, 1, 1, 1, 1])
    })

    it('should sort by aggregate expression without alias', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT city, COUNT(*) FROM users GROUP BY city ORDER BY COUNT(*)' }))
      expect(result[0].city).toBe('LA')
      expect(result[0].count_all).toBe(2)
    })

    it('should sort by aggregate expression without alias or group by', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT COUNT(*) FROM users ORDER BY COUNT(*)' }))
      expect(result[0].count_all).toBe(5)
    })

    it('should sort by GROUP BY expression alias', async () => {
      const data = [
        { value: 1 },
        { value: 2 },
        { value: 3 },
        { value: 4 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT value % 2 AS parity, COUNT(*) AS count FROM data GROUP BY parity ORDER BY parity DESC',
      }))
      expect(result).toEqual([{ parity: 1, count: 2 }, { parity: 0, count: 2 }])
    })

    it('should sort by SELECT alias nested inside aggregate ORDER BY expression', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT age AS a, COUNT(*) AS count FROM users GROUP BY a ORDER BY SUM(a)',
      }))
      expect(result).toEqual([
        { a: 25, count: 1 },
        { a: 28, count: 1 },
        { a: 35, count: 1 },
        { a: 30, count: 2 },
      ])
    })

    it('should sort by aggregate expression not selected by alias', async () => {
      const result = await collect(executeSql({ tables: { users }, query: 'SELECT city, COUNT(*) as cnt FROM users GROUP BY city ORDER BY COUNT(*)' }))
      expect(result).toEqual([{ city: 'LA', cnt: 2 }, { city: 'NYC', cnt: 3 }])
    })

    it('should sort by an arithmetic expression containing aggregates', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT city, COUNT(*) AS cnt
          FROM users
          GROUP BY city
          ORDER BY (SUM(CASE WHEN active THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) DESC
        `,
      }))
      // LA is 100% active (2/2); NYC is 67% active (2/3) — LA sorts first DESC.
      expect(result[0].city).toBe('LA')
      expect(result[1].city).toBe('NYC')
    })

    it('should sort by aggregate / aggregate ratio', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT city, COUNT(*) AS cnt
          FROM users
          GROUP BY city
          ORDER BY SUM(age) / COUNT(*) DESC
        `,
      }))
      // NYC avg age 31.67 (95/3); LA avg age 26.5 (53/2). NYC first DESC.
      expect(result[0].city).toBe('NYC')
      expect(result[1].city).toBe('LA')
    })

    it('should sort by an aggregate plus a constant', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT city, COUNT(*) AS cnt
          FROM users
          GROUP BY city
          ORDER BY (COUNT(*) + 0) DESC
        `,
      }))
      expect(result[0].city).toBe('NYC')
      expect(result[1].city).toBe('LA')
    })

    it('should evaluate grouped sort-key UDF concurrently', async () => {
      const data = Array.from({ length: 300 }, (_, i) => ({ g: 299 - i }))
      let inFlight = 0
      let peak = 0
      /** @type {Record<string, UserDefinedFunction>} */
      const functions = {
        SLOW: {
          async apply(x) {
            inFlight++
            peak = Math.max(peak, inFlight)
            await new Promise(r => setTimeout(r, 1))
            inFlight--
            return x
          },
          arguments: { min: 1, max: 1 },
        },
      }
      const result = await collect(executeSql({
        tables: { data },
        functions,
        query: 'SELECT g, COUNT(*) AS cnt FROM data GROUP BY g ORDER BY SLOW(g) LIMIT 5',
      }))
      expect(result).toEqual([
        { g: 0, cnt: 1 },
        { g: 1, cnt: 1 },
        { g: 2, cnt: 1 },
        { g: 3, cnt: 1 },
        { g: 4, cnt: 1 },
      ])
      expect(peak).toBeGreaterThan(10)
    })
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

  describe('ORDER BY with async UDF', () => {
    const data = Array.from({ length: 300 }, (_, i) => ({ id: i, val: i * 37 % 300 }))

    it('should evaluate sort-key UDF concurrently', async () => {
      let inFlight = 0
      let peak = 0
      /** @type {Record<string, UserDefinedFunction>} */
      const functions = {
        SLOW: {
          async apply(x) {
            inFlight++
            peak = Math.max(peak, inFlight)
            await new Promise(r => setTimeout(r, 1))
            inFlight--
            return x
          },
          arguments: { min: 1, max: 1 },
        },
      }
      const result = await collect(executeSql({
        tables: { data }, functions,
        query: 'SELECT id, SLOW(val) AS k FROM data ORDER BY k LIMIT 5',
      }))
      expect(result).toHaveLength(5)
      expect(result.map(r => r.k)).toEqual([0, 1, 2, 3, 4])
      // Sequential evaluation would yield peak=1; concurrent evaluation
      // should reach the chunk ceiling of 256.
      expect(peak).toBeGreaterThan(10)
    })

    it('should not re-invoke the sort-key UDF in the output projection', async () => {
      let calls = 0
      /** @type {Record<string, UserDefinedFunction>} */
      const functions = {
        TAG: {
          apply(x) { calls++; return x },
          arguments: { min: 1, max: 1 },
        },
      }
      await collect(executeSql({
        tables: { data }, functions,
        query: 'SELECT id, TAG(val) AS label FROM data ORDER BY label LIMIT 10',
      }))
      // Without cache writeback the projection re-invokes TAG for each of the
      // 10 output rows, giving 310 calls. With writeback it should be exactly
      // one call per source row.
      expect(calls).toBe(data.length)
    })
  })
})
