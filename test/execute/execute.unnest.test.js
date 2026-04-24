import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('UNNEST', () => {
  it('should expand a numeric array into rows', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT * FROM UNNEST([1, 2, 3]) AS t',
    }))
    expect(result).toEqual([{ unnest: 1 }, { unnest: 2 }, { unnest: 3 }])
  })

  it('should expand a string array into rows', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT * FROM UNNEST([\'a\', \'b\', \'c\']) AS t',
    }))
    expect(result).toEqual([{ unnest: 'a' }, { unnest: 'b' }, { unnest: 'c' }])
  })

  it('should support a column alias like AS t(x)', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT x FROM UNNEST([10, 20, 30]) AS t(x)',
    }))
    expect(result).toEqual([{ x: 10 }, { x: 20 }, { x: 30 }])
  })

  it('should support projecting with the column alias in expressions', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT x * 2 AS doubled FROM UNNEST([1, 2, 3]) AS t(x)',
    }))
    expect(result).toEqual([{ doubled: 2 }, { doubled: 4 }, { doubled: 6 }])
  })

  it('should produce zero rows for an empty array', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT * FROM UNNEST([]) AS t',
    }))
    expect(result).toEqual([])
  })

  it('should produce zero rows for a NULL argument', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT * FROM UNNEST(NULL) AS t',
    }))
    expect(result).toEqual([])
  })

  it('should work with WHERE', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT x FROM UNNEST([10, 20, 30, 40]) AS t(x) WHERE x > 15',
    }))
    expect(result).toEqual([{ x: 20 }, { x: 30 }, { x: 40 }])
  })

  it('should work with ORDER BY and LIMIT', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT x FROM UNNEST([30, 10, 20]) AS t(x) ORDER BY x DESC LIMIT 2',
    }))
    expect(result).toEqual([{ x: 30 }, { x: 20 }])
  })

  it('should work with aggregation', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT SUM(x) AS total FROM UNNEST([1, 2, 3, 4]) AS t(x)',
    }))
    expect(result).toEqual([{ total: 10 }])
  })

  it('should throw for wrong argument count', () => {
    expect(() => executeSql({
      tables: {},
      query: 'SELECT * FROM UNNEST() AS t',
    })).toThrow('UNNEST(array) function requires 1 argument, got 0')
  })

  it('should throw when used as a scalar expression', () => {
    expect(() => executeSql({
      tables: {},
      query: 'SELECT UNNEST([1, 2, 3])',
    })).toThrow('UNNEST is a table function and can only be used in FROM clauses at position 7')
  })

  it('should throw when the argument references a column', () => {
    expect(() => executeSql({
      tables: { t: [{ arr: [1, 2] }] },
      query: 'SELECT * FROM UNNEST(arr) AS u',
    })).toThrow('UNNEST argument cannot reference column "arr" — use JOIN UNNEST(...) to reference columns from another table')
  })

  it('should throw when the argument uses a qualified column ref', () => {
    expect(() => executeSql({
      tables: { t: [{ arr: [1, 2] }] },
      query: 'SELECT * FROM UNNEST(t.arr) AS u',
    })).toThrow('UNNEST argument cannot reference column "t.arr" — use JOIN UNNEST(...) to reference columns from another table')
  })

  it('should throw when a column is referenced on the LHS of IN', () => {
    expect(() => executeSql({
      tables: { t: [{ arr: [1, 2] }], nums: [{ n: 1 }] },
      query: 'SELECT * FROM UNNEST(arr IN (SELECT n FROM nums)) AS u',
    })).toThrow('UNNEST argument cannot reference column "arr" — use JOIN UNNEST(...) to reference columns from another table')
  })

  it('should throw for multi-column aliases', () => {
    expect(() => executeSql({
      tables: {},
      query: 'SELECT * FROM UNNEST([1, 2]) AS t(x, y)',
    })).toThrow('UNNEST produces a single column; only one column alias is allowed')
  })

  it('should stop yielding when the signal aborts', async () => {
    const controller = new AbortController()
    const iter = executeSql({
      tables: {},
      query: 'SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS t(x)',
      signal: controller.signal,
    }).rows()
    const first = await iter.next()
    expect(await first.value?.cells.x()).toBe(1)
    controller.abort()
    const second = await iter.next()
    expect(second.done).toBe(true)
  })

  it('should join UNNEST on the left side with a regular table', async () => {
    const labels = [
      { id: 1, name: 'one' },
      { id: 2, name: 'two' },
      { id: 3, name: 'three' },
    ]
    const result = await collect(executeSql({
      tables: { labels },
      query: 'SELECT x, name FROM UNNEST([2, 3]) AS t(x) JOIN labels ON labels.id = t.x',
    }))
    expect(result).toEqual([
      { x: 2, name: 'two' },
      { x: 3, name: 'three' },
    ])
  })
})

describe('LATERAL UNNEST', () => {
  it('should expand an array column from the left-side row (qualified ref)', async () => {
    const t = [
      { id: 1, arr: [10, 20] },
      { id: 2, arr: [30] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, u.x FROM t JOIN UNNEST(t.arr) AS u(x) ON TRUE',
    }))
    expect(result).toEqual([
      { id: 1, x: 10 },
      { id: 1, x: 20 },
      { id: 2, x: 30 },
    ])
  })

  it('should resolve an unqualified ref from the left-side row', async () => {
    const t = [
      { id: 1, arr: [10, 20] },
      { id: 2, arr: [30] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, u.x FROM t JOIN UNNEST(arr) AS u(x) ON TRUE',
    }))
    expect(result).toEqual([
      { id: 1, x: 10 },
      { id: 1, x: 20 },
      { id: 2, x: 30 },
    ])
  })

  it('should accept an explicit LATERAL keyword', async () => {
    const t = [
      { id: 1, arr: [1, 2] },
      { id: 2, arr: [3] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, u.x FROM t JOIN LATERAL UNNEST(t.arr) AS u(x) ON TRUE',
    }))
    expect(result).toEqual([
      { id: 1, x: 1 },
      { id: 1, x: 2 },
      { id: 2, x: 3 },
    ])
  })

  it('should filter rows with an ON condition', async () => {
    const t = [
      { id: 1, arr: [1, 2, 3] },
      { id: 2, arr: [4, 5] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, u.x FROM t JOIN UNNEST(t.arr) AS u(x) ON u.x > 2',
    }))
    expect(result).toEqual([
      { id: 1, x: 3 },
      { id: 2, x: 4 },
      { id: 2, x: 5 },
    ])
  })

  it('should LEFT JOIN yielding a NULL row for empty arrays', async () => {
    const t = [
      { id: 1, arr: [10, 20] },
      { id: 2, arr: [] },
      { id: 3, arr: null },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, u.x FROM t LEFT JOIN UNNEST(t.arr) AS u(x) ON TRUE',
    }))
    expect(result).toEqual([
      { id: 1, x: 10 },
      { id: 1, x: 20 },
      { id: 2, x: null },
      { id: 3, x: null },
    ])
  })

  it('should support two chained lateral UNNESTs', async () => {
    const t = [
      { id: 1, a: [1, 2], b: ['x', 'y'] },
      { id: 2, a: [3], b: ['z'] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, u.x, v.y FROM t JOIN UNNEST(t.a) AS u(x) ON TRUE JOIN UNNEST(t.b) AS v(y) ON TRUE',
    }))
    expect(result).toEqual([
      { id: 1, x: 1, y: 'x' },
      { id: 1, x: 1, y: 'y' },
      { id: 1, x: 2, y: 'x' },
      { id: 1, x: 2, y: 'y' },
      { id: 2, x: 3, y: 'z' },
    ])
  })

  it('should aggregate over lateral output with GROUP BY', async () => {
    const t = [
      { id: 1, arr: [1, 2, 3] },
      { id: 2, arr: [10, 20] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, SUM(u.x) AS total FROM t JOIN UNNEST(t.arr) AS u(x) ON TRUE GROUP BY t.id',
    }))
    expect(result).toEqual([
      { id: 1, total: 6 },
      { id: 2, total: 30 },
    ])
  })

  it('should reject RIGHT JOIN with a table function', () => {
    expect(() => executeSql({
      tables: { t: [{ arr: [1] }] },
      query: 'SELECT * FROM t RIGHT JOIN UNNEST(t.arr) AS u(x) ON TRUE',
    })).toThrow(/RIGHT JOIN not supported with table functions/)
  })

  it('should reject FULL JOIN with a table function', () => {
    expect(() => executeSql({
      tables: { t: [{ arr: [1] }] },
      query: 'SELECT * FROM t FULL JOIN UNNEST(t.arr) AS u(x) ON TRUE',
    })).toThrow(/FULL JOIN not supported with table functions/)
  })

  it('should reject LATERAL on a plain table join', () => {
    expect(() => executeSql({
      tables: { t: [{ id: 1 }], labels: [{ id: 1, name: 'one' }] },
      query: 'SELECT * FROM t JOIN LATERAL labels ON labels.id = t.id',
    })).toThrow(/LATERAL is only supported with table functions/)
  })

  it('should still reject FROM UNNEST with a column ref (no scope)', () => {
    expect(() => executeSql({
      tables: { t: [{ arr: [1, 2] }] },
      query: 'SELECT * FROM UNNEST(arr) AS u',
    })).toThrow(/cannot reference column "arr"/)
  })

  it('should stop yielding when the signal aborts mid-stream', async () => {
    const t = [
      { id: 1, arr: [1, 2, 3] },
      { id: 2, arr: [4, 5, 6] },
    ]
    const controller = new AbortController()
    const iter = executeSql({
      tables: { t },
      query: 'SELECT u.x FROM t JOIN UNNEST(t.arr) AS u(x) ON TRUE',
      signal: controller.signal,
    }).rows()
    const first = await iter.next()
    expect(await first.value?.cells.x()).toBe(1)
    controller.abort()
    const second = await iter.next()
    expect(second.done).toBe(true)
  })
})

describe('array literals', () => {
  const singleRow = [{ x: 1 }]

  it('should parse an empty array literal', async () => {
    const result = await collect(executeSql({
      tables: { singleRow },
      query: 'SELECT CARDINALITY([]) AS len FROM singleRow',
    }))
    expect(result).toEqual([{ len: 0 }])
  })

  it('should parse a numeric array literal', async () => {
    const result = await collect(executeSql({
      tables: { singleRow },
      query: 'SELECT ARRAY_LENGTH([1, 2, 3]) AS len FROM singleRow',
    }))
    expect(result).toEqual([{ len: 3 }])
  })

  it('should parse a string array literal', async () => {
    const result = await collect(executeSql({
      tables: { singleRow },
      query: 'SELECT ARRAY_POSITION([\'a\', \'b\', \'c\'], \'b\') AS pos FROM singleRow',
    }))
    expect(result).toEqual([{ pos: 2 }])
  })

  it('should parse a negative-number array literal', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT x FROM UNNEST([-1, -2, 3]) AS t(x)',
    }))
    expect(result).toEqual([{ x: -1 }, { x: -2 }, { x: 3 }])
  })

  it('should parse nested array literals', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT x FROM UNNEST([[1, 2], [3, 4]]) AS t(x)',
    }))
    expect(result).toEqual([{ x: [1, 2] }, { x: [3, 4] }])
  })

  it('should reject non-literal elements', () => {
    expect(() => executeSql({
      tables: { singleRow },
      query: 'SELECT [x + 1] AS arr FROM singleRow',
    })).toThrow('Array literal elements must be constant literals')
  })
})
