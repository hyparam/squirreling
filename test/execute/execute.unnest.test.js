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

  it('should evaluate EXISTS with a correlated UNNEST against the outer row', async () => {
    const traces = [
      { id: 1, tool_calls: [{ name: 'web_search' }] },
      { id: 2, tool_calls: [{ name: 'calculator' }] },
      { id: 3, tool_calls: [] },
    ]
    const result = await collect(executeSql({
      tables: { traces },
      query: 'SELECT id FROM traces WHERE EXISTS (SELECT 1 FROM UNNEST(tool_calls) AS tc WHERE tc.name = \'web_search\')',
    }))
    expect(result).toEqual([{ id: 1 }])
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

  it('should expand via CROSS JOIN UNNEST (no ON clause)', async () => {
    const t = [
      { id: 1, arr: [10, 20] },
      { id: 2, arr: [30] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, u.x FROM t CROSS JOIN UNNEST(t.arr) AS u(x)',
    }))
    expect(result).toEqual([
      { id: 1, x: 10 },
      { id: 1, x: 20 },
      { id: 2, x: 30 },
    ])
  })

  it('should expand via comma-join UNNEST (implicit CROSS JOIN LATERAL)', async () => {
    const t = [
      { id: 1, arr: [10, 20] },
      { id: 2, arr: [30] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, u.x FROM t, UNNEST(t.arr) AS u(x)',
    }))
    expect(result).toEqual([
      { id: 1, x: 10 },
      { id: 1, x: 20 },
      { id: 2, x: 30 },
    ])
  })

  it('should resolve an unqualified ref in CROSS JOIN UNNEST', async () => {
    const t = [
      { id: 1, arr: [1, 2] },
      { id: 2, arr: [3] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, u.x FROM t CROSS JOIN UNNEST(arr) AS u(x)',
    }))
    expect(result).toEqual([
      { id: 1, x: 1 },
      { id: 1, x: 2 },
      { id: 2, x: 3 },
    ])
  })

  it('should reject comma-join with a regular table', () => {
    expect(() => executeSql({
      tables: { t: [{ id: 1 }], labels: [{ id: 1, name: 'one' }] },
      query: 'SELECT * FROM t, labels',
    })).toThrow('Comma-separated FROM is only supported with table functions like UNNEST; use explicit JOIN ... ON ... for regular tables')
  })

  it('should reject CROSS JOIN with a regular table', () => {
    expect(() => executeSql({
      tables: { t: [{ id: 1 }], labels: [{ id: 1, name: 'one' }] },
      query: 'SELECT * FROM t CROSS JOIN labels',
    })).toThrow('CROSS JOIN is currently supported only with table functions like UNNEST')
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

  it('should support later lateral UNNEST arguments that reference earlier lateral outputs', async () => {
    const t = [
      { id: 1, arr: [[10, 20], [30]] },
      { id: 2, arr: [[40]] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, v.y FROM t JOIN UNNEST(t.arr) AS u(x) ON TRUE JOIN UNNEST(x) AS v(y) ON TRUE',
    }))
    expect(result).toEqual([
      { id: 1, y: 10 },
      { id: 1, y: 20 },
      { id: 1, y: 30 },
      { id: 2, y: 40 },
    ])
  })

  it('should execute lateral UNNEST argument subqueries that reference enclosing CTEs', async () => {
    const one = [{ id: 1 }]
    const arrays = [{ arr: [10, 20] }]

    const result = await collect(executeSql({
      tables: { one, arrays },
      query: `
        WITH c AS (SELECT arr FROM arrays)
        SELECT u.x
        FROM one
        JOIN UNNEST((SELECT arr FROM c LIMIT 1)) AS u(x) ON TRUE
      `,
    }))

    expect(result).toEqual([{ x: 10 }, { x: 20 }])
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

  it('should reject selecting the UNNEST table alias as a bare column', async () => {
    const t = [
      { id: 1, arr: [10, 20] },
      { id: 2, arr: [30] },
    ]
    await expect(collect(executeSql({
      tables: { t },
      query: 'SELECT tc_item FROM t CROSS JOIN UNNEST(t.arr) AS tc_item',
    }))).rejects.toThrow('Column "tc_item" not found. Available columns: t.id, t.arr, tc_item.unnest (row 1)')
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

describe('UNNEST array-of-struct', () => {
  const traces = [
    { id: 1, tools: [{ name: 'web_search', args: '{"q":"sql"}' }, { name: 'calculator', args: '1+1' }] },
    { id: 2, tools: [{ name: 'web_search', args: '{"q":"json"}' }] },
    { id: 3, tools: [] },
  ]

  it('should expose struct fields as columns on the unnest alias via dot access', async () => {
    const result = await collect(executeSql({
      tables: { traces },
      query: 'SELECT t.id, tc.name FROM traces t JOIN UNNEST(t.tools) AS tc ON TRUE',
    }))
    expect(result).toEqual([
      { id: 1, name: 'web_search' },
      { id: 1, name: 'calculator' },
      { id: 2, name: 'web_search' },
    ])
  })

  it('should expose struct fields via comma-join UNNEST + dot access', async () => {
    const result = await collect(executeSql({
      tables: { traces },
      query: 'SELECT t.id, tc.name FROM traces t, UNNEST(t.tools) AS tc',
    }))
    expect(result).toEqual([
      { id: 1, name: 'web_search' },
      { id: 1, name: 'calculator' },
      { id: 2, name: 'web_search' },
    ])
  })

  it('should support filtering by an unnested struct field', async () => {
    const result = await collect(executeSql({
      tables: { traces },
      query: 'SELECT t.id FROM traces t JOIN UNNEST(t.tools) AS tc ON TRUE WHERE tc.name = \'web_search\'',
    }))
    expect(result).toEqual([{ id: 1 }, { id: 2 }])
  })

  it('should support COUNT(*) with a filter on an unnested struct field', async () => {
    const result = await collect(executeSql({
      tables: { traces },
      query: 'SELECT COUNT(*) AS cnt FROM traces t JOIN UNNEST(t.tools) AS tc ON TRUE WHERE tc.name = \'web_search\'',
    }))
    expect(result).toEqual([{ cnt: 2 }])
  })

  it('should support subscript notation on the unnest alias', async () => {
    const result = await collect(executeSql({
      tables: { traces },
      query: 'SELECT t.id, tc[\'name\'] AS tool FROM traces t JOIN UNNEST(t.tools) AS tc ON TRUE',
    }))
    expect(result).toEqual([
      { id: 1, tool: 'web_search' },
      { id: 1, tool: 'calculator' },
      { id: 2, tool: 'web_search' },
    ])
  })

  it('should expose struct fields when the alias has an explicit column name', async () => {
    // `AS tc(item)` aliases the unnested element as `item`; struct fields
    // should then be reachable via `item.name` (dot access on the struct).
    const result = await collect(executeSql({
      tables: { traces },
      query: 'SELECT t.id, item.name FROM traces t JOIN UNNEST(t.tools) AS tc(item) ON TRUE',
    }))
    expect(result).toEqual([
      { id: 1, name: 'web_search' },
      { id: 1, name: 'calculator' },
      { id: 2, name: 'web_search' },
    ])
  })
})

describe('EXPLODE', () => {
  it('should expand a numeric array into rows', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT * FROM EXPLODE([1, 2, 3]) AS t',
    }))
    expect(result).toEqual([{ explode: 1 }, { explode: 2 }, { explode: 3 }])
  })

  it('should support a column alias like AS t(x)', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT x FROM EXPLODE([10, 20, 30]) AS t(x)',
    }))
    expect(result).toEqual([{ x: 10 }, { x: 20 }, { x: 30 }])
  })

  it('should produce zero rows for a NULL argument', async () => {
    const result = await collect(executeSql({
      tables: {},
      query: 'SELECT * FROM EXPLODE(NULL) AS t',
    }))
    expect(result).toEqual([])
  })

  it('should work in a lateral JOIN with a column ref', async () => {
    const t = [
      { id: 1, arr: [10, 20] },
      { id: 2, arr: [30] },
    ]
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, u.x FROM t JOIN EXPLODE(t.arr) AS u(x) ON TRUE',
    }))
    expect(result).toEqual([
      { id: 1, x: 10 },
      { id: 1, x: 20 },
      { id: 2, x: 30 },
    ])
  })

  it('should throw for wrong argument count', () => {
    expect(() => executeSql({
      tables: {},
      query: 'SELECT * FROM EXPLODE() AS t',
    })).toThrow('EXPLODE(array) function requires 1 argument, got 0')
  })

  it('should throw when used as a scalar expression', () => {
    expect(() => executeSql({
      tables: {},
      query: 'SELECT EXPLODE([1, 2, 3])',
    })).toThrow('EXPLODE is a table function and can only be used in FROM clauses at position 7')
  })
})

describe('LATERAL VIEW', () => {
  const t = [
    { id: 1, tags: ['a', 'b'] },
    { id: 2, tags: ['c'] },
    { id: 3, tags: [] },
    { id: 4, tags: null },
  ]

  it('should expand an array column into rows', async () => {
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT id, tag FROM t LATERAL VIEW EXPLODE(tags) e AS tag',
    }))
    expect(result).toEqual([
      { id: 1, tag: 'a' },
      { id: 1, tag: 'b' },
      { id: 2, tag: 'c' },
    ])
  })

  it('should resolve a qualified column ref in the array argument', async () => {
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT t.id, tag FROM t LATERAL VIEW EXPLODE(t.tags) e AS tag',
    }))
    expect(result).toEqual([
      { id: 1, tag: 'a' },
      { id: 1, tag: 'b' },
      { id: 2, tag: 'c' },
    ])
  })

  it('should drop rows with empty or NULL arrays (INNER semantics)', async () => {
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT id FROM t LATERAL VIEW EXPLODE(tags) e AS tag',
    }))
    expect(result).toEqual([
      { id: 1 },
      { id: 1 },
      { id: 2 },
    ])
  })

  it('should emit a NULL row for empty/NULL arrays with OUTER', async () => {
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT id, tag FROM t LATERAL VIEW OUTER EXPLODE(tags) e AS tag',
    }))
    expect(result).toEqual([
      { id: 1, tag: 'a' },
      { id: 1, tag: 'b' },
      { id: 2, tag: 'c' },
      { id: 3, tag: null },
      { id: 4, tag: null },
    ])
  })

  it('should support a WHERE clause on the exploded column', async () => {
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT id, tag FROM t LATERAL VIEW EXPLODE(tags) e AS tag WHERE tag > \'a\'',
    }))
    expect(result).toEqual([
      { id: 1, tag: 'b' },
      { id: 2, tag: 'c' },
    ])
  })

  it('should aggregate over the exploded column', async () => {
    const result = await collect(executeSql({
      tables: { t },
      query: 'SELECT id, COUNT(*) AS n FROM t LATERAL VIEW EXPLODE(tags) e AS tag GROUP BY id',
    }))
    expect(result).toEqual([
      { id: 1, n: 2 },
      { id: 2, n: 1 },
    ])
  })

  it('should chain two LATERAL VIEWs', async () => {
    const t2 = [
      { id: 1, a: [1, 2], b: ['x', 'y'] },
      { id: 2, a: [3], b: ['z'] },
    ]
    const result = await collect(executeSql({
      tables: { t: t2 },
      query: 'SELECT id, x, y FROM t LATERAL VIEW EXPLODE(a) ea AS x LATERAL VIEW EXPLODE(b) eb AS y',
    }))
    expect(result).toEqual([
      { id: 1, x: 1, y: 'x' },
      { id: 1, x: 1, y: 'y' },
      { id: 1, x: 2, y: 'x' },
      { id: 1, x: 2, y: 'y' },
      { id: 2, x: 3, y: 'z' },
    ])
  })

  it('should require VIEW after LATERAL', () => {
    expect(() => executeSql({
      tables: { t },
      query: 'SELECT id FROM t LATERAL EXPLODE(tags) e AS tag',
    })).toThrow('Expected VIEW after "LATERAL" but found "EXPLODE" at position 25')
  })

  it('should require a table alias', () => {
    expect(() => executeSql({
      tables: { t },
      query: 'SELECT id FROM t LATERAL VIEW EXPLODE(tags) AS tag',
    })).toThrow('LATERAL VIEW requires a table alias before AS')
  })

  it('should require AS columnAlias', () => {
    expect(() => executeSql({
      tables: { t },
      query: 'SELECT id FROM t LATERAL VIEW EXPLODE(tags) e',
    })).toThrow('Expected AS after "e" but found end of query at position 45')
  })

  it('should reject LATERAL VIEW with a non-table function', () => {
    expect(() => executeSql({
      tables: { t },
      query: 'SELECT id FROM t LATERAL VIEW UPPER(tags) e AS tag',
    })).toThrow('LATERAL VIEW requires a table function like EXPLODE')
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
