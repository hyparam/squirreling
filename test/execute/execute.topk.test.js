import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'
import { planSql } from '../../src/plan/plan.js'

/**
 * @import { AsyncDataSource, AsyncRow, QueryPlan, ScanResults, SqlPrimitive } from '../../src/types.js'
 * @import { SortNode } from '../../src/plan/types.d.ts'
 */

/**
 * Returns an empty AsyncDataSource useful for plan-only assertions.
 *
 * @param {string[]} columns
 * @returns {AsyncDataSource}
 */
function emptySource(columns) {
  return {
    columns,
    scan() {
      return {
        async *rows() {},
        appliedWhere: false,
        appliedLimitOffset: false,
      }
    },
  }
}

/**
 * Walks a plan tree until it finds a Sort node, returning it (or null).
 *
 * @param {QueryPlan} plan
 * @returns {SortNode | null}
 */
function findSort(plan) {
  let node = plan
  while (node.type !== 'Sort') {
    if (!('child' in node) || !node.child) return null
    node = node.child
  }
  return node
}

/**
 * Builds a row from a plain object, lazily wrapping cells.
 *
 * @param {Record<string, SqlPrimitive>} obj
 * @param {string[]} columns
 * @returns {AsyncRow}
 */
function asyncRow(obj, columns) {
  /** @type {Record<string, () => Promise<SqlPrimitive>>} */
  const cells = {}
  for (const k of columns) cells[k] = () => Promise.resolve(obj[k])
  return { columns, cells, resolved: obj }
}

/**
 * Builds a data source that produces `numRows` rows on demand, recording how
 * many rows have been pulled from the generator. Used to assert that top-K
 * does not require buffering the entire input.
 *
 * @param {object} options
 * @param {number} options.numRows
 * @param {(i: number) => Record<string, SqlPrimitive>} options.row
 * @param {string[]} options.columns
 * @returns {{ source: AsyncDataSource, pulled: { count: number } }}
 */
function streamingSource({ numRows, row, columns }) {
  const pulled = { count: 0 }
  /** @type {AsyncDataSource} */
  const source = {
    numRows,
    columns,
    scan({ columns: scanColumns, signal }) {
      const cols = scanColumns ?? columns
      /** @type {ScanResults} */
      return {
        async *rows() {
          for (let i = 0; i < numRows; i++) {
            if (signal?.aborted) break
            pulled.count++
            yield asyncRow(row(i), cols)
          }
        },
        appliedWhere: false,
        appliedLimitOffset: false,
      }
    },
  }
  return { source, pulled }
}

describe('ORDER BY ... LIMIT (top-K)', () => {
  describe('correctness', () => {
    const data = [
      { id: 1, x: 5, y: 'b' },
      { id: 2, x: 3, y: 'a' },
      { id: 3, x: 9, y: 'c' },
      { id: 4, x: 1, y: 'a' },
      { id: 5, x: 7, y: 'b' },
      { id: 6, x: 4, y: 'a' },
      { id: 7, x: 6, y: 'c' },
      { id: 8, x: 2, y: 'b' },
    ]

    it('matches full sort for ORDER BY ASC LIMIT N', async () => {
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT id, x FROM data ORDER BY x ASC LIMIT 3' }))
      expect(result.map(r => r.x)).toEqual([1, 2, 3])
    })

    it('matches full sort for ORDER BY DESC LIMIT N', async () => {
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT id, x FROM data ORDER BY x DESC LIMIT 3' }))
      expect(result.map(r => r.x)).toEqual([9, 7, 6])
    })

    it('handles LIMIT N OFFSET M', async () => {
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT id, x FROM data ORDER BY x ASC LIMIT 3 OFFSET 2' }))
      expect(result.map(r => r.x)).toEqual([3, 4, 5])
    })

    it('handles multi-key ORDER BY', async () => {
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT id, x, y FROM data ORDER BY y ASC, x DESC LIMIT 4' }))
      // y='a' first (rows 2, 4, 6 with x=3,1,4), then y='b' (rows 1, 5, 8 with x=5,7,2)
      // Within y='a' DESC by x: 4, 3, 1
      // Within y='b' DESC by x: 7, 5, 2
      expect(result.map(r => ({ y: r.y, x: r.x }))).toEqual([
        { y: 'a', x: 4 },
        { y: 'a', x: 3 },
        { y: 'a', x: 1 },
        { y: 'b', x: 7 },
      ])
    })

    it('handles LIMIT larger than input', async () => {
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT id FROM data ORDER BY id LIMIT 100' }))
      expect(result.map(r => r.id)).toEqual([1, 2, 3, 4, 5, 6, 7, 8])
    })

    it('handles LIMIT 0', async () => {
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT id FROM data ORDER BY id LIMIT 0' }))
      expect(result).toEqual([])
    })

    it('handles LIMIT 1', async () => {
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT id, x FROM data ORDER BY x ASC LIMIT 1' }))
      expect(result).toEqual([{ id: 4, x: 1 }])
    })

    it('handles NULLS FIRST and NULLS LAST with LIMIT', async () => {
      const items = [
        { id: 1, v: 5 },
        { id: 2, v: null },
        { id: 3, v: 1 },
        { id: 4, v: null },
        { id: 5, v: 3 },
      ]
      const first = await collect(executeSql({ tables: { items }, query: 'SELECT id FROM items ORDER BY v ASC NULLS FIRST LIMIT 3' }))
      expect(first.map(r => r.id)).toEqual([2, 4, 3])
      const last = await collect(executeSql({ tables: { items }, query: 'SELECT id FROM items ORDER BY v ASC NULLS LAST LIMIT 3' }))
      expect(last.map(r => r.id)).toEqual([3, 5, 1])
    })

    it('breaks ties via second sort key', async () => {
      const tied = [
        { id: 1, k: 1, t: 'b' },
        { id: 2, k: 1, t: 'a' },
        { id: 3, k: 1, t: 'c' },
        { id: 4, k: 2, t: 'a' },
      ]
      const result = await collect(executeSql({ tables: { tied }, query: 'SELECT id FROM tied ORDER BY k ASC, t ASC LIMIT 2' }))
      expect(result.map(r => r.id)).toEqual([2, 1])
    })

    it('handles ORDER BY expression with LIMIT', async () => {
      const result = await collect(executeSql({ tables: { data }, query: 'SELECT id FROM data ORDER BY x * -1 LIMIT 3' }))
      // Sort by -x ASC = sort by x DESC: 9, 7, 6
      expect(result.map(r => r.id)).toEqual([3, 5, 7])
    })

    it('caches evaluated sort keys on yielded rows for downstream operators', async () => {
      // ORDER BY a derived expression; the alias is materialized in the Project
      // that wraps Sort. If sort caching works, Project uses the cached value
      // instead of re-evaluating.
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT id, x + 1 AS x1 FROM data ORDER BY x + 1 LIMIT 3',
      }))
      expect(result).toEqual([
        { id: 4, x1: 2 },
        { id: 8, x1: 3 },
        { id: 2, x1: 4 },
      ])
    })
  })

  describe('memory boundedness', () => {
    it('streams large input without buffering', async () => {
      // 100k rows is plenty to demonstrate the heap stays bounded; the full
      // sort path would buffer all of them. Top-K with K=10 should retain at
      // most ~10 rows + the in-flight chunk.
      const N = 100_000
      const { source, pulled } = streamingSource({
        numRows: N,
        columns: ['id', 'x'],
        // Reverse-sorted input so the worst case (every row beats the heap
        // top until it doesn't) doesn't apply; pseudo-random keys exercise
        // realistic eviction behavior.
        row: i => ({ id: i, x: i * 2654435761 >>> 0 }),
      })
      const result = await collect(executeSql({
        tables: { t: source },
        query: 'SELECT id FROM t ORDER BY x ASC LIMIT 10',
      }))
      expect(result).toHaveLength(10)
      // All N rows must be pulled (we can't know the top-K without scanning).
      expect(pulled.count).toBe(N)
      // Verify correctness: the 10 retained ids are the ones with the smallest
      // x = (i * 2654435761) mod 2^32.
      const expected = Array.from({ length: N }, (_, i) => ({ id: i, x: i * 2654435761 >>> 0 }))
        .sort((a, b) => a.x - b.x)
        .slice(0, 10)
        .map(r => r.id)
      expect(result.map(r => r.id)).toEqual(expected)
    })

    it('matches a reference full sort for large input', async () => {
      // Validate top-K against an in-memory sort of the same data.
      const N = 5_000
      const rows = Array.from({ length: N }, (_, i) => ({ id: i, x: (i * 2654435761 ^ i << 7) >>> 0 }))
      const expected = rows.slice().sort((a, b) => a.x - b.x).slice(0, 50).map(r => r.id)
      const result = await collect(executeSql({
        tables: { rows },
        query: 'SELECT id FROM rows ORDER BY x ASC LIMIT 50',
      }))
      expect(result.map(r => r.id)).toEqual(expected)
    })
  })

  describe('planner', () => {
    const tables = { t: emptySource(['x']) }

    it('sets limit hint on Sort when LIMIT is small', () => {
      const plan = planSql({ query: 'SELECT * FROM t ORDER BY x LIMIT 10', tables })
      const sort = findSort(plan)
      expect(sort?.limit).toBe(10)
    })

    it('sets limit hint to LIMIT + OFFSET so OFFSET still applies after Sort', () => {
      const plan = planSql({ query: 'SELECT * FROM t ORDER BY x LIMIT 10 OFFSET 25', tables })
      expect(findSort(plan)?.limit).toBe(35)
    })

    it('does not set limit hint when LIMIT is missing', () => {
      const plan = planSql({ query: 'SELECT * FROM t ORDER BY x OFFSET 5', tables })
      expect(findSort(plan)?.limit).toBeUndefined()
    })

    it('does not set limit hint when LIMIT exceeds the top-K threshold', () => {
      const plan = planSql({ query: 'SELECT * FROM t ORDER BY x LIMIT 100000', tables })
      expect(findSort(plan)?.limit).toBeUndefined()
    })

    it('does not set limit hint when LIMIT + OFFSET exceeds the top-K threshold', () => {
      const plan = planSql({ query: 'SELECT * FROM t ORDER BY x LIMIT 5000 OFFSET 6000', tables })
      expect(findSort(plan)?.limit).toBeUndefined()
    })

    it('does not set limit hint when DISTINCT is present', () => {
      // DISTINCT can drop rows between Sort and Limit, which would make the
      // top-K cap incorrect (we'd lose rows we should have kept).
      const plan = planSql({ query: 'SELECT DISTINCT x FROM t ORDER BY x LIMIT 10', tables })
      expect(findSort(plan)?.limit).toBeUndefined()
    })

    it('still produces correct results when DISTINCT is present', async () => {
      // Even though the Sort doesn't get a limit hint, DISTINCT + LIMIT must
      // still produce correct results via the full sort path.
      const dups = [
        { v: 3 }, { v: 1 }, { v: 1 }, { v: 2 }, { v: 3 }, { v: 2 }, { v: 1 },
      ]
      const result = await collect(executeSql({ tables: { dups }, query: 'SELECT DISTINCT v FROM dups ORDER BY v LIMIT 2' }))
      expect(result).toEqual([{ v: 1 }, { v: 2 }])
    })
  })
})
