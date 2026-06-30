import { describe, expect, it } from 'vitest'
import { QueryBudgetExceededError, collect, executeSql } from '../../src/index.js'
import { memorySource } from '../../src/backend/dataSource.js'

/**
 * @import { AsyncDataSource, SqlPrimitive } from '../../src/types.js'
 */

/** @type {{ id: number, bucket: number }[]} */
const rows = []
for (let i = 0; i < 10; i++) rows.push({ id: i, bucket: i % 3 })

/**
 * Runs a query to completion and returns the error it throws, asserting that it
 * is a QueryBudgetExceededError.
 *
 * @param {import('../../src/types.js').ExecuteSqlOptions} options
 * @returns {Promise<QueryBudgetExceededError>}
 */
async function captureBudgetError(options) {
  let caught
  try {
    await collect(executeSql(options))
  } catch (err) {
    caught = err
  }
  expect(caught).toBeInstanceOf(QueryBudgetExceededError)
  if (caught instanceof QueryBudgetExceededError) return caught
  throw new Error('expected QueryBudgetExceededError, but the query succeeded')
}

describe('execution budget', () => {
  describe('refuses over the buffered-row ceiling', () => {
    it('ORDER BY beyond the row ceiling throws QueryBudgetExceededError', async () => {
      const err = await captureBudgetError({
        tables: { s: memorySource({ data: rows }) },
        query: 'SELECT * FROM s ORDER BY id',
        budget: { maxBufferedRows: 5 },
      })
      expect(err.operator).toBe('ORDER BY')
      expect(err.limitKind).toBe('rows')
      expect(err.limit).toBe(5)
      expect(err.observed).toBe(6)
    })

    it('scalar-aggregate slow path beyond the row ceiling throws QueryBudgetExceededError', async () => {
      // SUM of a derived expression bypasses the scanColumn fast path, so the
      // whole input is collected into one group and the budget bites.
      const err = await captureBudgetError({
        tables: { s: memorySource({ data: rows }) },
        query: 'SELECT SUM(id + 1) AS total FROM s',
        budget: { maxBufferedRows: 5 },
      })
      expect(err.operator).toBe('aggregate')
      expect(err.limitKind).toBe('rows')
      expect(err.limit).toBe(5)
    })

    it('GROUP BY beyond the row ceiling throws QueryBudgetExceededError', async () => {
      const err = await captureBudgetError({
        tables: { s: memorySource({ data: rows }) },
        query: 'SELECT bucket, COUNT(*) AS n FROM s GROUP BY bucket',
        budget: { maxBufferedRows: 5 },
      })
      expect(err.operator).toBe('GROUP BY')
      expect(err.limit).toBe(5)
    })

    it('DISTINCT beyond the row ceiling throws QueryBudgetExceededError', async () => {
      const err = await captureBudgetError({
        tables: { s: memorySource({ data: rows }) },
        query: 'SELECT DISTINCT id FROM s',
        budget: { maxBufferedRows: 5 },
      })
      expect(err.operator).toBe('DISTINCT')
      expect(err.limit).toBe(5)
    })
  })

  describe('refuses over the buffered-byte ceiling', () => {
    it('ORDER BY beyond the byte ceiling throws with limitKind bytes', async () => {
      const err = await captureBudgetError({
        tables: { s: memorySource({ data: rows }) },
        query: 'SELECT * FROM s ORDER BY id',
        // Each buffered row estimates to well over 1 byte, so the byte ceiling
        // trips before all 10 rows are buffered.
        budget: { maxBufferedBytes: 1 },
      })
      expect(err.operator).toBe('ORDER BY')
      expect(err.limitKind).toBe('bytes')
      expect(err.limit).toBe(1)
    })
  })

  describe('under the ceiling succeeds normally', () => {
    it('ORDER BY under the row ceiling returns all sorted rows', async () => {
      const result = await collect(executeSql({
        tables: { s: memorySource({ data: rows }) },
        query: 'SELECT id FROM s ORDER BY id DESC',
        budget: { maxBufferedRows: 100 },
      }))
      expect(result).toHaveLength(10)
      expect(result[0]).toEqual({ id: 9 })
      expect(result[9]).toEqual({ id: 0 })
    })

    it('scalar aggregate under the row ceiling returns the aggregate', async () => {
      const result = await collect(executeSql({
        tables: { s: memorySource({ data: rows }) },
        query: 'SELECT SUM(id + 1) AS total FROM s',
        budget: { maxBufferedRows: 100 },
      }))
      expect(result).toEqual([{ total: 55 }])
    })

    it('a query with no budget is unbounded (backward compatible)', async () => {
      const result = await collect(executeSql({
        tables: { s: memorySource({ data: rows }) },
        query: 'SELECT * FROM s ORDER BY id',
      }))
      expect(result).toHaveLength(10)
    })
  })

  describe('streaming operators emit before refusing (not all-or-nothing)', () => {
    it('DISTINCT emits rows up to the ceiling, then throws mid-stream', async () => {
      // DISTINCT is a streaming operator: it bounds only its dedup-set memory and
      // yields each new distinct row as it sees it. So unlike the buffering
      // operators (which throw before emitting any row), DISTINCT has ALREADY
      // emitted rows 1..ceiling by the time a later key trips the budget. The
      // contract is that a thrown error invalidates the whole result.
      const results = executeSql({
        tables: { s: memorySource({ data: rows }) },
        query: 'SELECT DISTINCT id FROM s',
        budget: { maxBufferedRows: 5 },
      })

      /** @type {Record<string, SqlPrimitive>[]} */
      const emitted = []
      let caught
      try {
        for await (const row of results.rows()) {
          /** @type {Record<string, SqlPrimitive>} */
          const obj = {}
          for (const col of row.columns) obj[col] = await row.cells[col]()
          emitted.push(obj)
        }
      } catch (err) {
        caught = err
      }

      expect(caught).toBeInstanceOf(QueryBudgetExceededError)
      // The first five distinct rows were emitted before the sixth key tripped
      // the ceiling; a consumer must discard them, not treat them as truncated.
      expect(emitted).toEqual([{ id: 0 }, { id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }])
    })
  })

  describe('COUNT(DISTINCT) fast path is bounded; plain COUNT stays immune', () => {
    /**
     * A scanColumn-only source streaming `count` ascending distinct ids in two
     * chunks. Its buffering `scan` path throws, proving the column-scan fast
     * path is taken rather than the buffered slow path.
     *
     * @param {number} count
     * @returns {AsyncDataSource}
     */
    function scanColumnSource(count) {
      return {
        numRows: count,
        columns: ['id'],
        scan() {
          throw new Error('scan (buffering path) should not be called')
        },
        async *scanColumn() {
          const half = Math.floor(count / 2)
          /** @type {number[]} */
          const first = []
          for (let i = 0; i < half; i++) first.push(i)
          yield first
          /** @type {number[]} */
          const second = []
          for (let i = half; i < count; i++) second.push(i)
          yield second
        },
      }
    }

    it('COUNT(DISTINCT id) past the ceiling throws QueryBudgetExceededError', async () => {
      // COUNT(DISTINCT) retains O(distinct) dedup keys even on the scanColumn
      // fast path, so a budget must bound it or a high-cardinality column OOMs.
      const err = await captureBudgetError({
        tables: { data: scanColumnSource(100) },
        query: 'SELECT COUNT(DISTINCT id) AS n FROM data',
        budget: { maxBufferedRows: 10 },
      })
      expect(err.operator).toBe('COUNT(DISTINCT)')
      expect(err.limitKind).toBe('rows')
      expect(err.limit).toBe(10)
    })

    it('COUNT(id) over the same source with the same low ceiling stays immune (O(1))', async () => {
      // Plain COUNT holds O(1) state, so the same ceiling that refuses
      // COUNT(DISTINCT) must not bite it.
      const result = await collect(executeSql({
        tables: { data: scanColumnSource(100) },
        query: 'SELECT COUNT(id) AS n FROM data',
        budget: { maxBufferedRows: 10, maxBufferedBytes: 10 },
      }))
      expect(result).toEqual([{ n: 100 }])
    })
  })

  describe('streaming paths bypass the budget', () => {
    it('a streaming column-scan aggregate is NOT affected by a low ceiling', async () => {
      // This source streams a single column via scanColumn and throws if its
      // buffering `scan` path is ever taken, proving the aggregate fast path
      // holds O(1) state and never consults the budget.
      let scanColumnCalls = 0
      /** @type {AsyncDataSource} */
      const streamingSource = {
        numRows: 100,
        columns: ['id'],
        scan() {
          throw new Error('scan (buffering path) should not be called')
        },
        async *scanColumn() {
          scanColumnCalls++
          const first = []
          for (let i = 0; i < 50; i++) first.push(i)
          yield first
          const second = []
          for (let i = 50; i < 100; i++) second.push(i)
          yield second
        },
      }

      const result = await collect(executeSql({
        tables: { data: streamingSource },
        query: 'SELECT COUNT(id) AS n FROM data',
        // A ceiling of 1 would refuse any buffering operator immediately.
        budget: { maxBufferedRows: 1, maxBufferedBytes: 1 },
      }))

      expect(result).toEqual([{ n: 100 }])
      expect(scanColumnCalls).toBe(1)
    })
  })
})
