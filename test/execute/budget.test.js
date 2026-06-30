import { describe, expect, it } from 'vitest'
import { QueryBudgetExceededError, collect, executeSql } from '../../src/index.js'
import { memorySource } from '../../src/backend/dataSource.js'

/**
 * @import { AsyncDataSource } from '../../src/types.js'
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
