import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { SqlBudgetError, collect, createBudget, executeSql } from '../../src/index.js'

const users = [
  { id: 1, name: 'Alice', team: 'red' },
  { id: 2, name: 'Bob', team: 'red' },
  { id: 3, name: 'Charlie', team: 'blue' },
  { id: 4, name: 'Diana', team: 'blue' },
  { id: 5, name: 'Eve', team: 'green' },
]

const orders = [
  { id: 1, user_id: 1, amount: 100 },
  { id: 2, user_id: 1, amount: 200 },
  { id: 3, user_id: 2, amount: 150 },
  { id: 4, user_id: 3, amount: 300 },
  { id: 5, user_id: 4, amount: 250 },
]

/**
 * Wraps a memorySource and adds a scanColumn implementation so the executor
 * can take its column-scan fast paths. Returns the source plus call counters.
 *
 * @param {Record<string, import('../../src/types.js').SqlPrimitive>[]} data
 * @returns {{
 *   source: import('../../src/types.js').AsyncDataSource,
 *   scanCalls: () => number,
 *   scanColumnCalls: () => number,
 * }}
 */
function columnScannableSource(data) {
  const inner = memorySource({ data })
  let scanColumnCalls = 0
  let scanCalls = 0
  return {
    source: {
      ...inner,
      /**
       * @param {import('../../src/types.js').ScanOptions} options
       * @returns {import('../../src/types.js').ScanResults}
       */
      scan(options) {
        scanCalls++
        return inner.scan(options)
      },
      /**
       * @param {import('../../src/types.js').ScanColumnOptions} options
       * @returns {AsyncIterable<ArrayLike<import('../../src/types.js').SqlPrimitive>>}
       */
      scanColumn(options) {
        scanColumnCalls++
        return (async function* () {
          const start = options.offset ?? 0
          const end = options.limit !== undefined ? start + options.limit : data.length
          const slice = data.slice(start, Math.min(end, data.length))
          yield slice.map(row => row[options.column])
        })()
      },
    },
    scanCalls: () => scanCalls,
    scanColumnCalls: () => scanColumnCalls,
  }
}

/**
 * Runs a query and returns the SqlBudgetError it throws (or rethrows the
 * original error). Materialization happens inside the async generator so
 * callers must iterate to surface the error.
 *
 * @param {Parameters<typeof executeSql>[0]} options
 * @returns {Promise<SqlBudgetError>} the captured budget error
 */
async function expectBudgetError(options) {
  try {
    await collect(executeSql(options))
  } catch (err) {
    if (err instanceof SqlBudgetError) return err
    throw err
  }
  throw new Error('Expected SqlBudgetError but query completed')
}

describe('SqlExecutionBudget', () => {
  describe('createBudget', () => {
    it('returns undefined when no budget is provided', () => {
      expect(createBudget()).toBeUndefined()
      expect(createBudget(undefined)).toBeUndefined()
    })

    it('returns a tracker when a budget object is provided', () => {
      const tracker = createBudget({ maxRowsToMaterialize: 100 })
      expect(tracker).toBeDefined()
      expect(tracker?.budget).toEqual({ maxRowsToMaterialize: 100 })
    })

    it('defaults allowDerivedColumnScan to true', () => {
      const tracker = createBudget({ maxRowsToMaterialize: 100 })
      expect(tracker?.allowDerivedColumnScan).toBe(true)
    })

    it('honors allowDerivedColumnScan: false', () => {
      const tracker = createBudget({ allowDerivedColumnScan: false })
      expect(tracker?.allowDerivedColumnScan).toBe(false)
    })
  })

  describe('SqlBudgetError shape', () => {
    it('exposes structured limit/value/max/operator fields', async () => {
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT * FROM users ORDER BY id',
        budget: { maxRowsToMaterialize: 2 },
      })
      expect(err).toBeInstanceOf(SqlBudgetError)
      expect(err.name).toBe('SqlBudgetError')
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.value).toBe(3)
      expect(err.max).toBe(2)
      expect(err.operator).toBe('Sort')
      expect(err.message).toContain('maxRowsToMaterialize')
      expect(err.message).toContain('Sort')
    })
  })

  describe('maxRowsToMaterialize', () => {
    it('aborts ORDER BY when buffer exceeds limit', async () => {
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT id FROM users ORDER BY id',
        budget: { maxRowsToMaterialize: 3 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('Sort')
    })

    it('aborts GROUP BY when buffer exceeds limit', async () => {
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT team, COUNT(*) FROM users GROUP BY team',
        budget: { maxRowsToMaterialize: 2 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('HashAggregate')
    })

    it('aborts scalar aggregate (slow path) when buffer exceeds limit', async () => {
      // STDDEV_POP isn't on the scanColumn fast-path allowlist, so this falls
      // through to the slow path that buffers all rows.
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT STDDEV_POP(id) FROM users',
        budget: { maxRowsToMaterialize: 2 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('ScalarAggregate')
    })

    it('aborts HashJoin when right-side buffer exceeds limit', async () => {
      const err = await expectBudgetError({
        tables: { users, orders },
        query: 'SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id',
        budget: { maxRowsToMaterialize: 3 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('HashJoin')
    })

    it('aborts NestedLoopJoin when right-side buffer exceeds limit', async () => {
      const err = await expectBudgetError({
        tables: { users, orders },
        query: 'SELECT users.name, orders.amount FROM users JOIN orders ON users.id < orders.user_id + 100',
        budget: { maxRowsToMaterialize: 3 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('NestedLoopJoin')
    })

    it('aborts PositionalJoin when buffers exceed limit', async () => {
      const err = await expectBudgetError({
        tables: { users, orders },
        query: 'SELECT * FROM users POSITIONAL JOIN orders',
        budget: { maxRowsToMaterialize: 3 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('PositionalJoin')
    })

    it('aborts Distinct when distinct buffer exceeds limit', async () => {
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT DISTINCT team FROM users',
        budget: { maxRowsToMaterialize: 1 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('Distinct')
    })

    it('aborts UNION when seen-set exceeds limit', async () => {
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT id FROM users WHERE id < 4 UNION SELECT id FROM users WHERE id > 2',
        budget: { maxRowsToMaterialize: 2 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('Union')
    })

    it('aborts INTERSECT when right-keys exceed limit', async () => {
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT id FROM users INTERSECT SELECT id FROM users',
        budget: { maxRowsToMaterialize: 2 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('Intersect')
    })

    it('aborts EXCEPT when right-keys exceed limit', async () => {
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT id FROM users EXCEPT SELECT id FROM users WHERE id < 2',
        budget: { maxRowsToMaterialize: 0 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('Except')
    })

    it('aborts Window when buffer exceeds limit (with PARTITION BY)', async () => {
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT id, ROW_NUMBER() OVER (PARTITION BY team) AS rn FROM users',
        budget: { maxRowsToMaterialize: 3 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('Window')
    })

    it('lets queries that fit under the limit complete', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT id FROM users ORDER BY id',
        budget: { maxRowsToMaterialize: 100 },
      }))
      expect(result).toHaveLength(5)
    })

    it('shares the row counter across operators', async () => {
      // Sort buffers 5 rows; with limit=4 it must fail before the next operator.
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT * FROM users ORDER BY id',
        budget: { maxRowsToMaterialize: 4 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('Sort')
      expect(err.value).toBe(5)
      expect(err.max).toBe(4)
    })
  })

  describe('maxIntermediateBytes', () => {
    it('aborts a single operator when its buffer exceeds the per-operator byte limit', async () => {
      // Default 64 bytes per row. ORDER BY buffers all rows → 5 * 64 = 320.
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT * FROM users ORDER BY id',
        budget: { maxIntermediateBytes: 200 },
      })
      expect(err.limit).toBe('maxIntermediateBytes')
      expect(err.operator).toBe('Sort')
      expect(err.value).toBeGreaterThan(200)
      expect(err.max).toBe(200)
    })

    it('does not trip when each operator stays under its own per-operator limit', async () => {
      // GROUP BY buffers 5 rows = 320 bytes < 400. Per-operator only.
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT team FROM users GROUP BY team',
        budget: { maxIntermediateBytes: 400 },
      }))
      expect(result).toHaveLength(3)
    })
  })

  describe('maxHeapBytes', () => {
    it('aborts when total bytes across operators exceed the heap limit', async () => {
      // HashJoin buffers 5 right rows (320 bytes), then Sort starts. Sort
      // row 1 pushes total to 384, which trips threshold 350.
      const err = await expectBudgetError({
        tables: { users, orders },
        query: 'SELECT users.id FROM users JOIN orders ON users.id = orders.user_id ORDER BY users.id',
        budget: { maxHeapBytes: 350 },
      })
      expect(err.limit).toBe('maxHeapBytes')
      expect(err.value).toBeGreaterThan(350)
      expect(err.max).toBe(350)
      // The threshold trips inside the second materializing operator.
      expect(err.operator).toBe('Sort')
    })
  })

  describe('timeoutMs', () => {
    it('aborts a materializing query when the deadline has passed', async () => {
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT * FROM users ORDER BY id',
        budget: { timeoutMs: -1 },
      })
      expect(err.limit).toBe('timeoutMs')
      expect(err.max).toBe(-1)
      expect(err.operator).toBe('Sort')
    })

    it('aborts on the first row materialized when the deadline is already past', async () => {
      const err = await expectBudgetError({
        tables: { users },
        query: 'SELECT team, COUNT(*) FROM users GROUP BY team',
        budget: { timeoutMs: -1 },
      })
      expect(err.limit).toBe('timeoutMs')
    })

    it('does not abort when the deadline has not been reached', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM users ORDER BY id',
        budget: { timeoutMs: 60_000 },
      }))
      expect(result).toHaveLength(5)
    })
  })

  describe('allowDerivedColumnScan', () => {
    it('aggregate fast path runs without materializing rows', async () => {
      const { source, scanColumnCalls } = columnScannableSource(users)
      const result = await collect(executeSql({
        tables: { users: source },
        query: 'SELECT COUNT(id) AS c FROM users',
        budget: { maxRowsToMaterialize: 0 },
      }))
      expect(result).toEqual([{ c: 5 }])
      expect(scanColumnCalls()).toBe(1)
    })

    it('opts out of the aggregate fast path when allowDerivedColumnScan is false', async () => {
      const { source } = columnScannableSource(users)
      const err = await expectBudgetError({
        tables: { users: source },
        query: 'SELECT COUNT(id) AS c FROM users',
        budget: { allowDerivedColumnScan: false, maxRowsToMaterialize: 0 },
      })
      expect(err.limit).toBe('maxRowsToMaterialize')
      expect(err.operator).toBe('ScalarAggregate')
    })

    it('produces the same result with or without the fast path', async () => {
      const a = columnScannableSource(users)
      const b = columnScannableSource(users)
      const fast = await collect(executeSql({
        tables: { users: a.source },
        query: 'SELECT COUNT(id) AS c FROM users',
      }))
      const slow = await collect(executeSql({
        tables: { users: b.source },
        query: 'SELECT COUNT(id) AS c FROM users',
        budget: { allowDerivedColumnScan: false },
      }))
      expect(slow).toEqual(fast)
    })
  })

  describe('without a budget', () => {
    it('queries run unchanged', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT users.name FROM users JOIN orders ON users.id = orders.user_id ORDER BY users.id',
      }))
      expect(result).toHaveLength(5)
    })
  })
})
