import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'
import { memorySource } from '../../src/backend/dataSource.js'

/**
 * @import { AsyncDataSource, AsyncRow, ScanOptions } from '../../src/types.js'
 */

const users = [
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
  { id: 3, name: 'Charlie' },
  { id: 4, name: 'Diana' },
  { id: 5, name: 'Eve' },
]

const orders = [
  { id: 1, user_id: 1, amount: 100 },
  { id: 2, user_id: 1, amount: 200 },
  { id: 3, user_id: 2, amount: 150 },
  { id: 4, user_id: 3, amount: 300 },
  { id: 5, user_id: 4, amount: 250 },
]

describe('abort signal', () => {
  describe('executeSql with signal', () => {
    it('should stop execution when signal is aborted before query starts', async () => {
      const { source, getScannedCount } = trackingSource(users)
      const controller = new AbortController()
      controller.abort()

      const rows = []
      for await (const row of executeSql({
        tables: { users: source },
        query: 'SELECT * FROM users',
        signal: controller.signal,
      })) {
        rows.push(row)
      }
      expect(rows).toHaveLength(0)
      expect(getScannedCount()).toBe(0)
    })

    it('should stop execution when signal is aborted during query', async () => {
      const { source, getScannedCount } = trackingSource(users)
      const controller = new AbortController()

      const rows = []
      for await (const row of executeSql({
        tables: { users: source },
        query: 'SELECT * FROM users',
        signal: controller.signal,
      })) {
        rows.push(await row.cells['name']())
        if (rows.length === 2) {
          controller.abort()
        }
      }
      expect(rows).toEqual(['Alice', 'Bob'])
      expect(getScannedCount()).toBe(2)
    })

    it('should complete query when signal is not aborted', async () => {
      const { source, getScannedCount } = trackingSource(users)
      const controller = new AbortController()

      const result = await collect(executeSql({
        tables: { users: source },
        query: 'SELECT * FROM users',
        signal: controller.signal,
      }))

      expect(result).toHaveLength(5)
      expect(getScannedCount()).toBe(5)
    })
  })

  describe('data source scan', () => {
    it('should stop scan when signal is aborted before iteration', async () => {
      const { source, getScannedCount } = trackingSource(users)
      const controller = new AbortController()
      controller.abort()

      const rows = []
      for await (const row of source.scan({ signal: controller.signal })) {
        rows.push(row)
      }
      expect(rows).toHaveLength(0)
      expect(getScannedCount()).toBe(0)
    })

    it('should stop scan when signal is aborted during iteration', async () => {
      const { source, getScannedCount } = trackingSource(users)
      const controller = new AbortController()

      const rows = []
      for await (const row of source.scan({ signal: controller.signal })) {
        rows.push(await row.cells['name']())
        if (rows.length === 2) {
          controller.abort()
        }
      }
      expect(rows).toEqual(['Alice', 'Bob'])
      expect(getScannedCount()).toBe(2)
    })

    it('should scan all rows when signal is not aborted', async () => {
      const { source, getScannedCount } = trackingSource(users)
      const controller = new AbortController()

      const rows = []
      for await (const row of source.scan({ signal: controller.signal })) {
        rows.push(await row.cells['name']())
      }
      expect(rows).toHaveLength(5)
      expect(getScannedCount()).toBe(5)
    })

    it('should scan all rows when no signal provided', async () => {
      const { source, getScannedCount } = trackingSource(users)

      const rows = []
      for await (const row of source.scan({})) {
        rows.push(await row.cells['name']())
      }
      expect(rows).toHaveLength(5)
      expect(getScannedCount()).toBe(5)
    })
  })

  describe('join queries', () => {
    it('should scan all rows in a join query', async () => {
      const { source: usersSource, getScannedCount: getUsersScanned } = trackingSource(users)
      const { source: ordersSource, getScannedCount: getOrdersScanned } = trackingSource(orders)

      const result = await collect(executeSql({
        tables: { users: usersSource, orders: ordersSource },
        query: 'SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id',
      }))

      expect(result).toHaveLength(5)
      expect(getUsersScanned()).toBe(5)
      expect(getOrdersScanned()).toBe(5)
    })

    it('should stop scanning when consumer breaks early from join', async () => {
      const { source: usersSource, getScannedCount: getUsersScanned } = trackingSource(users)
      const { source: ordersSource, getScannedCount: getOrdersScanned } = trackingSource(orders)

      const rows = []
      for await (const row of executeSql({
        tables: { users: usersSource, orders: ordersSource },
        query: 'SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id',
      })) {
        rows.push({
          name: await row.cells['name'](),
          amount: await row.cells['amount'](),
        })
        if (rows.length === 2) break
      }

      expect(rows).toHaveLength(2)
      // Orders are buffered for hash join, but users stream
      expect(getOrdersScanned()).toBe(5) // right side is always buffered
      expect(getUsersScanned()).toBeLessThanOrEqual(2) // left side should stop early
    })
  })
})

/**
 * Creates a data source that tracks how many rows were scanned.
 * The source respects the abort signal.
 *
 * @param {Record<string, import('../../src/types.js').SqlPrimitive>[]} data
 * @returns {{ source: AsyncDataSource, getScannedCount: () => number }}
 */
function trackingSource(data) {
  const inner = memorySource(data)
  let scannedCount = 0

  return {
    source: {
      /**
       * @param {ScanOptions} options
       * @yields {AsyncRow}
       */
      async *scan(options) {
        for await (const row of inner.scan(options)) {
          if (options.signal?.aborted) break
          scannedCount++
          yield row
        }
      },
    },
    getScannedCount: () => scannedCount,
  }
}
