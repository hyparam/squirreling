import { describe, expect, it } from 'vitest'
import { collect, executeSql, readCell } from '../../src/index.js'
import { trackingSource } from './trackingSource.js'

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
      const { source, getScanCount, getRowCount } = trackingSource(users)
      const controller = new AbortController()
      controller.abort()

      const rows = []
      for await (const row of executeSql({
        tables: { users: source },
        query: 'SELECT * FROM users',
        signal: controller.signal,
      }).rows()) {
        rows.push(row)
      }
      expect(rows).toHaveLength(0)
      expect(getScanCount()).toBe(1)
      expect(getRowCount()).toBe(0)
    })

    it('should stop execution when signal is aborted during query', async () => {
      const { source, getScanCount, getRowCount } = trackingSource(users)
      const controller = new AbortController()

      const rows = []
      for await (const row of executeSql({
        tables: { users: source },
        query: 'SELECT * FROM users',
        signal: controller.signal,
      }).rows()) {
        rows.push(await readCell(row.cells['name']))
        if (rows.length === 2) {
          controller.abort()
        }
      }
      expect(rows).toEqual(['Alice', 'Bob'])
      expect(getScanCount()).toBe(1) // scan is started but should stop early
      expect(getRowCount()).toBe(2)
    })

    it('should complete query when signal is not aborted', async () => {
      const { source, getScanCount, getRowCount } = trackingSource(users)
      const controller = new AbortController()

      const result = await collect(executeSql({
        tables: { users: source },
        query: 'SELECT * FROM users',
        signal: controller.signal,
      }))

      expect(result).toHaveLength(5)
      expect(getScanCount()).toBe(1)
      expect(getRowCount()).toBe(5)
    })
  })

  describe('data source scan', () => {
    it('should stop scan when signal is aborted before iteration', async () => {
      const { source, getScanCount, getRowCount } = trackingSource(users)
      const controller = new AbortController()
      controller.abort()

      const rows = []
      for await (const row of source.scan({ signal: controller.signal }).rows()) {
        rows.push(row)
      }
      expect(rows).toHaveLength(0)
      expect(getScanCount()).toBe(1)
      expect(getRowCount()).toBe(0)
    })

    it('should stop scan when signal is aborted during iteration', async () => {
      const { source, getScanCount, getRowCount } = trackingSource(users)
      const controller = new AbortController()

      const rows = []
      for await (const row of source.scan({ signal: controller.signal }).rows()) {
        rows.push(await readCell(row.cells['name']))
        if (rows.length === 2) {
          controller.abort()
        }
      }
      expect(rows).toEqual(['Alice', 'Bob'])
      expect(getScanCount()).toBe(1)
      expect(getRowCount()).toBe(2)
    })

    it('should scan all rows when signal is not aborted', async () => {
      const { source, getScanCount, getRowCount } = trackingSource(users)
      const controller = new AbortController()

      const rows = []
      for await (const row of source.scan({ signal: controller.signal }).rows()) {
        rows.push(await readCell(row.cells['name']))
      }
      expect(rows).toHaveLength(5)
      expect(getScanCount()).toBe(1)
      expect(getRowCount()).toBe(5)
    })

    it('should scan all rows when no signal provided', async () => {
      const { source, getScanCount, getRowCount } = trackingSource(users)

      const rows = []
      for await (const row of source.scan({}).rows()) {
        rows.push(await readCell(row.cells['name']))
      }
      expect(rows).toHaveLength(5)
      expect(getScanCount()).toBe(1)
      expect(getRowCount()).toBe(5)
    })
  })

  describe('join queries', () => {
    it('should scan all rows in a join query', async () => {
      const { source: usersSource, getScanCount: getUsersScanCount, getRowCount: getUsersRowCount } = trackingSource(users)
      const { source: ordersSource, getScanCount: getOrdersScanCount, getRowCount: getOrdersRowCount } = trackingSource(orders)

      const result = await collect(executeSql({
        tables: { users: usersSource, orders: ordersSource },
        query: 'SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id',
      }))

      expect(result).toHaveLength(5)
      expect(getUsersScanCount()).toBe(1)
      expect(getOrdersScanCount()).toBe(1)
      expect(getUsersRowCount()).toBe(5)
      expect(getOrdersRowCount()).toBe(5)
    })

    it('should stop scanning when consumer breaks early from join', async () => {
      const { source: usersSource, getScanCount: getUsersScanCount, getRowCount: getUsersRowCount } = trackingSource(users)
      const { source: ordersSource, getScanCount: getOrdersScanCount, getRowCount: getOrdersRowCount } = trackingSource(orders)

      const rows = []
      for await (const row of executeSql({
        tables: { users: usersSource, orders: ordersSource },
        query: 'SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id',
      }).rows()) {
        rows.push({
          name: await readCell(row.cells['name']),
          amount: await readCell(row.cells['amount']),
        })
        if (rows.length === 2) break
      }

      expect(rows).toHaveLength(2)
      // Orders are buffered for hash join, but users stream
      expect(getOrdersScanCount()).toBe(1)
      expect(getUsersScanCount()).toBe(1)
      expect(getOrdersRowCount()).toBe(5) // right side is always buffered
      expect(getUsersRowCount()).toBe(1) // left side should stop early
    })
  })
})
