import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * @import { AsyncDataSource, ScanOptions } from '../../src/types.js'
 */

describe('scan hints', () => {
  it('should not pass * as a column hint for SUM(*)', async () => {
    const hints = await captureHints('SELECT SUM(amount) FROM data')
    expect(hints.columns).not.toContain('*')
    expect(hints.columns).toEqual(['amount'])
  })

  it('should pass column hints for COUNT(column)', async () => {
    const hints = await captureHints('SELECT COUNT(name) FROM data')
    expect(hints.columns).toEqual(['name'])
  })

  it('should pass column hints for selected columns', async () => {
    const hints = await captureHints('SELECT id, name FROM data')
    expect(hints.columns).toEqual(['id', 'name'])
  })

  it('should pass column hints for selected expression', async () => {
    const hints = await captureHints('SELECT a + b FROM data')
    expect(hints.columns).toEqual(['a', 'b'])
  })

  it('should pass column and where hints from WHERE clause', async () => {
    const hints = await captureHints('SELECT id FROM data WHERE name = \'Alice\'')
    expect(hints.columns).toEqual(['id', 'name'])
    expect(hints.where).toMatchObject({ type: 'binary', op: '=' })
  })

  it('should return undefined columns for SELECT *', async () => {
    const hints = await captureHints('SELECT * FROM data')
    expect(hints.columns).toBeUndefined()
  })

  it('should return undefined columns for * with other columns', async () => {
    const hints = await captureHints('SELECT *, id FROM data')
    expect(hints.columns).toBeUndefined()
  })

  it('should pass limit hint', async () => {
    const hints = await captureHints('SELECT * FROM data LIMIT 10')
    expect(hints.limit).toBe(10)
  })

  it('should pass offset hint', async () => {
    const hints = await captureHints('SELECT * FROM data OFFSET 5')
    expect(hints.offset).toBe(5)
  })

  it('should not pass limit/offset hints for ORDER BY queries', async () => {
    const hints = await captureHints('SELECT id FROM data ORDER BY name LIMIT 10 OFFSET 5')
    expect(hints.columns).toEqual(['id', 'name'])
    expect(hints.limit).toBeUndefined()
    expect(hints.offset).toBeUndefined()
  })

  it('should not pass limit/offset hints for DISTINCT queries', async () => {
    const hints = await captureHints('SELECT DISTINCT id FROM data LIMIT 10 OFFSET 5')
    expect(hints.columns).toEqual(['id'])
    expect(hints.limit).toBeUndefined()
    expect(hints.offset).toBeUndefined()
  })
})

describe('join scan hints', () => {
  it('should pass per-table column hints in join queries', async () => {
    /** @type {ScanOptions} */
    let usersHints = {}
    /** @type {ScanOptions} */
    let ordersHints = {}

    /** @type {AsyncDataSource} */
    const usersSource = {
      scan(options) {
        usersHints = options
        return {
          rows: (async function* () {})(),
          appliedWhere: false,
          appliedLimitOffset: false,
        }
      },
    }

    /** @type {AsyncDataSource} */
    const ordersSource = {
      scan(options) {
        ordersHints = options
        return {
          rows: (async function* () {})(),
          appliedWhere: false,
          appliedLimitOffset: false,
        }
      },
    }

    await collect(executeSql({
      tables: { users: usersSource, orders: ordersSource },
      query: 'SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id',
    }))

    expect(usersHints.columns).toEqual(['name', 'id'])
    expect(ordersHints.columns).toEqual(['total', 'user_id'])
  })
})

describe('scan results', () => {
  it('should throw if scan() does not return a ScanResults object', async () => {
    // Test error handling when scan() returns AsyncGenerator (older api)
    const badSource = {
      scan() {
        return (async function* () {})()
      },
    }
    await expect(collect(executeSql({
      // @ts-expect-error testing invalid scan() return type
      tables: { data: badSource },
      query: 'SELECT * FROM data',
    }))).rejects.toThrow('scan() must return a ScanResults object')
  })

  it('should throw if data source applies limit/offset without where', async () => {
    /** @type {AsyncDataSource} */
    const badSource = {
      scan() {
        return {
          rows: (async function* () {})(),
          appliedWhere: false,
          appliedLimitOffset: true,
        }
      },
    }
    await expect(collect(executeSql({
      tables: { data: badSource },
      query: 'SELECT * FROM data WHERE id = 1 LIMIT 5 OFFSET 2',
    }))).rejects.toThrow('Data source "data" applied limit/offset without applying where')
  })
})

/**
 * Executes a query and captures the hints passed to the data source.
 *
 * @param {string} query
 * @returns {Promise<ScanOptions>}
 */
async function captureHints(query) {
  /** @type {ScanOptions} */
  let capturedHints = {}
  /** @type {AsyncDataSource} */
  const capturingSource = {
    scan(options) {
      capturedHints = options
      return {
        rows: (async function* () {})(),
        appliedWhere: false,
        appliedLimitOffset: false,
      }
    },
  }
  await collect(executeSql({
    tables: { data: capturingSource },
    query,
  }))
  return capturedHints
}
