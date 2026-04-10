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
    expect(hints.where).toEqual({
      type: 'binary',
      op: '=',
      left: { type: 'identifier', name: 'name', positionStart: 26, positionEnd: 30 },
      right: { type: 'literal', value: 'Alice', positionStart: 33, positionEnd: 40 },
      positionStart: 26,
      positionEnd: 40,
    })
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
      columns: ['id', 'name'],
      scan(options) {
        usersHints = options
        return {
          async *rows() {},
          appliedWhere: false,
          appliedLimitOffset: false,
        }
      },
    }

    /** @type {AsyncDataSource} */
    const ordersSource = {
      columns: ['user_id', 'total'],
      scan(options) {
        ordersHints = options
        return {
          async *rows() {},
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
  it('should throw if data source applies limit/offset without where', () => {
    /** @type {AsyncDataSource} */
    const badSource = {
      columns: ['id'],
      scan() {
        return {
          async *rows() {},
          appliedWhere: false,
          appliedLimitOffset: true,
        }
      },
    }
    expect(() => executeSql({
      tables: { data: badSource },
      query: 'SELECT * FROM data WHERE id = 1 LIMIT 5 OFFSET 2',
    })).toThrow('Data source "data" applied limit/offset without applying where')
  })
})

describe('scanColumn fast path', () => {
  it('should use scanColumn for single column query without WHERE', async () => {
    let scanCalled = false
    let scanColumnCalled = false

    /** @type {AsyncDataSource} */
    const source = {
      columns: ['id', 'name'],
      scan() {
        scanCalled = true
        return {
          async *rows() {},
          appliedWhere: false,
          appliedLimitOffset: false,
        }
      },
      async *scanColumn() {
        scanColumnCalled = true
        yield [1, 2, 3]
      },
    }

    const result = await collect(executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data',
    }))
    expect(scanColumnCalled).toBe(true)
    expect(scanCalled).toBe(false)
    expect(result).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }])
  })

  it('should use scanColumn fast path with limit and offset', async () => {
    /** @type {AsyncDataSource} */
    const source = {
      numRows: 100,
      columns: ['id', 'name'],
      scan() {
        throw new Error('scan should not be called')
      },
      async *scanColumn({ column, limit, offset }) {
        expect(column).toBe('id')
        expect(limit).toBe(10)
        expect(offset).toBe(5)
        yield [6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
      },
    }

    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data LIMIT 10 OFFSET 5',
    })

    expect(await collect(results)).toEqual([
      { id: 6 },
      { id: 7 },
      { id: 8 },
      { id: 9 },
      { id: 10 },
      { id: 11 },
      { id: 12 },
      { id: 13 },
      { id: 14 },
      { id: 15 },
    ])
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
    columns: ['id', 'name', 'amount', 'a', 'b'],
    scan(options) {
      capturedHints = options
      return {
        async *rows() {},
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
