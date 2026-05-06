import { describe, expect, it } from 'vitest'
import {
  adaptBatchesToRows,
  adaptRowsToBatches,
  scanBatches,
} from '../../src/index.js'
import { asyncRow, memorySource } from '../../src/backend/dataSource.js'

/**
 * @import { AsyncDataSource, AsyncRow, ColumnBatch, SqlPrimitive } from '../../src/types.js'
 */

/**
 * Collect an async iterable into an array.
 * @template T
 * @param {AsyncIterable<T>} iter
 * @returns {Promise<T[]>}
 */
async function collectAll(iter) {
  /** @type {T[]} */
  const out = []
  for await (const item of iter) out.push(item)
  return out
}

/**
 * Materialize an AsyncRow's cells into a plain object using the given columns.
 * @param {AsyncRow} row
 * @param {string[]} columns
 * @returns {Promise<Record<string, SqlPrimitive>>}
 */
async function rowToObject(row, columns) {
  /** @type {Record<string, SqlPrimitive>} */
  const obj = {}
  for (const col of columns) obj[col] = await row.cells[col]()
  return obj
}

/**
 * Build an async iterable of rows from an array of plain objects.
 * @param {Record<string, SqlPrimitive>[]} data
 * @param {string[]} columns
 * @returns {AsyncIterable<AsyncRow>}
 */
async function* rowsFrom(data, columns) {
  for (const obj of data) yield asyncRow(obj, columns)
}

describe('adaptRowsToBatches', () => {
  it('yields nothing for empty input', async () => {
    const batches = await collectAll(adaptRowsToBatches(rowsFrom([], ['a']), ['a']))
    expect(batches).toEqual([])
  })

  it('packs rows into a single batch when below batch size', async () => {
    const data = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]
    const batches = await collectAll(adaptRowsToBatches(rowsFrom(data, ['id', 'name']), ['id', 'name']))
    expect(batches).toEqual([
      {
        rowStart: 0,
        rowCount: 2,
        columns: { id: [1, 2], name: ['Alice', 'Bob'] },
      },
    ])
  })

  it('splits rows into multiple batches at batchSize', async () => {
    const data = [{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }]
    const batches = await collectAll(adaptRowsToBatches(rowsFrom(data, ['x']), ['x'], { batchSize: 2 }))
    expect(batches).toEqual([
      { rowStart: 0, rowCount: 2, columns: { x: [1, 2] } },
      { rowStart: 2, rowCount: 2, columns: { x: [3, 4] } },
      { rowStart: 4, rowCount: 1, columns: { x: [5] } },
    ])
  })

  it('respects rowStart option for the first batch', async () => {
    const data = [{ x: 1 }, { x: 2 }]
    const batches = await collectAll(adaptRowsToBatches(rowsFrom(data, ['x']), ['x'], { rowStart: 100 }))
    expect(batches[0].rowStart).toBe(100)
  })

  it('only materializes the requested columns', async () => {
    let cellsCalled = 0
    /** @type {AsyncRow} */
    const row = {
      columns: ['a', 'b'],
      cells: {
        a: () => { cellsCalled++; return Promise.resolve(1) },
        b: () => { cellsCalled++; return Promise.resolve(2) },
      },
    }
    async function* one() { yield row }
    const batches = await collectAll(adaptRowsToBatches(one(), ['a']))
    expect(batches).toEqual([{ rowStart: 0, rowCount: 1, columns: { a: [1] } }])
    expect(cellsCalled).toBe(1)
  })

  it('reads from row.resolved when present without invoking cells', async () => {
    let cellsCalled = 0
    /** @type {AsyncRow} */
    const row = {
      columns: ['a'],
      cells: {
        a: () => { cellsCalled++; return Promise.resolve(99) },
      },
      resolved: { a: 7 },
    }
    async function* one() { yield row }
    const batches = await collectAll(adaptRowsToBatches(one(), ['a']))
    expect(batches[0].columns.a[0]).toBe(7)
    expect(cellsCalled).toBe(0)
  })

  it('stops when the abort signal fires', async () => {
    const controller = new AbortController()
    const data = [{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }]
    async function* rows() {
      for (const obj of data) {
        yield asyncRow(obj, ['x'])
        if (obj.x === 2) controller.abort()
      }
    }
    const batches = await collectAll(adaptRowsToBatches(rows(), ['x'], { batchSize: 1, signal: controller.signal }))
    expect(batches).toEqual([
      { rowStart: 0, rowCount: 1, columns: { x: [1] } },
      { rowStart: 1, rowCount: 1, columns: { x: [2] } },
    ])
  })
})

describe('adaptBatchesToRows', () => {
  it('yields nothing for empty input', async () => {
    async function* empty() {}
    const rows = await collectAll(adaptBatchesToRows(empty()))
    expect(rows).toEqual([])
  })

  it('flattens batches into rows in order', async () => {
    /** @type {ColumnBatch[]} */
    const batches = [
      { rowStart: 0, rowCount: 2, columns: { id: [1, 2], name: ['Alice', 'Bob'] } },
      { rowStart: 2, rowCount: 1, columns: { id: [3], name: ['Carol'] } },
    ]
    async function* gen() { for (const b of batches) yield b }
    const rows = await collectAll(adaptBatchesToRows(gen()))
    const objects = await Promise.all(rows.map(r => rowToObject(r, r.columns)))
    expect(objects).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Carol' },
    ])
  })

  it('exposes batch values via row.resolved', async () => {
    /** @type {ColumnBatch} */
    const batch = { rowStart: 0, rowCount: 1, columns: { x: [42] } }
    async function* gen() { yield batch }
    const rows = await collectAll(adaptBatchesToRows(gen()))
    expect(rows[0].resolved).toEqual({ x: 42 })
  })

  it('uses rowCount, not column array length, to bound row emission', async () => {
    /** @type {ColumnBatch} */
    const batch = {
      rowStart: 0,
      rowCount: 2,
      columns: { x: [1, 2, 999, 999] },
    }
    async function* gen() { yield batch }
    const rows = await collectAll(adaptBatchesToRows(gen()))
    expect(rows).toHaveLength(2)
    const xs = await Promise.all(rows.map(r => r.cells.x()))
    expect(xs).toEqual([1, 2])
  })
})

describe('round-trip rows -> batches -> rows', () => {
  it('preserves all values for representative shapes', async () => {
    /** @type {Record<string, SqlPrimitive>[]} */
    const data = [
      { id: 1, name: 'Alice', score: 1.5, active: true, tags: ['a', 'b'], note: null },
      { id: 2, name: 'Bob', score: 2.5, active: false, tags: [], note: 'hi' },
      { id: 3, name: 'Carol', score: 3.5, active: true, tags: ['x'], note: null },
    ]
    const columns = ['id', 'name', 'score', 'active', 'tags', 'note']
    const batches = adaptRowsToBatches(rowsFrom(data, columns), columns, { batchSize: 2 })
    const rows = await collectAll(adaptBatchesToRows(batches))
    const objects = await Promise.all(rows.map(r => rowToObject(r, columns)))
    expect(objects).toEqual(data)
  })

  it('preserves order across batch boundaries', async () => {
    const data = Array.from({ length: 10 }, (_, i) => ({ i }))
    const batches = adaptRowsToBatches(rowsFrom(data, ['i']), ['i'], { batchSize: 3 })
    const rows = await collectAll(adaptBatchesToRows(batches))
    const objects = await Promise.all(rows.map(r => rowToObject(r, ['i'])))
    expect(objects).toEqual(data)
  })
})

describe('round-trip batches -> rows -> batches', () => {
  it('produces the same column data', async () => {
    /** @type {ColumnBatch[]} */
    const original = [
      { rowStart: 0, rowCount: 2, columns: { x: [10, 20], y: ['a', 'b'] } },
      { rowStart: 2, rowCount: 2, columns: { x: [30, 40], y: ['c', 'd'] } },
    ]
    async function* gen() { for (const b of original) yield b }
    const rows = adaptBatchesToRows(gen())
    const batches = await collectAll(adaptRowsToBatches(rows, ['x', 'y'], { batchSize: 2 }))
    expect(batches).toEqual([
      { rowStart: 0, rowCount: 2, columns: { x: [10, 20], y: ['a', 'b'] } },
      { rowStart: 2, rowCount: 2, columns: { x: [30, 40], y: ['c', 'd'] } },
    ])
  })
})

describe('scanBatches helper', () => {
  it('falls back to scan() when the source has no scanBatches', async () => {
    const source = memorySource({ data: [{ id: 1 }, { id: 2 }, { id: 3 }] })
    const batches = await collectAll(scanBatches(source, { batchSize: 2 }))
    expect(batches).toEqual([
      { rowStart: 0, rowCount: 2, columns: { id: [1, 2] } },
      { rowStart: 2, rowCount: 1, columns: { id: [3] } },
    ])
  })

  it('uses the native scanBatches when available', async () => {
    let nativeCalled = 0
    let scanCalled = 0
    /** @type {AsyncDataSource} */
    const source = {
      columns: ['x'],
      scan() {
        scanCalled++
        return { async *rows() {}, appliedWhere: false, appliedLimitOffset: false }
      },
      async *scanBatches() {
        nativeCalled++
        yield { rowStart: 0, rowCount: 1, columns: { x: [42] } }
      },
    }
    const batches = await collectAll(scanBatches(source))
    expect(nativeCalled).toBe(1)
    expect(scanCalled).toBe(0)
    expect(batches).toEqual([{ rowStart: 0, rowCount: 1, columns: { x: [42] } }])
  })

  it('forwards columns hint to scan() in the fallback path', async () => {
    /** @type {string[] | undefined} */
    let receivedColumns
    /** @type {AsyncDataSource} */
    const source = {
      columns: ['a', 'b'],
      scan(opts) {
        receivedColumns = opts.columns
        return {
          async *rows() { yield asyncRow({ a: 1, b: 2 }, ['a', 'b']) },
          appliedWhere: false,
          appliedLimitOffset: false,
        }
      },
    }
    await collectAll(scanBatches(source, { columns: ['a'] }))
    expect(receivedColumns).toEqual(['a'])
  })

  it('honors AsyncDataSource.scanBatches as optional in the type system', () => {
    /** @type {AsyncDataSource} */
    const source = {
      columns: ['x'],
      scan() {
        return { async *rows() {}, appliedWhere: false, appliedLimitOffset: false }
      },
    }
    expect(source.scanBatches).toBeUndefined()
  })
})
