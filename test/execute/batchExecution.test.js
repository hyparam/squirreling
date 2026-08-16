import { describe, expect, it, vi } from 'vitest'
import { collect, executeSql } from '../../src/index.js'
import { batchResultsFor } from '../../src/execute/batchResults.js'

/**
 * @import { AsyncDataSource, ScanColumnResults } from '../../src/types.js'
 */

describe('native batch execution', () => {
  it('preserves a typed scan chunk through projection', async () => {
    const values = new Int32Array([10, 20, 30])
    const source = columnSource(function chunks() { return [values] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id AS value FROM data',
    })

    expect(Object.keys(results)).toEqual(['columns', 'numRows', 'maxRows', 'rows'])
    const batchResults = batchResultsFor(results)
    if (!batchResults) throw new Error('expected internal batches')
    const iterator = batchResults.batches()[Symbol.asyncIterator]()
    const first = await iterator.next()
    if (first.done) throw new Error('expected a batch')
    const column = first.value.columns[0]
    expect(column.type).toBe('loaded')
    if (column.type !== 'loaded') throw new Error('expected a loaded column')
    expect(column.vector.type).toBe('typed')
    if (column.vector.type !== 'typed') throw new Error('expected a typed vector')
    expect(column.vector.values).toBe(values)
    expect(first.value.schema.fields[0].name).toBe('value')
  })

  it('applies limit and offset as a zero-copy selection across chunks', async () => {
    const source = columnSource(function chunks() {
      return [[1, 2], [3, 4, 5], [6]]
    }, false)

    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data LIMIT 3 OFFSET 2',
    })

    expect(await collect(results)).toEqual([{ id: 3 }, { id: 4 }, { id: 5 }])
  })

  it('keeps rows as a compatibility view over native batches', async () => {
    const source = columnSource(function chunks() { return [[4, 5]] })
    const results = executeSql({ tables: { data: source }, query: 'SELECT id FROM data' })

    const rows = []
    for await (const row of results.rows()) rows.push(await row.cells.id())
    expect(rows).toEqual([4, 5])
  })

  it('keeps computed projection private and lazy at column granularity', async () => {
    const source = columnSource(function chunks() { return [['a', 'abcd', null]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT LENGTH(id) + 1 AS size FROM data',
    })

    expect(Object.keys(results)).toEqual(['columns', 'numRows', 'maxRows', 'rows'])
    const batchResults = batchResultsFor(results)
    if (!batchResults) throw new Error('expected internal batches')
    const iterator = batchResults.batches()[Symbol.asyncIterator]()
    const first = await iterator.next()
    if (first.done) throw new Error('expected a batch')
    const [column] = first.value.columns
    expect(column.type).toBe('computed')
    if (column.type !== 'computed') throw new Error('expected a computed column')
    expect(column.dependencies).toEqual([0])

    expect(await collect(results)).toEqual([{ size: 2 }, { size: 5 }, { size: null }])
  })

  it('collects computed projections without constructing compatibility rows', async () => {
    const source = columnSource(function chunks() { return [['one', 'three']] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT UPPER(id) AS value FROM data',
    })

    results.rows = vi.fn(function rows() {
      throw new Error('row adapter should not be called')
    })
    expect(await collect(results)).toEqual([{ value: 'ONE' }, { value: 'THREE' }])
    expect(results.rows).not.toHaveBeenCalled()
  })

  it('retains row projection for an unsupported batch expression', async () => {
    const source = columnSource(function chunks() { return [[null, 2]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT COALESCE(id, 0) AS value FROM data',
    })

    expect(batchResultsFor(results)).toBeUndefined()
    expect(await collect(results)).toEqual([{ value: 0 }, { value: 2 }])
  })

  it('turns a residual predicate into a private batch selection', async () => {
    const source = columnSource(function chunks() { return [[null, 2, 3, 4]] }, false)
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data WHERE id > 2',
    })

    expect(Object.keys(results)).toEqual(['columns', 'numRows', 'maxRows', 'rows'])
    expect(Object.hasOwn(results, 'batches')).toBe(false)
    expect(Object.hasOwn(results, 'schema')).toBe(false)
    const batchResults = batchResultsFor(results)
    if (!batchResults) throw new Error('expected internal batches')
    const iterator = batchResults.batches()[Symbol.asyncIterator]()
    const first = await iterator.next()
    if (first.done) throw new Error('expected a batch')
    expect(first.value.selection).toEqual({
      type: 'bitmap',
      values: new Uint8Array([0, 0, 1, 1]),
      length: 4,
    })
    expect(await collect(results)).toEqual([{ id: 3 }, { id: 4 }])
  })

  it('applies a residual predicate before limit and offset', async () => {
    const source = columnSource(function chunks() { return [[1, 2, 3, 4], [5, 6, 7, 8]] }, false)
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data WHERE id % 2 = 0 LIMIT 2 OFFSET 1',
    })

    expect(batchResultsFor(results)).toBeDefined()
    expect(await collect(results)).toEqual([{ id: 4 }, { id: 6 }])
  })

  it('retains row filtering for an unsupported batch predicate', async () => {
    const source = columnSource(function chunks() { return [[null, 2, 3]] }, false)
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data WHERE COALESCE(id, 0) > 2',
    })

    expect(batchResultsFor(results)).toBeUndefined()
    expect(await collect(results)).toEqual([{ id: 3 }])
  })

  it('filters a computed subquery through private batches', async () => {
    const source = columnSource(function chunks() { return [[1, 2, 3, 4]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT value FROM (SELECT id * 3 AS value FROM data) WHERE value > 6',
    })

    expect(Object.hasOwn(results, 'batches')).toBe(false)
    expect(Object.hasOwn(results, 'schema')).toBe(false)
    expect(batchResultsFor(results)).toBeDefined()
    expect(await collect(results)).toEqual([{ value: 9 }, { value: 12 }])
  })

  it('uses rows for an unsupported predicate over a batched subquery', async () => {
    const source = columnSource(function chunks() { return [[null, 2, 3]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT value FROM (SELECT id AS value FROM data) WHERE COALESCE(value, 0) > 2',
    })

    expect(batchResultsFor(results)).toBeUndefined()
    expect(await collect(results)).toEqual([{ value: 3 }])
  })
})

/**
 * @param {() => ArrayLike<import('../../src/types.js').SqlPrimitive>[]} readChunks
 * @param {boolean} [appliedLimitOffset]
 * @returns {AsyncDataSource}
 */
function columnSource(readChunks, appliedLimitOffset = true) {
  return {
    columns: ['id'],
    scan: vi.fn(function scan() {
      throw new Error('row scan should not be called')
    }),
    scanColumn() {
      /** @type {ScanColumnResults} */
      const result = {
        appliedWhere: false,
        appliedLimitOffset,
        async *chunks() {
          yield* readChunks()
        },
      }
      return result
    },
  }
}
