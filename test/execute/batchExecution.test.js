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

  it('uses row execution when a source declines filter pushdown', async () => {
    const source = columnSource(function chunks() { return [[1, 2, 3, 4]] }, false)
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data WHERE id > 2',
    })

    expect(batchResultsFor(results)).toBeUndefined()
    expect(Object.hasOwn(results, 'batches')).toBe(false)
    expect(Object.hasOwn(results, 'schema')).toBe(false)
    expect(await collect(results)).toEqual([{ id: 3 }, { id: 4 }])
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
