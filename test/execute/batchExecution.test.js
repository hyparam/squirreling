import { describe, expect, it, vi } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * @import { AsyncDataSource, ScanColumnResults } from '../../src/types.js'
 */

describe('native batch execution', () => {
  it('preserves a typed scan chunk through projection', async () => {
    const values = new Int32Array([10, 20, 30])
    const source = columnSource(function chunks() { return [values] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT data.id AS value FROM data',
    })

    expect(results.batches).toBeTypeOf('function')
    if (!results.batches) throw new Error('expected native batches')
    const iterator = results.batches()[Symbol.asyncIterator]()
    const first = await iterator.next()
    if (first.done) throw new Error('expected a batch')
    const column = first.value.columns[0]
    expect(column.type).toBe('typed')
    if (column.type !== 'typed') throw new Error('expected a typed vector')
    expect(column.values).toBe(values)
    expect(first.value.columnNames[0]).toBe('value')
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

  it('keeps computed projection lazy at column granularity', async () => {
    const source = columnSource(function chunks() { return [['a', 'abcd', null]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT LENGTH(id) + 1 AS size FROM data',
    })

    expect(results.batches).toBeTypeOf('function')
    if (!results.batches) throw new Error('expected native batches')
    const iterator = results.batches()[Symbol.asyncIterator]()
    const first = await iterator.next()
    if (first.done) throw new Error('expected a batch')
    const [column] = first.value.columns
    expect(column.type).toBe('computed')
    if (column.type !== 'computed') throw new Error('expected a computed column')
    expect(column.expression).toBeDefined()

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

  it('executes CASE projections through private batches', async () => {
    const source = columnSource(function chunks() { return [[null, 2]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT CASE WHEN id IS NULL THEN 0 ELSE id END AS value FROM data',
    })

    expect(results.batches).toBeTypeOf('function')
    expect(await collect(results)).toEqual([{ value: 0 }, { value: 2 }])
  })

  it('turns a residual predicate into a batch selection', async () => {
    const source = columnSource(function chunks() { return [[null, 2, 3, 4]] }, false)
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data WHERE id > 2',
    })

    expect(results.batches).toBeTypeOf('function')
    if (!results.batches) throw new Error('expected native batches')
    const iterator = results.batches()[Symbol.asyncIterator]()
    const first = await iterator.next()
    if (first.done) throw new Error('expected a batch')
    expect(first.value.selection).toEqual({
      type: 'indices',
      indices: new Uint32Array([2, 3]),
      length: 4,
      selectedCount: 2,
    })
    expect(await collect(results)).toEqual([{ id: 3 }, { id: 4 }])
  })
  it('applies a residual predicate before limit and offset', async () => {
    const source = columnSource(function chunks() { return [[1, 2, 3, 4], [5, 6, 7, 8]] }, false)
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data WHERE id % 2 = 0 LIMIT 2 OFFSET 1',
    })

    expect(results.batches).toBeTypeOf('function')
    expect(await collect(results)).toEqual([{ id: 4 }, { id: 6 }])
  })

  it('executes compound logical predicates through private batches', async () => {
    const source = columnSource(function chunks() { return [[1, 2, 3, 4, 5]] }, false)
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data WHERE (id > 1 AND id < 4) OR id = 5',
    })

    expect(results.batches).toBeTypeOf('function')
    expect(await collect(results)).toEqual([{ id: 2 }, { id: 3 }, { id: 5 }])
  })

  it('executes CASE predicates through private batches', async () => {
    const source = columnSource(function chunks() { return [[null, 2, 3]] }, false)
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT id FROM data WHERE CASE WHEN id IS NULL THEN 0 ELSE id END > 2',
    })

    expect(results.batches).toBeTypeOf('function')
    expect(await collect(results)).toEqual([{ id: 3 }])
  })

  it('filters a computed subquery through batches', async () => {
    const source = columnSource(function chunks() { return [[1, 2, 3, 4]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT value FROM (SELECT id * 3 AS value FROM data) WHERE value > 6',
    })

    expect(results.batches).toBeTypeOf('function')
    expect(results.schema).toBeDefined()
    expect(await collect(results)).toEqual([{ value: 9 }, { value: 12 }])
  })

  it('keeps CASE predicates over subqueries in private batches', async () => {
    const source = columnSource(function chunks() { return [[null, 2, 3]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT value FROM (SELECT id AS value FROM data) WHERE CASE WHEN value IS NULL THEN 0 ELSE value END > 2',
    })

    expect(results.batches).toBeTypeOf('function')
    expect(await collect(results)).toEqual([{ value: 3 }])
  })

  it('preserves outer scope in filters above private batch subqueries', async () => {
    const orders = columnSource(function chunks() { return [[1]] }, true, 'user_id')

    await expect(collect(executeSql({
      tables: { users: [{ id: 1 }, { id: 2 }], orders },
      query: `SELECT u.id FROM users u
        WHERE EXISTS (
          SELECT 1 FROM (SELECT user_id AS id FROM orders) q
          WHERE q.id = u.id
        )
        ORDER BY u.id`,
    }))).resolves.toEqual([{ id: 1 }])
  })

  it('preserves outer scope in private batch projections', async () => {
    const items = columnSource(function chunks() { return [[{ id: 99 }]] }, true, 'u')

    await expect(collect(executeSql({
      tables: { users: [{ id: 1 }, { id: 2 }], items },
      query: `SELECT u.id,
        (SELECT CASE WHEN u IS NOT NULL THEN u.id ELSE 0 END FROM items) AS projected
        FROM users u ORDER BY u.id`,
    }))).resolves.toEqual([
      { id: 1, projected: 1 },
      { id: 2, projected: 2 },
    ])
  })

  it('executes distinct over computed batches', async () => {
    const source = columnSource(function chunks() { return [[1, 2, 3], [4, 5]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT DISTINCT id % 2 AS value FROM data',
    })

    expect(results.batches).toBeTypeOf('function')
    expect(results.schema).toBeDefined()
    expect(await collect(results)).toEqual([{ value: 1 }, { value: 0 }])
  })

  it('falls back to row projection for duplicate aliases', async () => {
    const source = columnSource(function chunks() { return [[1, 2]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT DISTINCT x FROM (SELECT id AS x, 1 AS x FROM data)',
    })

    expect(batchResultsFor(results)).toBeDefined()
    expect(await collect(results)).toEqual([{ x: 1 }])
  })

  it('preserves projected row positions through a later offset', async () => {
    const source = columnSource(function chunks() { return [[1, 2, 3, { value: 4 }]] })
    const results = executeSql({
      tables: { data: source },
      query: 'SELECT value FROM (SELECT CAST(id AS INTEGER) AS value FROM data) OFFSET 3',
    })

    await expect(collect(results)).rejects.toThrow('Cannot CAST object to INTEGER (row 4)')
  })
})

/**
 * @param {() => ArrayLike<import('../../src/types.js').SqlPrimitive>[]} readChunks
 * @param {boolean} [appliedLimitOffset]
 * @param {string} [column]
 * @returns {AsyncDataSource}
 */
function columnSource(readChunks, appliedLimitOffset = true, column = 'id') {
  return {
    columns: [column],
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
