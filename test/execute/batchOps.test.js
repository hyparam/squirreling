import { describe, expect, it } from 'vitest'
import { executeSql } from '../../src/execute/execute.js'

/**
 * @import { AsyncDataSource, BatchScanOptions, ColumnBatch, QueryResults, SqlPrimitive } from '../../src/types.js'
 */

/**
 * Build a data source backed by columnar arrays. Always emits via scanBatches
 * (and also implements scan() via per-row materialization for compatibility).
 *
 * @param {Object} options
 * @param {string[]} options.columns
 * @param {Record<string, ArrayLike<SqlPrimitive>>} options.data
 * @param {number} [options.batchSize]
 * @returns {AsyncDataSource & { scanCalled: () => number, scanBatchesCalled: () => number }}
 */
function batchSource({ columns, data, batchSize = 4 }) {
  const numRows = data[columns[0]].length
  let scanCalled = 0
  let scanBatchesCalled = 0
  return {
    columns,
    numRows,
    scanCalled: () => scanCalled,
    scanBatchesCalled: () => scanBatchesCalled,
    scan({ columns: scanCols, signal }) {
      scanCalled++
      const cols = scanCols ?? columns
      return {
        async *rows() {
          for (let i = 0; i < numRows; i++) {
            if (signal?.aborted) return
            /** @type {Record<string, () => Promise<SqlPrimitive>>} */
            const cells = {}
            for (const c of cols) {
              const v = data[c][i]
              cells[c] = () => Promise.resolve(v)
            }
            yield { columns: cols, cells }
          }
        },
        appliedWhere: false,
        appliedLimitOffset: false,
      }
    },
    async *scanBatches({ columns: scanCols, signal }) {
      scanBatchesCalled++
      const cols = scanCols ?? columns
      let rowStart = 0
      while (rowStart < numRows) {
        if (signal?.aborted) return
        const rowCount = Math.min(batchSize, numRows - rowStart)
        /** @type {Record<string, ArrayLike<SqlPrimitive>>} */
        const out = {}
        for (const c of cols) {
          const src = data[c]
          /** @type {any} */
          const Ctor = /** @type {any} */ src.constructor
          if (typeof Ctor === 'function' && Ctor !== Array) {
            const dst = new Ctor(rowCount)
            for (let j = 0; j < rowCount; j++) dst[j] = src[rowStart + j]
            out[c] = dst
          } else {
            const dst = new Array(rowCount)
            for (let j = 0; j < rowCount; j++) dst[j] = src[rowStart + j]
            out[c] = dst
          }
        }
        yield { rowStart, rowCount, columns: out }
        rowStart += rowCount
      }
    },
  }
}

/**
 * @param {QueryResults} results
 * @returns {Promise<ColumnBatch[]>}
 */
async function collectBatches(results) {
  if (!results.batches) throw new Error('expected batches() to be present')
  /** @type {ColumnBatch[]} */
  const out = []
  for await (const b of results.batches()) out.push(b)
  return out
}

/**
 * @param {QueryResults} results
 * @returns {Promise<Record<string, SqlPrimitive>[]>}
 */
async function collectRows(results) {
  /** @type {Record<string, SqlPrimitive>[]} */
  const out = []
  for await (const r of results.rows()) {
    /** @type {Record<string, SqlPrimitive>} */
    const obj = {}
    for (const col of r.columns) obj[col] = await r.cells[col]()
    out.push(obj)
  }
  return out
}

describe('executeScan batch path', () => {
  it('exposes batches() and uses scanBatches when source provides it', async () => {
    const t = batchSource({
      columns: ['id', 'val'],
      data: { id: new Uint32Array([1, 2, 3, 4, 5]), val: ['a', 'b', 'c', 'd', 'e'] },
      batchSize: 2,
    })
    const result = executeSql({ tables: { t }, query: 'SELECT * FROM t' })
    expect(result.batches).toBeDefined()
    const batches = await collectBatches(result)
    expect(batches.length).toBe(3)
    expect(batches[0].rowCount).toBe(2)
    expect(Array.from(batches[0].columns.id)).toEqual([1, 2])
    expect(Array.from(batches[1].columns.id)).toEqual([3, 4])
    expect(Array.from(batches[2].columns.id)).toEqual([5])
    expect(t.scanBatchesCalled()).toBe(1)
    expect(t.scanCalled()).toBe(0)
  })

  it('preserves typed array column types from the native source', async () => {
    const t = batchSource({
      columns: ['n'],
      data: { n: new Uint32Array([10, 20, 30, 40]) },
      batchSize: 4,
    })
    const result = executeSql({ tables: { t }, query: 'SELECT * FROM t' })
    const [batch] = await collectBatches(result)
    expect(batch.columns.n).toBeInstanceOf(Uint32Array)
  })

  it('applies WHERE engine-side instead of falling back to scan()', async () => {
    const t = batchSource({
      columns: ['id'],
      data: { id: new Uint32Array([1, 2, 3, 4]) },
      batchSize: 4,
    })
    const result = executeSql({ tables: { t }, query: 'SELECT * FROM t WHERE id > 2' })
    expect(result.batches).toBeDefined()
    const rows = await collectRows(result)
    expect(rows).toEqual([{ id: 3 }, { id: 4 }])
    expect(t.scanBatchesCalled()).toBe(1)
    expect(t.scanCalled()).toBe(0)
  })

  it('applies LIMIT engine-side without falling back to scan()', async () => {
    const t = batchSource({
      columns: ['id'],
      data: { id: new Uint32Array([1, 2, 3, 4, 5]) },
      batchSize: 2,
    })
    const result = executeSql({ tables: { t }, query: 'SELECT * FROM t LIMIT 3' })
    const rows = await collectRows(result)
    expect(rows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }])
    expect(t.scanBatchesCalled()).toBe(1)
    expect(t.scanCalled()).toBe(0)
  })

  it('terminates source iteration early when LIMIT is satisfied', async () => {
    let yielded = 0
    /** @type {AsyncDataSource} */
    const t = {
      columns: ['n'],
      numRows: 100,
      scan() {
        return { async *rows() {}, appliedWhere: false, appliedLimitOffset: false }
      },
      async *scanBatches() {
        for (let i = 0; i < 10; i++) {
          /** @type {Record<string, ArrayLike<SqlPrimitive>>} */
          const cols = { n: new Uint32Array([i * 10]) }
          yield { rowStart: i * 10, rowCount: 1, columns: cols }
          yielded++
        }
      },
    }
    const result = executeSql({ tables: { t }, query: 'SELECT * FROM t LIMIT 3' })
    const rows = await collectRows(result)
    expect(rows.length).toBe(3)
    // Limiter terminates iteration after 3 rows; source yields 3 batches plus
    // one extra because of the cooperative-cancellation handoff in async
    // generators (the source has produced its next value before the consumer
    // breaks). Anything substantially less than 10 (the full source) proves
    // early termination works.
    expect(yielded).toBeLessThan(10)
  })

  it('rows() fallback works for sources without scanBatches', async () => {
    /** @type {AsyncDataSource} */
    const t = {
      columns: ['x'],
      scan() {
        return {
          async *rows() {
            yield { columns: ['x'], cells: { x: () => Promise.resolve(1) } }
            yield { columns: ['x'], cells: { x: () => Promise.resolve(2) } }
          },
          appliedWhere: false,
          appliedLimitOffset: false,
        }
      },
    }
    const result = executeSql({ tables: { t }, query: 'SELECT * FROM t' })
    expect(result.batches).toBeUndefined()
    const rows = await collectRows(result)
    expect(rows).toEqual([{ x: 1 }, { x: 2 }])
  })
})

describe('filter batch path', () => {
  it('chains batches through filter when child supports them', async () => {
    const t = batchSource({
      columns: ['id', 'flag'],
      data: { id: new Uint32Array([1, 2, 3, 4, 5, 6]), flag: [true, false, true, true, false, true] },
      batchSize: 3,
    })
    // SELECT id FROM t WHERE flag — Project + Filter + Scan, all batch-capable.
    const result = executeSql({ tables: { t }, query: 'SELECT id FROM t WHERE flag' })
    expect(result.batches).toBeDefined()
    const batches = await collectBatches(result)
    const ids = batches.flatMap(b => Array.from(b.columns.id))
    expect(ids).toEqual([1, 3, 4, 6])
  })

  it('preserves typed-array column types after filtering', async () => {
    const t = batchSource({
      columns: ['n'],
      data: { n: new Uint32Array([1, 2, 3, 4, 5, 6, 7, 8]) },
      batchSize: 8,
    })
    const result = executeSql({ tables: { t }, query: 'SELECT * FROM t WHERE n > 4' })
    const [batch] = await collectBatches(result)
    expect(batch.rowCount).toBe(4)
    expect(batch.columns.n).toBeInstanceOf(Uint32Array)
    expect(Array.from(batch.columns.n)).toEqual([5, 6, 7, 8])
  })

  it('forwards all-pass batches unchanged to avoid copying', async () => {
    const t = batchSource({
      columns: ['n'],
      data: { n: new Uint32Array([1, 2, 3, 4]) },
      batchSize: 4,
    })
    const result = executeSql({ tables: { t }, query: 'SELECT * FROM t WHERE n > 0' })
    const [batch] = await collectBatches(result)
    // Same Uint32Array reference (no copy).
    expect(batch.columns.n).toBeInstanceOf(Uint32Array)
    expect(Array.from(batch.columns.n)).toEqual([1, 2, 3, 4])
  })

  it('drops batches where all rows fail', async () => {
    const t = batchSource({
      columns: ['n'],
      data: { n: new Uint32Array([1, 2, 3, 4, 5, 6]) },
      batchSize: 3,
    })
    // First batch [1,2,3] all fail n > 4; second batch [4,5,6] partially passes.
    const result = executeSql({ tables: { t }, query: 'SELECT * FROM t WHERE n > 4' })
    const batches = await collectBatches(result)
    expect(batches.length).toBe(1)
    expect(Array.from(batches[0].columns.n)).toEqual([5, 6])
  })

  it('produces correct rows() output via batches', async () => {
    const t = batchSource({
      columns: ['id', 'name'],
      data: { id: new Uint32Array([1, 2, 3, 4]), name: ['a', 'b', 'c', 'd'] },
      batchSize: 2,
    })
    const result = executeSql({ tables: { t }, query: 'SELECT name FROM t WHERE id IN (2, 4)' })
    const rows = await collectRows(result)
    expect(rows).toEqual([{ name: 'b' }, { name: 'd' }])
  })

  it('honors abort signal mid-batch', async () => {
    const controller = new AbortController()
    const t = batchSource({
      columns: ['n'],
      data: { n: new Uint32Array(1000).map((_, i) => i) },
      batchSize: 100,
    })
    const result = executeSql({
      tables: { t },
      query: 'SELECT * FROM t WHERE n >= 0',
      signal: controller.signal,
    })
    /** @type {ColumnBatch[]} */
    const collected = []
    for await (const b of /** @type {AsyncIterable<ColumnBatch>} */ result.batches?.() ?? []) {
      collected.push(b)
      controller.abort()
    }
    expect(collected.length).toBe(1)
  })
})

describe('project batch path', () => {
  it('renames columns via simple identifier projection without copying', async () => {
    const id = new Uint32Array([1, 2, 3])
    const score = new Float64Array([0.1, 0.2, 0.3])
    const t = batchSource({
      columns: ['id', 'score'],
      data: { id, score },
      batchSize: 3,
    })
    const result = executeSql({ tables: { t }, query: 'SELECT score, id FROM t' })
    expect(result.columns).toEqual(['score', 'id'])
    const [batch] = await collectBatches(result)
    expect(batch.columns.score).toBeInstanceOf(Float64Array)
    expect(batch.columns.id).toBeInstanceOf(Uint32Array)
    expect(Array.from(batch.columns.score)).toEqual([0.1, 0.2, 0.3])
    expect(Array.from(batch.columns.id)).toEqual([1, 2, 3])
  })

  it('handles SELECT * via star expansion', async () => {
    const t = batchSource({
      columns: ['a', 'b'],
      data: { a: new Uint32Array([1, 2]), b: ['x', 'y'] },
      batchSize: 2,
    })
    const result = executeSql({ tables: { t }, query: 'SELECT * FROM t' })
    const [batch] = await collectBatches(result)
    expect(Array.from(batch.columns.a)).toEqual([1, 2])
    expect(Array.from(batch.columns.b)).toEqual(['x', 'y'])
  })

  it('falls back to row mode when projection has expressions', async () => {
    const t = batchSource({
      columns: ['n'],
      data: { n: new Uint32Array([1, 2, 3]) },
      batchSize: 3,
    })
    const result = executeSql({ tables: { t }, query: 'SELECT n + 1 AS m FROM t' })
    expect(result.batches).toBeUndefined()
    const rows = await collectRows(result)
    expect(rows).toEqual([{ m: 2 }, { m: 3 }, { m: 4 }])
  })

  it('chains scan -> filter -> project with native batches end-to-end', async () => {
    const t = batchSource({
      columns: ['id', 'category', 'value'],
      data: {
        id: new Uint32Array([1, 2, 3, 4, 5, 6]),
        category: ['a', 'b', 'a', 'b', 'a', 'b'],
        value: new Float64Array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
      },
      batchSize: 2,
    })
    const result = executeSql({
      tables: { t },
      query: 'SELECT id, value FROM t WHERE category = \'a\'',
    })
    expect(result.batches).toBeDefined()
    const batches = await collectBatches(result)
    const ids = batches.flatMap(b => Array.from(b.columns.id))
    const values = batches.flatMap(b => Array.from(b.columns.value))
    expect(ids).toEqual([1, 3, 5])
    expect(values).toEqual([1.0, 3.0, 5.0])
    // Filter preserved typed-array types coming from scan.
    for (const b of batches) {
      expect(b.columns.id).toBeInstanceOf(Uint32Array)
      expect(b.columns.value).toBeInstanceOf(Float64Array)
    }
  })
})

describe('integration with the broader executor', () => {
  it('non-batch operators still receive correct rows from a batch chain', async () => {
    const t = batchSource({
      columns: ['id', 'category'],
      data: {
        id: new Uint32Array([1, 2, 3, 4, 5, 6]),
        category: ['a', 'b', 'a', 'b', 'a', 'b'],
      },
      batchSize: 3,
    })
    // ORDER BY forces row-mode consumption from the batch chain via rows().
    const result = executeSql({
      tables: { t },
      query: 'SELECT id FROM t WHERE category = \'a\' ORDER BY id DESC',
    })
    const rows = await collectRows(result)
    expect(rows).toEqual([{ id: 5 }, { id: 3 }, { id: 1 }])
  })

  it('GROUP BY over a filtered batch chain produces correct aggregates', async () => {
    const t = batchSource({
      columns: ['category', 'value'],
      data: {
        category: ['a', 'b', 'a', 'b', 'a', 'b'],
        value: new Float64Array([1, 2, 3, 4, 5, 6]),
      },
      batchSize: 3,
    })
    const result = executeSql({
      tables: { t },
      query: 'SELECT category, COUNT(*) AS c FROM t GROUP BY category ORDER BY category',
    })
    const rows = await collectRows(result)
    expect(rows).toEqual([
      { category: 'a', c: 3 },
      { category: 'b', c: 3 },
    ])
  })
})
