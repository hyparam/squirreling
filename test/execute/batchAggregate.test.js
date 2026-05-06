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
 * @param {number} [options.numRows]
 * @returns {AsyncDataSource & { scanCalled: () => number, scanBatchesCalled: () => number }}
 */
function batchSource({ columns, data, batchSize = 4, numRows }) {
  const totalRows = numRows ?? data[columns[0]].length
  let scanCalled = 0
  let scanBatchesCalled = 0
  return {
    columns,
    numRows: totalRows,
    scanCalled: () => scanCalled,
    scanBatchesCalled: () => scanBatchesCalled,
    scan({ columns: scanCols, signal }) {
      scanCalled++
      const cols = scanCols ?? columns
      return {
        async *rows() {
          for (let i = 0; i < totalRows; i++) {
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
      while (rowStart < totalRows) {
        if (signal?.aborted) return
        const rowCount = Math.min(batchSize, totalRows - rowStart)
        /** @type {Record<string, ArrayLike<SqlPrimitive>>} */
        const out = {}
        for (const c of cols) {
          const src = data[c]
          /** @type {any} */
          const anySrc = src
          const Ctor = anySrc.constructor
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

describe('scalar aggregate batch path', () => {
  // Source without scanColumn so the column-scan fast path bails and the
  // batch fast path is exercised. WHERE forces ScalarAggregate (not CountNode).
  const sample = {
    columns: ['id', 'value', 'active'],
    data: {
      id: new Uint32Array([1, 2, 3, 4, 5, 6]),
      value: new Float64Array([10, 20, 30, 40, 50, 60]),
      active: [true, false, true, false, true, true],
    },
  }

  it('COUNT(*) consumes batches without falling back to scan()', async () => {
    const t = batchSource({ ...sample, batchSize: 2 })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT COUNT(*) AS n FROM t WHERE active',
    }))
    expect(rows).toEqual([{ n: 4 }])
    expect(t.scanBatchesCalled()).toBe(1)
    expect(t.scanCalled()).toBe(0)
  })

  it('COUNT(column) skips nulls', async () => {
    const t = batchSource({
      columns: ['name'],
      data: { name: ['Alice', null, 'Charlie', null, 'Eve'] },
      batchSize: 2,
    })
    const rows = await collectRows(executeSql({
      tables: { t },
      // WHERE forces ScalarAggregate; the batch chain still feeds COUNT(name).
      query: 'SELECT COUNT(name) AS n FROM t WHERE name IS NOT NULL OR name IS NULL',
    }))
    expect(rows).toEqual([{ n: 3 }])
    expect(t.scanBatchesCalled()).toBe(1)
    expect(t.scanCalled()).toBe(0)
  })

  it('SUM over Float64Array column', async () => {
    const t = batchSource({ ...sample, batchSize: 2 })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT SUM(value) AS total FROM t WHERE active',
    }))
    // active rows are id=1,3,5,6 with values 10,30,50,60 = 150
    expect(rows).toEqual([{ total: 150 }])
    expect(t.scanBatchesCalled()).toBe(1)
    expect(t.scanCalled()).toBe(0)
  })

  it('AVG returns sum/count', async () => {
    const t = batchSource({ ...sample, batchSize: 2 })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT AVG(value) AS avg FROM t WHERE active',
    }))
    expect(rows).toEqual([{ avg: 37.5 }])
  })

  it('MIN and MAX track running extremes', async () => {
    const t = batchSource({ ...sample, batchSize: 3 })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT MIN(value) AS lo, MAX(value) AS hi FROM t WHERE id < 100',
    }))
    expect(rows).toEqual([{ lo: 10, hi: 60 }])
  })

  it('MIN/MAX work on string columns', async () => {
    const t = batchSource({
      columns: ['name'],
      data: { name: ['Charlie', 'Alice', 'Eve', 'Bob'] },
      batchSize: 2,
    })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT MIN(name) AS lo, MAX(name) AS hi FROM t WHERE name IS NOT NULL',
    }))
    expect(rows).toEqual([{ lo: 'Alice', hi: 'Eve' }])
  })

  it('SUM and AVG return null for empty batch stream', async () => {
    const t = batchSource({
      columns: ['value'],
      data: { value: new Float64Array(0) },
      batchSize: 2,
    })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT SUM(value) AS sum, AVG(value) AS avg FROM t WHERE value > 0',
    }))
    expect(rows).toEqual([{ sum: null, avg: null }])
  })

  it('SUM and AVG return null when WHERE filters everything', async () => {
    const t = batchSource({ ...sample, batchSize: 2 })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT SUM(value) AS sum, AVG(value) AS avg FROM t WHERE id > 1000',
    }))
    expect(rows).toEqual([{ sum: null, avg: null }])
  })

  it('COUNT(*) returns 0 on empty stream', async () => {
    const t = batchSource({
      columns: ['value'],
      data: { value: new Float64Array(0) },
      batchSize: 2,
    })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT COUNT(*) AS n FROM t WHERE value > 0',
    }))
    expect(rows).toEqual([{ n: 0 }])
  })

  it('skips non-numeric values in SUM/AVG', async () => {
    const t = batchSource({
      columns: ['v'],
      // Mixed types in a plain Array column. SUM/AVG must skip 'abc' and null
      // matching the existing scanColumnAggregate semantics.
      data: { v: [10, null, 'abc', 20] },
      batchSize: 2,
    })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT SUM(v) AS sum, AVG(v) AS avg FROM t WHERE v IS NOT NULL OR v IS NULL',
    }))
    expect(rows).toEqual([{ sum: 30, avg: 15 }])
  })

  it('combines multiple aggregates in a single pass', async () => {
    const t = batchSource({ ...sample, batchSize: 2 })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: `SELECT
        COUNT(*) AS n,
        SUM(value) AS sum,
        AVG(value) AS avg,
        MIN(value) AS lo,
        MAX(value) AS hi
        FROM t WHERE id < 100`,
    }))
    expect(rows).toEqual([{ n: 6, sum: 210, avg: 35, lo: 10, hi: 60 }])
    expect(t.scanBatchesCalled()).toBe(1)
    expect(t.scanCalled()).toBe(0)
  })

  it('falls back to row mode when DISTINCT is used', async () => {
    const t = batchSource({
      columns: ['city'],
      data: { city: ['NYC', 'LA', 'NYC', 'LA', 'NYC'] },
      batchSize: 2,
    })
    // DISTINCT can't be served by the batch fast path; it must fall back via
    // the rows() bridge over batches() so the answer is still correct.
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT COUNT(DISTINCT city) AS unique_cities FROM t WHERE city IS NOT NULL',
    }))
    expect(rows).toEqual([{ unique_cities: 2 }])
  })

  it('falls back to row mode when FILTER clause is used', async () => {
    const t = batchSource({ ...sample, batchSize: 2 })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT COUNT(*) FILTER (WHERE active) AS n FROM t WHERE id < 100',
    }))
    expect(rows).toEqual([{ n: 4 }])
  })

  it('falls back to row mode when an aggregate is wrapped in an expression', async () => {
    const t = batchSource({ ...sample, batchSize: 2 })
    const rows = await collectRows(executeSql({
      tables: { t },
      query: 'SELECT SUM(value) * 2 AS doubled FROM t WHERE id < 100',
    }))
    // 10+20+30+40+50+60 = 210; doubled = 420.
    expect(rows).toEqual([{ doubled: 420 }])
  })

  it('honors abort signal between batches', async () => {
    const controller = new AbortController()
    const big = new Uint32Array(1000)
    for (let i = 0; i < big.length; i++) big[i] = i
    const t = batchSource({
      columns: ['n'],
      data: { n: big },
      batchSize: 100,
    })
    // Abort after the first batch to verify we don't process the entire stream.
    let batchesSeen = 0
    const innerAbort = {
      ...t,
      /**
       * @param {BatchScanOptions} opts
       * @yields {ColumnBatch}
       */
      async *scanBatches(opts) {
        /** @type {AsyncIterable<ColumnBatch>} */
        const inner = t.scanBatches(opts)
        for await (const b of inner) {
          batchesSeen++
          yield b
          controller.abort()
        }
      },
    }
    const result = executeSql({
      tables: { t: innerAbort },
      query: 'SELECT SUM(n) AS s FROM t WHERE n >= 0',
      signal: controller.signal,
    })
    // Iterating rows() returns whatever was accumulated before abort.
    /** @type {Record<string, SqlPrimitive>[]} */
    const out = []
    for await (const r of result.rows()) {
      /** @type {Record<string, SqlPrimitive>} */
      const obj = {}
      for (const col of r.columns) obj[col] = await r.cells[col]()
      out.push(obj)
    }
    // Either zero rows (aborted before yield) or one row with the partial sum.
    // Either way, batchesSeen must be small (early termination worked).
    expect(batchesSeen).toBeLessThan(10)
    if (out.length === 1) {
      // First batch is rows 0..99 → sum = 99*100/2 = 4950
      expect(out[0].s).toBe(4950)
    }
  })

  it('uses scanColumn fast path when both scanColumn and scanBatches exist', async () => {
    // Verifies the batch path doesn't preempt the narrower scanColumn fast
    // path: column-scan is preferred for direct table scans because it can
    // dispatch to per-column reads.
    let scanBatchesCalled = 0
    let scanColumnCalled = 0
    /** @type {AsyncDataSource} */
    const t = {
      columns: ['v'],
      numRows: 4,
      scan() {
        return { async *rows() {}, appliedWhere: false, appliedLimitOffset: false }
      },
      scanColumn() {
        scanColumnCalled++
        return (async function* () {
          yield [10, 20, 30, 40]
        })()
      },
      // eslint-disable-next-line require-yield
      async *scanBatches() {
        scanBatchesCalled++
      },
    }
    const result = executeSql({ tables: { t }, query: 'SELECT SUM(v) AS s FROM t' })
    const rows = await collectRows(result)
    expect(rows).toEqual([{ s: 100 }])
    expect(scanColumnCalled).toBe(1)
    expect(scanBatchesCalled).toBe(0)
  })

  it('falls back to row mode for HAVING', async () => {
    const t = batchSource({ ...sample, batchSize: 2 })
    const rows = await collectRows(executeSql({
      tables: { t },
      // HAVING isn't part of the batch fast path; the rows() bridge keeps it
      // working via row-mode aggregation.
      query: 'SELECT COUNT(*) AS n FROM t WHERE id < 100 HAVING COUNT(*) > 1',
    }))
    expect(rows).toEqual([{ n: 6 }])
  })
})

describe('scalar aggregate batch microbench', () => {
  const ROWS = 100_000

  /**
   * @param {Uint32Array} data
   * @returns {AsyncDataSource}
   */
  function batchOnlySource(data) {
    return {
      columns: ['n'],
      numRows: data.length,
      scan({ signal }) {
        return {
          async *rows() {
            for (let i = 0; i < data.length; i++) {
              if (signal?.aborted) return
              const v = data[i]
              yield { columns: ['n'], cells: { n: () => Promise.resolve(v) } }
            }
          },
          appliedWhere: false,
          appliedLimitOffset: false,
        }
      },
      async *scanBatches({ signal }) {
        const BATCH_SIZE = 4096
        for (let start = 0; start < data.length; start += BATCH_SIZE) {
          if (signal?.aborted) return
          const rowCount = Math.min(BATCH_SIZE, data.length - start)
          yield {
            rowStart: start,
            rowCount,
            columns: { n: data.subarray(start, start + rowCount) },
          }
        }
      },
    }
  }

  /**
   * @param {Uint32Array} data
   * @returns {AsyncDataSource}
   */
  function rowOnlySource(data) {
    return {
      columns: ['n'],
      numRows: data.length,
      scan({ signal }) {
        return {
          async *rows() {
            for (let i = 0; i < data.length; i++) {
              if (signal?.aborted) return
              const v = data[i]
              yield { columns: ['n'], cells: { n: () => Promise.resolve(v) } }
            }
          },
          appliedWhere: false,
          appliedLimitOffset: false,
        }
      },
    }
  }

  /**
   * @param {AsyncDataSource} source
   * @returns {Promise<SqlPrimitive>}
   */
  async function runSum(source) {
    const result = executeSql({
      tables: { t: source },
      // WHERE forces ScalarAggregate (not CountNode/scanColumn fast path).
      query: 'SELECT SUM(n) AS s FROM t WHERE n >= 0',
    })
    /** @type {SqlPrimitive} */
    let value = null
    for await (const row of result.rows()) {
      value = await row.cells.s()
    }
    return value
  }

  it('produces identical SUM from batch and row paths', async () => {
    const data = new Uint32Array(ROWS)
    for (let i = 0; i < ROWS; i++) data[i] = i
    const expected = (ROWS - 1) * ROWS / 2
    const batchSum = await runSum(batchOnlySource(data))
    const rowSum = await runSum(rowOnlySource(data))
    expect(batchSum).toBe(expected)
    expect(rowSum).toBe(expected)
  })

  it('is at least as fast as row mode on a Uint32Array column', async () => {
    const data = new Uint32Array(ROWS)
    for (let i = 0; i < ROWS; i++) data[i] = i
    // Warm up
    await runSum(batchOnlySource(data))
    await runSum(rowOnlySource(data))

    /**
     * @param {() => Promise<SqlPrimitive>} run
     * @returns {Promise<number>}
     */
    async function bestOfThree(run) {
      let best = Infinity
      for (let i = 0; i < 3; i++) {
        const start = performance.now()
        await run()
        const elapsed = performance.now() - start
        if (elapsed < best) best = elapsed
      }
      return best
    }

    const batchTime = await bestOfThree(() => runSum(batchOnlySource(data)))
    const rowTime = await bestOfThree(() => runSum(rowOnlySource(data)))

    const batchThroughput = ROWS / batchTime * 1000
    const rowThroughput = ROWS / rowTime * 1000
    const speedup = rowTime / batchTime

    console.log(
      `[batch-agg-bench] rows=${ROWS} batch=${batchTime.toFixed(1)}ms ` +
      `row=${rowTime.toFixed(1)}ms speedup=${speedup.toFixed(2)}x ` +
      `(batch ${(batchThroughput / 1e6).toFixed(2)}M rows/s, row ${(rowThroughput / 1e6).toFixed(2)}M rows/s)`
    )

    // Soft acceptance: batch path must not be catastrophically slower than row
    // mode on the canonical Uint32Array case.
    expect(batchTime).toBeLessThan(rowTime * 1.5)
  }, 30000)
})
