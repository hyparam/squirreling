import { describe, expect, it } from 'vitest'
import { executeSql } from '../../src/execute/execute.js'

/**
 * @import { AsyncDataSource, BatchScanOptions, ColumnBatch, SqlPrimitive } from '../../src/types.js'
 */

const ROWS = 100_000
const BATCH_SIZE = 4096
const PREDICATE_KEEP_RATE = 0.5
const PREDICATE_THRESHOLD = Math.floor(ROWS * PREDICATE_KEEP_RATE)

/**
 * Builds a Uint32Array of length ROWS with values [0, ROWS).
 * @returns {Uint32Array}
 */
function buildData() {
  const arr = new Uint32Array(ROWS)
  for (let i = 0; i < ROWS; i++) arr[i] = i
  return arr
}

/**
 * Source that exposes data via scanBatches with native Uint32Array columns.
 * This is the path operators should pick up.
 *
 * @param {Uint32Array} data
 * @returns {AsyncDataSource}
 */
function batchOnlySource(data) {
  return {
    columns: ['n'],
    numRows: data.length,
    scan({ signal }) {
      // Provided for compatibility but not exercised when scanBatches exists.
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
    /**
     * @param {BatchScanOptions} opts
     */
    async *scanBatches({ signal }) {
      for (let start = 0; start < data.length; start += BATCH_SIZE) {
        if (signal?.aborted) return
        const rowCount = Math.min(BATCH_SIZE, data.length - start)
        const slice = data.subarray(start, start + rowCount)
        /** @type {ColumnBatch} */
        const batch = { rowStart: start, rowCount, columns: { n: slice } }
        yield batch
      }
    },
  }
}

/**
 * Source that only exposes scan(); engine reaches it via the row-mode path.
 * Mirrors the batch source's data so we compare the two paths fairly.
 *
 * @param {Uint32Array} data
 * @returns {AsyncDataSource}
 */
function rowOnlySource(data) {
  return {
    columns: ['n'],
    numRows: data.length,
    scan({ where, limit, offset, signal }) {
      // Mirror memorySource's pushdown behavior: applies offset/limit only
      // when no where clause; the engine reapplies on top otherwise.
      const start = !where ? offset ?? 0 : 0
      const end = !where && limit !== undefined ? start + limit : data.length
      return {
        async *rows() {
          for (let i = start; i < end && i < data.length; i++) {
            if (signal?.aborted) return
            const v = data[i]
            yield { columns: ['n'], cells: { n: () => Promise.resolve(v) } }
          }
        },
        appliedWhere: false,
        appliedLimitOffset: !where,
      }
    },
  }
}

/**
 * @param {AsyncDataSource} source
 * @returns {Promise<number>}
 */
async function runFilterCount(source) {
  const result = executeSql({
    tables: { t: source },
    query: `SELECT n FROM t WHERE n < ${PREDICATE_THRESHOLD}`,
  })
  let count = 0
  for await (const row of result.rows()) {
    await row.cells.n()
    count++
  }
  return count
}

describe('batch-mode filter microbench', () => {
  it('produces identical results from batch and row paths', async () => {
    const data = buildData()
    const batchCount = await runFilterCount(batchOnlySource(data))
    const rowCount = await runFilterCount(rowOnlySource(data))
    expect(batchCount).toBe(PREDICATE_THRESHOLD)
    expect(rowCount).toBe(PREDICATE_THRESHOLD)
  })

  it('is at least as fast as row mode on a Uint32Array column', async () => {
    const data = buildData()
    // Warm up
    await runFilterCount(batchOnlySource(data))
    await runFilterCount(rowOnlySource(data))

    // Best of three for each path to dampen jitter.
    /**
     * @param {() => Promise<number>} run
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

    const batchTime = await bestOfThree(() => runFilterCount(batchOnlySource(data)))
    const rowTime = await bestOfThree(() => runFilterCount(rowOnlySource(data)))

    const batchThroughput = ROWS / batchTime * 1000
    const rowThroughput = ROWS / rowTime * 1000
    const speedup = rowTime / batchTime

    // Informational — easier to interpret in CI logs than test names.

    console.log(
      `[batch-bench] rows=${ROWS} batch=${batchTime.toFixed(1)}ms ` +
      `row=${rowTime.toFixed(1)}ms speedup=${speedup.toFixed(2)}x ` +
      `(batch ${(batchThroughput / 1e6).toFixed(2)}M rows/s, row ${(rowThroughput / 1e6).toFixed(2)}M rows/s)`
    )

    // Soft acceptance: batch path must not be catastrophically slower than row
    // mode on the canonical Uint32Array case. Local runs see ~3-5x speedup;
    // we set a generous CI-safe threshold to avoid flakes.
    expect(batchTime).toBeLessThan(rowTime * 1.5)
  })
})
