import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * @import { AsyncCells, AsyncDataSource } from '../../src/types.js'
 */

/**
 * A data source whose cells each take `cellDelayMs` to resolve (real
 * setTimeout). Lets us measure whether the executor evaluates per-row cell
 * accesses concurrently or sequentially.
 *
 * @param {Record<string, number>[]} rows
 * @param {string[]} columns
 * @param {number} cellDelayMs
 * @returns {AsyncDataSource}
 */
function slowSource(rows, columns, cellDelayMs) {
  return {
    numRows: rows.length,
    columns,
    scan() {
      return {
        appliedWhere: false,
        appliedLimitOffset: false,
        async *rows() {
          for (const obj of rows) {
            /** @type {AsyncCells} */
            const cells = {}
            for (const col of columns) {
              cells[col] = () => new Promise(resolve => setTimeout(() => resolve(obj[col]), cellDelayMs))
            }
            yield { columns, cells }
          }
        },
      }
    },
  }
}

describe('chunked parallelism', () => {
  it('GROUP BY key build evaluates cells in parallel within a chunk', async () => {
    // 200 rows × 20ms cell delay. Sequential = 200×20 = 4000ms.
    // Chunked (chunk size 4000, one chunk here) = ~20ms + overhead.
    const data = []
    for (let i = 0; i < 200; i++) data.push({ id: i, bucket: i % 10 })
    const source = slowSource(data, ['id', 'bucket'], 20)

    const start = performance.now()
    const result = await collect(executeSql({
      tables: { s: source },
      query: 'SELECT bucket, COUNT(*) AS n FROM s GROUP BY bucket',
    }))
    const ms = performance.now() - start

    expect(result).toHaveLength(10)
    expect(ms).toBeLessThan(500)
  }, 10_000)

  it('window partition + order value builds evaluate cells in parallel within chunks', async () => {
    // 200 rows × 10ms cell delay, 10 partitions of 20 rows each. ROW_NUMBER
    // is used so the LAG/LEAD per-row evaluateExpr (not chunked) doesn't
    // dominate. Sequential partition-key build alone = 200×10 = 2000ms.
    // Chunked: ~10ms for partition keys + ~10ms × 10 partitions for order
    // values = ~120ms.
    const data = []
    for (let i = 0; i < 200; i++) data.push({ id: i, bucket: i % 10 })
    const source = slowSource(data, ['id', 'bucket'], 10)

    const start = performance.now()
    const result = await collect(executeSql({
      tables: { s: source },
      query: 'SELECT id, ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY id) AS rn FROM s',
    }))
    const ms = performance.now() - start

    expect(result).toHaveLength(200)
    expect(ms).toBeLessThan(500)
  }, 10_000)
})
