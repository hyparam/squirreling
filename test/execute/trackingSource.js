import { memorySource } from '../../src/backend/dataSource.js'

/**
 * @import { AsyncDataSource, ScanOptions, ScanResults, SqlPrimitive } from '../../src/types.js'
 */

/**
 * Creates a data source that tracks how many rows were scanned.
 * The source respects the abort signal.
 *
 * @param {Record<string, SqlPrimitive>[]} data
 * @returns {{ source: AsyncDataSource, getScanCount: () => number, getRowCount: () => number, getActiveCount: () => number, getPeakCount: () => number }}
 */
export function trackingSource(data) {
  const inner = memorySource({ data })
  let scanCount = 0
  let rowCount = 0
  let activeCount = 0
  let peakCount = 0

  return {
    source: {
      ...inner,
      /**
       * @param {ScanOptions} options
       * @returns {ScanResults}
       */
      scan(options) {
        scanCount++
        const { rows, appliedWhere, appliedLimitOffset } = inner.scan(options)
        return {
          async *rows() {
            activeCount++
            peakCount = Math.max(peakCount, activeCount)
            try {
              for await (const row of rows()) {
                if (options.signal?.aborted) break
                rowCount++
                yield row
              }
            } finally {
              activeCount--
            }
          },
          appliedWhere,
          appliedLimitOffset,
        }
      },
    },
    getScanCount: () => scanCount,
    getRowCount: () => rowCount,
    getActiveCount: () => activeCount,
    getPeakCount: () => peakCount,
  }
}
