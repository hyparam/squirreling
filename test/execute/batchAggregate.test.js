import { describe, expect, it, vi } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * @import { AsyncDataSource, ScanColumnResults } from '../../src/types.js'
 */

describe('private batch aggregate execution', () => {
  it('groups computed values from the existing column scan API', async () => {
    const source = columnSource([1, 2, 3, 4])

    const results = executeSql({
      tables: { data: source },
      query: `SELECT id % 2 AS bucket,
        COUNT(*) AS parts,
        COUNT(DISTINCT id % 3) AS distinct_values,
        SUM(id * 10) AS total,
        COUNT(*) FILTER (WHERE id > 2) AS filtered
        FROM data GROUP BY id % 2 ORDER BY bucket`,
    })

    expect(await collect(results)).toEqual([
      { bucket: 0, parts: 2, distinct_values: 2, total: 60, filtered: 1 },
      { bucket: 1, parts: 2, distinct_values: 2, total: 40, filtered: 1 },
    ])
    expect(source.scan).not.toHaveBeenCalled()
  })

  it('retains row semantics for filtered non-star aggregates', async () => {
    const source = columnSource([1, 2, 3, 4])

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT SUM(id * 10) FILTER (WHERE id > 2) AS total FROM data',
    }))).resolves.toEqual([{ total: 70 }])
  })

  it('aggregates CASE, compound predicates, and NULLIF from private batches', async () => {
    const source = columnSource([1, 2, 3, 4])

    await expect(collect(executeSql({
      tables: { data: source },
      query: `SELECT
        SUM(CASE WHEN id > 1 AND id < 4 THEN id ELSE 0 END) AS middle_total,
        SUM(NULLIF(id, 2)) AS without_two
        FROM data`,
    }))).resolves.toEqual([{ middle_total: 5, without_two: 8 }])
    expect(source.scan).not.toHaveBeenCalled()
  })
})

/**
 * @param {import('../../src/types.js').SqlPrimitive[]} values
 * @returns {AsyncDataSource}
 */
function columnSource(values) {
  return {
    columns: ['id'],
    scan: vi.fn(function scan() {
      throw new Error('row scan should not be called')
    }),
    scanColumn() {
      /** @type {ScanColumnResults} */
      const results = {
        appliedWhere: false,
        appliedLimitOffset: false,
        async *chunks() { yield values },
      }
      return results
    },
  }
}
