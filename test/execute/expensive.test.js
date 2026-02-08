import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'
import { cachedDataSource, memorySource } from '../../src/backend/dataSource.js'

/**
 * @import { AsyncDataSource, SqlPrimitive, AsyncCells, ScanOptions } from '../../src/types.js'
 */

const data = [
  { id: 1, name: 'Alice', llm: '4.5' },
  { id: 2, name: 'Bob', llm: '4.5' },
  { id: 3, name: 'Charlie', llm: '3.5' },
  { id: 4, name: 'Diana', llm: null },
  { id: 5, name: 'Eve', llm: '4.0' },
]

const other = [
  { id: 1, value: '4.5' },
  { id: 2, value: '3.5' },
  { id: 3, value: '4.0' },
]

describe('expensive cell access', () => {
  it('should make no expensive calls when not accessing expensive columns', async () => {
    await expect(countExpensiveCalls('SELECT id, name FROM data')).resolves.toBe(0)
  })

  it('should make no expensive calls counting rows', async () => {
    await expect(countExpensiveCalls('SELECT COUNT(*) FROM data')).resolves.toBe(0)
    await expect(countExpensiveCalls('SELECT COUNT(id) FROM data')).resolves.toBe(0)
  })

  it('should make no expensive calls aggregating rows', async () => {
    await expect(countExpensiveCalls('SELECT SUM(id) FROM data')).resolves.toBe(0)
    await expect(countExpensiveCalls('SELECT AVG(id) FROM data')).resolves.toBe(0)
    await expect(countExpensiveCalls('SELECT MIN(id) FROM data')).resolves.toBe(0)
    await expect(countExpensiveCalls('SELECT MAX(id) FROM data')).resolves.toBe(0)
  })

  it('should make expensive calls when selecting expensive columns', async () => {
    await expect(countExpensiveCalls('SELECT * FROM data')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT llm FROM data')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT id, llm FROM data')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT MIN(llm) FROM data')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT MAX(llm) FROM data')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT AVG(llm) FROM data')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT COUNT(llm) FROM data')).resolves.toBe(5)
  })

  it('should make expensive calls when filtering on expensive columns', async () => {
    await expect(countExpensiveCalls('SELECT * FROM data WHERE llm = \'4.5\'')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE llm > \'4.0\'')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE llm > \'4.0\' OR llm < \'4.0\'')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE llm BETWEEN \'3.0\' AND \'4.0\'')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE llm IN (\'4.5\', \'3.5\')')).resolves.toBe(5)
  })

  it('should minimize expensive calls when filtering on non-expensive columns', async () => {
    await expect(countExpensiveCalls('SELECT * FROM data WHERE name = \'Bob\'')).resolves.toBe(1)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE name LIKE \'Ch%\'')).resolves.toBe(1)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE name IN (\'Alice\', \'Eve\')')).resolves.toBe(2)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE name IS NULL')).resolves.toBe(0)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE name IS NOT NULL')).resolves.toBe(5)
  })

  it('should minimize expensive calls when filtering on multiple non-expensive columns', async () => {
    await expect(countExpensiveCalls('SELECT * FROM data WHERE name = \'Bob\' OR name = \'Charlie\'')).resolves.toBe(2)
  })

  it('should minimize expensive calls when limited', async () => {
    await expect(countExpensiveCalls('SELECT * FROM data LIMIT 2')).resolves.toBe(2)
  })

  it('should minimize expensive calls when offset', async () => {
    await expect(countExpensiveCalls('SELECT * FROM data OFFSET 3')).resolves.toBe(2)
  })

  it('should minimize expensive calls when limit + offset', async () => {
    await expect(countExpensiveCalls('SELECT * FROM data LIMIT 1 OFFSET 3')).resolves.toBe(1)
  })

  it('should minimize expensive calls when limit + order by', async () => {
    await expect(countExpensiveCalls('SELECT * FROM data ORDER BY name DESC LIMIT 1'))
      .resolves.toBe(1)
  })

  it('should minimize expensive calls when sorting by multiple columns', async () => {
    // ORDER BY cheap column, then expensive column
    // Should only evaluate expensive column for rows that tie on cheap column
    // With 5 unique names, no ties occur, so llm only evaluated for LIMIT rows
    await expect(countExpensiveCalls('SELECT * FROM data ORDER BY name, llm LIMIT 1'))
      .resolves.toBe(1)
    await expect(countExpensiveCalls('SELECT * FROM data ORDER BY name, llm LIMIT 2'))
      .resolves.toBe(2)
    // Without LIMIT, all rows need llm for final materialization
    await expect(countExpensiveCalls('SELECT * FROM data ORDER BY name, llm'))
      .resolves.toBe(5)
  })

  it('should minimize expensive calls when filtering by non-expensive columns with limit', async () => {
    await expect(countExpensiveCalls('SELECT * FROM data WHERE name > \'B\' LIMIT 2')).resolves.toBe(2)
  })

  it('should minimize expensive calls when filtering by cheap + expensive columns', async () => {
    await expect(countExpensiveCalls('SELECT * FROM data WHERE name = \'Eve\' AND llm = \'4.0\''))
      .resolves.toBe(1)
  })

  it('should make expensive calls when double selecting expensive column', async () => {
    await expect(countExpensiveCalls('SELECT llm, llm FROM data')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT llm as llm1, llm as llm2 FROM data')).resolves.toBe(5)
  })

  it('should minimize expensive calls when using DISTINCT', async () => {
    await expect(countExpensiveCalls('SELECT DISTINCT name FROM data')).resolves.toBe(0)
    await expect(countExpensiveCalls('SELECT DISTINCT llm FROM data')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT DISTINCT * FROM data')).resolves.toBe(5)
  })

  it('should minimize expensive calls in a subquery', async () => {
    // would be 5 if we materialized the subquery eagerly
    // would be 2 without late materialization
    await expect(countExpensiveCalls('SELECT name FROM (SELECT * FROM data) AS t LIMIT 2'))
      .resolves.toBe(0)
  })

  it('should minimize expensive calls in a join with LIMIT', async () => {
    // would be 5 if we buffered all join results before applying LIMIT
    // with streaming joins, only 1 expensive call should be made
    await expect(countExpensiveCalls('SELECT * FROM data JOIN other ON data.llm = other.value LIMIT 1'))
      .resolves.toBe(1)
  })

  it('should sort only once for ORDER BY without GROUP BY', async () => {
    // This test detects the double-sorting bug where ORDER BY without GROUP BY
    // would sort twice: once before projection and once after.
    const countingSource = countingDataSource(data, ['name']) // no cache
    await collect(executeSql({
      tables: { data: countingSource, other },
      query: 'SELECT * FROM data ORDER BY name',
    }))

    // With double-sorting bug: 15 accesses (2 sorts, 1 materialization)
    // Without bug: 10 accesses (1 sort, 1 materialization)
    expect(countingSource.getExpensiveCallCount()).toBe(10)
  })
})

/**
 * Executes a query against a counting data source and
 * return the number of expensive calls made.
 *
 * @param {string} query
 * @returns {Promise<number>}
 */
async function countExpensiveCalls(query) {
  const countingSource = countingDataSource(data, ['llm'])
  const cachedSource = cachedDataSource(countingSource)
  await collect(executeSql({
    tables: { data: cachedSource, other },
    query,
  }))
  return countingSource.getExpensiveCallCount()
}

/**
 * Creates a data source that wraps a memory source and counts
 * how many times getCell is called on "expensive" columns.
 * @param {Record<string, SqlPrimitive>[]} data
 * @param {string[]} expensiveColumns
 * @returns {AsyncDataSource & { getExpensiveCallCount: () => number }}
 */
function countingDataSource(data, expensiveColumns) {
  const source = memorySource(data)
  let expensiveCallCount = 0

  return {
    /**
     * @param {ScanOptions} options
     * @returns {import('../../src/types.js').ScanResults}
     */
    scan(options) {
      const { rows, appliedWhere, appliedLimitOffset } = source.scan(options)
      return {
        rows: (async function* () {
          for await (const row of rows) {
            if (options.signal?.aborted) break
            /** @type {AsyncCells} */
            const cells = {}
            for (const key of row.columns) {
              const cell = row.cells[key]
              if (expensiveColumns.includes(key)) {
                // Wrap the cell to count accesses
                cells[key] = () => {
                  expensiveCallCount++
                  return cell()
                }
              } else {
                cells[key] = cell
              }
            }
            yield { columns: row.columns, cells }
          }
        })(),
        appliedWhere,
        appliedLimitOffset,
      }
    },
    getExpensiveCallCount() {
      return expensiveCallCount
    },
  }
}
