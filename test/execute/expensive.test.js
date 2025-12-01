import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'
import { cachedDataSource, memorySource } from '../../src/backend/dataSource.js'

/**
 * @import { AsyncRow, AsyncDataSource, SqlPrimitive } from '../../src/types.js'
 */

const data = [
  { id: 1, name: 'Alice', llm: '4.5' },
  { id: 2, name: 'Bob', llm: '4.5' },
  { id: 3, name: 'Charlie', llm: '3.5' },
  { id: 4, name: 'Diana', llm: null },
  { id: 5, name: 'Eve', llm: '4.0' },
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
      .resolves.toBe(2)
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
    tables: { data: cachedSource },
    query,
  }))
  return countingSource.getExpensiveCallCount()
}

/**
 * Creates a data source that wraps a memory source and counts
 * how many times getCell is called on "expensive" columns.
 * @param {Record<string, any>[]} data
 * @param {string[]} expensiveColumns
 * @returns {AsyncDataSource & { getExpensiveCallCount: () => number }}
 */
function countingDataSource(data, expensiveColumns) {
  const source = memorySource(data)
  let expensiveCallCount = 0

  return {
    /**
     * @returns {AsyncGenerator<AsyncRow>}
     */
    async *getRows() {
      for await (const row of source.getRows()) {
        /** @type {AsyncRow} */
        const out = {}
        for (const [key, cell] of Object.entries(row)) {
          if (expensiveColumns.includes(key)) {
            // Wrap the cell to count accesses
            out[key] = () => {
              expensiveCallCount++
              return cell()
            }
          } else {
            out[key] = cell
          }
        }
        yield out
      }
    },
    getExpensiveCallCount() {
      return expensiveCallCount
    },
  }
}
