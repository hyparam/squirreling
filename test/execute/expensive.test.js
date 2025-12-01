import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'
import { memorySource } from '../../src/backend/dataSource.js'

/**
 * @import { AsyncRow, AsyncDataSource } from '../../src/types.js'
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
  })

  it('should make expensive calls when filtering on expensive columns', async () => {
    // TODO: should all be 5 (would require short-circuiting + caching)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE llm = \'4.5\'')).resolves.toBe(7)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE llm > \'4.0\'')).resolves.toBe(7)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE llm > \'4.0\' OR llm < \'4.0\'')).resolves.toBe(12)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE llm BETWEEN \'3.0\' AND \'4.0\'')).resolves.toBe(7)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE llm IN (\'4.5\', \'3.5\')')).resolves.toBe(8)
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
    // TODO: should be 1 (would require knowing that name is cheaper than llm)
    await expect(countExpensiveCalls('SELECT * FROM data WHERE name = \'Eve\' AND llm = \'4.0\''))
      .resolves.toBe(2)
  })

  it('should make expensive calls when double selecting expensive column', async () => {
    // TODO: should all be 5 (would require caching)
    await expect(countExpensiveCalls('SELECT llm, llm FROM data')).resolves.toBe(10)
    await expect(countExpensiveCalls('SELECT llm as llm1, llm as llm2 FROM data')).resolves.toBe(10)
  })

  it('should minimize expensive calls when using DISTINCT', async () => {
    await expect(countExpensiveCalls('SELECT DISTINCT name FROM data')).resolves.toBe(0)
    await expect(countExpensiveCalls('SELECT DISTINCT llm FROM data')).resolves.toBe(5)
    await expect(countExpensiveCalls('SELECT DISTINCT * FROM data')).resolves.toBe(5)
  })

  it('should minimize expensive calls in a subquery', async () => {
    // would be 5 if we materialized the subquery eagerly
    // TODO: should be 0 (with lazy materialization)
    await expect(countExpensiveCalls('SELECT name FROM (SELECT * FROM data) AS t LIMIT 2')).resolves.toBe(2)
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
  const countingSource = createCountingDataSource(data, ['llm'])
  await collect(executeSql({
    tables: { data: countingSource },
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
function createCountingDataSource(data, expensiveColumns) {
  const source = memorySource(data)
  let expensiveCallCount = 0

  return {
    /**
     * @returns {AsyncGenerator<AsyncRow>}
     */
    async *getRows() {
      for await (const row of source.getRows()) {
        yield {
          /**
           * @param {string} name
           * @returns {any}
           */
          getCell(name) {
            if (expensiveColumns.includes(name)) {
              expensiveCallCount++
            }
            return row.getCell(name)
          },
          getKeys() {
            return row.getKeys()
          },
        }
      }
    },
    getExpensiveCallCount() {
      return expensiveCallCount
    },
  }
}
