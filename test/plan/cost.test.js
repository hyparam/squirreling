import { describe, expect, it } from 'vitest'
import { estimateCost } from '../../src/plan/cost.js'
import { parseSql } from '../../src/parse/parse.js'

/**
 * @import { AsyncDataSource, DataSourceStatistics } from '../../src/types.js'
 */

/**
 * @param {DataSourceStatistics} [statistics]
 * @returns {AsyncDataSource}
 */
function mockSource(statistics) {
  return {
    scan() {
      return {
        rows: (async function* () {})(),
        appliedWhere: false,
        appliedLimitOffset: false,
      }
    },
    statistics,
  }
}

// Running example: a docs table with a small id column and a large text column
const docs = mockSource({ rowCount: 100, columnWeights: { id: 4, text: 1000 } })
const tags = mockSource({ rowCount: 500, columnWeights: { doc_id: 4, tag: 50 } })

describe('estimateCost', () => {
  it('should return undefined for table with no statistics', () => {
    const tables = { docs: mockSource() }
    expect(estimateCost({ query: 'SELECT * FROM docs', tables })).toBeUndefined()
  })

  it('should return undefined for table with no columnWeights', () => {
    const tables = { docs: mockSource({ rowCount: 100 }) }
    expect(estimateCost({ query: 'SELECT * FROM docs', tables })).toBeUndefined()
  })

  it('should compute cost for SELECT *', () => {
    const tables = { docs }
    // 100 rows * (4 + 1000) = 100400
    expect(estimateCost({ query: 'SELECT * FROM docs', tables })).toBe(100400)
  })

  it('should compute cost for specific columns', () => {
    const tables = { docs }
    // Selecting only id: 100 * 4 = 400
    expect(estimateCost({ query: 'SELECT id FROM docs', tables })).toBe(400)
  })

  it('should include WHERE columns in cost', () => {
    const tables = { docs }
    // Accessing id (SELECT) + text (WHERE): 100 * (4 + 1000) = 100400
    expect(estimateCost({ query: 'SELECT id FROM docs WHERE text = \'hello\'', tables })).toBe(100400)
  })

  it('should not increase cost for WHERE on already-selected column', () => {
    const tables = { docs }
    expect(estimateCost({ query: 'SELECT * FROM docs WHERE id = 5', tables })).toBe(100400)
  })

  it('should reduce cost for LIMIT', () => {
    const tables = { docs }
    // 5 rows * (4 + 1000) = 5020
    expect(estimateCost({ query: 'SELECT * FROM docs LIMIT 5', tables })).toBe(5020)
  })

  it('should reduce cost for LIMIT with OFFSET', () => {
    const tables = { docs }
    // (5 + 10) rows * (4 + 1000) = 15060
    expect(estimateCost({ query: 'SELECT * FROM docs LIMIT 5 OFFSET 10', tables })).toBe(15060)
  })

  it('should not reduce cost for LIMIT with ORDER BY', () => {
    const tables = { docs }
    // ORDER BY requires full scan: 100 * 4 = 400
    expect(estimateCost({ query: 'SELECT id FROM docs ORDER BY id LIMIT 5', tables })).toBe(400)
  })

  it('should not reduce cost for LIMIT with WHERE', () => {
    const tables = { docs }
    // Full scan needed: 100 * (4 + 1000) = 100400
    // TODO: 100 rows * (4) + 1 row * (1000) = 1400
    expect(estimateCost({ query: 'SELECT text FROM docs WHERE id = 5 LIMIT 1', tables })).toBe(100400)
  })

  it('should sum costs for JOIN', () => {
    const tables = { docs, tags }
    // docs: 100 * (4 + 1000) = 100400
    // tags: 500 * (4 + 50) = 27000
    const cost = estimateCost({
      query: 'SELECT * FROM docs JOIN tags ON docs.id = tags.doc_id',
      tables,
    })
    expect(cost).toBe(127400)
  })

  it('should return undefined for JOIN when one side lacks statistics', () => {
    const tables = {
      docs,
      tags: mockSource(),
    }
    const cost = estimateCost({
      query: 'SELECT * FROM docs JOIN tags ON docs.id = tags.doc_id',
      tables,
    })
    expect(cost).toBeUndefined()
  })

  it('COUNT(*) is free', () => {
    const tables = { docs }
    expect(estimateCost({ query: 'SELECT COUNT(*) FROM docs', tables })).toBe(0)
  })

  it('should handle aggregate with specific column', () => {
    const tables = { docs }
    // SUM(id): 100 * 4 = 400
    expect(estimateCost({ query: 'SELECT SUM(id) FROM docs', tables })).toBe(400)
  })

  it('should handle GROUP BY columns', () => {
    const tables = { docs }
    // id (GROUP BY) + text (MAX): 100 * (4 + 1000) = 100400
    expect(estimateCost({
      query: 'SELECT id, MAX(text) FROM docs GROUP BY id',
      tables,
    })).toBe(100400)
  })

  it('should handle subquery in FROM', () => {
    const tables = { docs }
    // Inner query accesses only id: 100 * 4 = 400
    const cost = estimateCost({
      query: 'SELECT * FROM (SELECT id FROM docs) AS d',
      tables,
    })
    expect(cost).toBe(400)
  })

  it('should accept parsed AST as query', () => {
    const tables = { docs }
    const ast = parseSql({ query: 'SELECT id FROM docs' })
    // 100 * 4 = 400
    expect(estimateCost({ query: ast, tables })).toBe(400)
  })
})
