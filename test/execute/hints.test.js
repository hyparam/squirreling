import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * @import { AsyncDataSource, QueryHints } from '../../src/types.js'
 */

describe('query hints', () => {
  it('should not pass * as a column hint for COUNT(*)', async () => {
    const hints = await captureHints('SELECT COUNT(*) FROM data')
    expect(hints.columns).not.toContain('*')
    expect(hints.columns).toEqual([])
  })

  it('should pass column hints for COUNT(column)', async () => {
    const hints = await captureHints('SELECT COUNT(name) FROM data')
    expect(hints.columns).toEqual(['name'])
  })

  it('should pass column hints for selected columns', async () => {
    const hints = await captureHints('SELECT id, name FROM data')
    expect(hints.columns).toEqual(['id', 'name'])
  })

  it('should pass column hints for selected expression', async () => {
    const hints = await captureHints('SELECT a + b FROM data')
    expect(hints.columns).toEqual(['a', 'b'])
  })

  it('should pass column hints from WHERE clause', async () => {
    const hints = await captureHints('SELECT id FROM data WHERE name = \'Alice\'')
    expect(hints.columns).toEqual(['id', 'name'])
  })

  it('should return undefined columns for SELECT *', async () => {
    const hints = await captureHints('SELECT * FROM data')
    expect(hints.columns).toBeUndefined()
  })

  it('should return undefined columns for * with other columns', async () => {
    const hints = await captureHints('SELECT *, id FROM data')
    expect(hints.columns).toBeUndefined()
  })

  it('should pass limit hint', async () => {
    const hints = await captureHints('SELECT * FROM data LIMIT 10')
    expect(hints.limit).toBe(10)
  })

  it('should pass offset hint', async () => {
    const hints = await captureHints('SELECT * FROM data OFFSET 5')
    expect(hints.offset).toBe(5)
  })

  it('should pass where hint', async () => {
    const hints = await captureHints('SELECT * FROM data WHERE id = 1')
    expect(hints.where).toMatchObject({ type: 'binary', op: '=' })
  })
})

/**
 * Executes a query and captures the hints passed to the data source.
 *
 * @param {string} query
 * @returns {Promise<QueryHints>}
 */
async function captureHints(query) {
  /** @type {QueryHints} */
  let capturedHints = {}
  /** @type {AsyncDataSource} */
  const capturingSource = {
    async *scan(options) {
      capturedHints = options.hints ?? {}
      if (false) yield
    },
  }
  await collect(executeSql({
    tables: { data: capturingSource },
    query,
  }))
  return capturedHints
}
