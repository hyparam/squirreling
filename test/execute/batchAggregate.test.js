import { describe, expect, it, vi } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * @import { AsyncBatch, AsyncDataSource, PrepareScan, ReadColumn, RelationSchema, ScanColumnResults } from '../../src/types.js'
 */

/** @type {RelationSchema} */
const schema = {
  fields: [
    { id: 1, name: 'provider', dataType: { type: 'string' }, nullable: false },
    { id: 2, name: 'session_id', dataType: { type: 'string' }, nullable: false },
    { id: 3, name: 'attributes', dataType: { type: 'unknown' }, nullable: true },
  ],
}

describe('batch aggregate execution', () => {
  it('groups production token expressions without using the row adapter', async () => {
    /** @type {ReadColumn} */
    function readAttributes({ selection }) {
      expect(selection).toEqual({ type: 'all', length: 4 })
      return {
        type: 'values',
        values: [
          { usage: { input_tokens: 10 } },
          { usage: { input_tokens: 7 } },
          { usage: { input_tokens: 20 } },
          { usage: {} },
        ],
        length: 4,
      }
    }
    const attributes = vi.fn(readAttributes)
    const batch = loadedBatch(attributes)
    /** @type {PrepareScan} */
    function prepareScan() {
      return {
        schema,
        residual: {},
        properties: { exactRows: 4, maxRows: 4 },
        async *batches() { yield batch },
      }
    }
    /** @type {AsyncDataSource} */
    const source = {
      columns: schema.fields.map(function fieldName(field) { return field.name }),
      schema,
      prepareScan,
      scan: vi.fn(function scan() { throw new Error('legacy row scan should not be called') }),
    }

    const results = executeSql({
      tables: { messages: source },
      query: `SELECT provider,
        COUNT(*) AS parts,
        COUNT(DISTINCT session_id) AS sessions,
        COALESCE(SUM(CAST(JSON_EXTRACT(attributes, '$.usage.input_tokens') AS BIGINT)), 0) AS tokens
        FROM messages GROUP BY provider ORDER BY provider`,
    })

    expect(await collect(results)).toEqual([
      { provider: 'claude', parts: 2, sessions: 2, tokens: 30 },
      { provider: 'codex', parts: 2, sessions: 1, tokens: 7 },
    ])
    expect(source.scan).not.toHaveBeenCalled()
    expect(attributes).toHaveBeenCalledTimes(1)
  })

  it('aggregates CASE, compound predicates, and NULLIF from native batches', async () => {
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

  it('preserves struct-field precedence over a bare column', async () => {
    const source = columnSource([{ x: 2 }, { x: 3 }])

    await expect(collect(executeSql({
      tables: { data: source },
      query: 'SELECT SUM(obj.x) AS total FROM (SELECT id AS obj, 99 AS x FROM data)',
    }))).resolves.toEqual([{ total: 5 }])
  })

  it('preserves outer references in aggregate inputs', async () => {
    await expect(collect(executeSql({
      tables: {
        users: [{ id: 1 }, { id: 2 }],
        orders: columnSource([10, 20]),
      },
      query: `SELECT u.id,
        (SELECT SUM(u.id + id) FROM orders) AS total
        FROM users u ORDER BY u.id`,
    }))).resolves.toEqual([
      { id: 1, total: 32 },
      { id: 2, total: 34 },
    ])
  })
})

/**
 * @param {ReadColumn} readAttributes
 * @returns {AsyncBatch}
 */
function loadedBatch(readAttributes) {
  return {
    selection: { type: 'all', length: 4 },
    columns: [
      { type: 'values', values: ['claude', 'codex', 'claude', 'codex'], length: 4 },
      { type: 'values', values: ['a', 'b', 'c', 'b'], length: 4 },
      { read: readAttributes },
    ],
  }
}

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
