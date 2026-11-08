import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * @import { AsyncDataSource, RelationSchema, RowSelection, SqlPrimitive } from '../../src/types.js'
 */

/** @type {RelationSchema} */
const schema = {
  fields: [
    { id: 1, name: 'id', dataType: { type: 'number' }, nullable: false },
    { id: 2, name: 'grp', dataType: { type: 'string' }, nullable: true },
    { id: 3, name: 'ts', dataType: { type: 'number' }, nullable: true },
    { id: 4, name: 'payload', dataType: { type: 'string' }, nullable: false },
  ],
}

/**
 * @param {number} count
 * @returns {Record<string, SqlPrimitive>[]}
 */
function makeRows(count) {
  /** @type {Record<string, SqlPrimitive>[]} */
  const rows = []
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i,
      grp: i % 7 === 0 ? null : ['a', 'b', 'c'][i % 3],
      ts: i % 11 === 0 ? null : i * 37 % 101,
      payload: `p${i}`,
    })
  }
  return rows
}

/**
 * A prepared source that yields fixed-size batches, filters out every fifth
 * row through its selection, and records every payload read.
 *
 * @param {Record<string, SqlPrimitive>[]} rows
 * @param {number} batchRows
 * @returns {{ source: AsyncDataSource, payloadReads: number[] }}
 */
function batchedSource(rows, batchRows) {
  /** @type {number[]} */
  const payloadReads = []
  /** @type {AsyncDataSource} */
  const source = {
    numRows: rows.length,
    schema,
    prepareScan(request) {
      const fields = request.columns.map(demand => {
        const field = schema.fields.find(candidate => candidate.id === demand.field)
        if (!field) throw new Error(`unknown field: ${demand.field}`)
        return field
      })
      return {
        schema: { fields },
        residual: { filter: request.filter, limit: request.limit, offset: request.offset },
        properties: {},
        async *batches() {
          for (let start = 0; start < rows.length; start += batchRows) {
            const slice = rows.slice(start, start + batchRows)
            const { length } = slice
            /** @type {number[]} */
            const kept = []
            for (let i = 0; i < length; i++) {
              if (Number(slice[i].id) % 5 !== 4) kept.push(i)
            }
            yield {
              selection: { type: 'indices', indices: new Uint32Array(kept), length },
              columns: fields.map(field => {
                if (field.name !== 'payload') {
                  return { type: 'values', values: slice.map(row => row[field.name]), length }
                }
                return {
                  /**
                   * @param {{ selection: RowSelection }} request
                   * @returns {{ type: 'values', values: SqlPrimitive[], length: number }}
                   */
                  read({ selection }) {
                    if (selection.type !== 'indices') throw new Error('expected indices')
                    payloadReads.push(selection.indices.length)
                    return {
                      type: 'values',
                      values: Array.from(selection.indices, index => slice[index].payload),
                      length: selection.indices.length,
                    }
                  },
                }
              }),
            }
          }
        },
      }
    },
  }
  return { source, payloadReads }
}

describe('batch sort', () => {
  const rows = makeRows(3000)
  const expectedRows = rows.filter(row => Number(row.id) % 5 !== 4)

  it.each([
    'SELECT id, payload FROM t ORDER BY ts',
    'SELECT id, payload FROM t ORDER BY ts DESC',
    'SELECT id, payload FROM t ORDER BY ts DESC NULLS LAST, grp NULLS FIRST, id DESC',
    'SELECT id, grp FROM t ORDER BY grp, ts',
    'SELECT id, payload FROM t ORDER BY ts + id % 3, id',
    'SELECT id, payload FROM t WHERE grp = \'b\' ORDER BY ts DESC',
    'SELECT id, payload FROM t ORDER BY ts LIMIT 25 OFFSET 10',
    'SELECT id FROM t ORDER BY ts DESC, id LIMIT 1500',
  ])('matches the row path: %s', async query => {
    const { source } = batchedSource(rows, 256)
    const result = executeSql({ tables: { t: source }, query })
    expect(result.batches).toBeTypeOf('function')
    const expected = await collect(executeSql({ tables: { t: expectedRows }, query }))
    expect(await collect(result)).toEqual(expected)
  })

  it('reads payload only for rows a streaming consumer takes', async () => {
    const { source, payloadReads } = batchedSource(rows, 1024)
    const result = executeSql({ tables: { t: source }, query: 'SELECT id, payload FROM t ORDER BY ts DESC NULLS LAST' })
    /** @type {SqlPrimitive[]} */
    const taken = []
    for await (const row of result.rows()) {
      taken.push(await row.cells.payload())
      if (taken.length === 10) break
    }
    expect(taken).toHaveLength(10)
    const total = payloadReads.reduce((sum, count) => sum + count, 0)
    // The first output chunk holds 64 rows; nothing past it is read.
    expect(total).toBe(64)
  })

  it('reads a source once when every chunk spans most of it', async () => {
    const { source, payloadReads } = batchedSource(rows.slice(0, 1000), 1000)
    const query = 'SELECT id, payload FROM t ORDER BY grp, id'
    const expected = await collect(executeSql({ tables: { t: expectedRows.slice(0, 800) }, query }))
    expect(await collect(executeSql({ tables: { t: source }, query }))).toEqual(expected)
    expect(payloadReads).toEqual([800])
  })

  it('keeps the row path for keys that do not compile', async () => {
    const { source } = batchedSource(rows.slice(0, 20), 8)
    const query = 'SELECT id FROM t ORDER BY (SELECT 1), id'
    const expected = await collect(executeSql({ tables: { t: expectedRows.slice(0, 16) }, query }))
    expect(await collect(executeSql({ tables: { t: source }, query }))).toEqual(expected)
  })
})
