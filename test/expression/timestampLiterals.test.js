import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * @import { AsyncDataSource, ExprNode, RelationSchema } from '../../src/types.js'
 */

const rows = [
  { id: 1, ts: new Date('2026-10-23T20:00:00Z'), name: '2026-10-23' },
  { id: 2, ts: new Date('2026-10-24T20:00:00Z'), name: '2026-10-24' },
  { id: 3, ts: null, name: 'x' },
]

/**
 * @param {string} where
 * @returns {Promise<number[]>}
 */
async function ids(where) {
  const result = await collect(executeSql({ tables: { t: rows }, query: `SELECT id FROM t WHERE ${where}` }))
  return result.map(row => Number(row.id))
}

describe('string literals compared with timestamps', () => {
  it.each([
    ['ts >= \'2026-10-24\'', [2]],
    ['ts < \'2026-10-24\'', [1]],
    ['\'2026-10-24\' <= ts', [2]],
    ['ts = \'2026-10-24T20:00:00Z\'', [2]],
    ['ts <> \'2026-10-24T20:00:00Z\'', [1]],
    ['ts BETWEEN \'2026-10-23\' AND \'2026-10-24\'', [1]],
    ['ts IN (\'2026-10-23T20:00:00Z\', \'2026-01-01\')', [1]],
    ['ts NOT IN (\'2026-10-23T20:00:00Z\')', [2]],
    ['ts >= TIMESTAMP \'2026-10-24\'', [2]],
  ])('types the literal: %s', async (where, expected) => {
    expect(await ids(where)).toEqual(expected)
  })

  it('leaves string comparisons alone', async () => {
    expect(await ids('name >= \'2026-10-24\'')).toEqual([2, 3])
    expect(await ids('CAST(ts AS VARCHAR) LIKE \'"2026-10-24%\'')).toEqual([2])
  })

  it('rejects a literal that is not a timestamp', async () => {
    await expect(ids('ts >= \'yesterday\'')).rejects.toThrow(
      'Cannot compare TIMESTAMP to string \'yesterday\', expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS'
    )
  })

  describe('prepared sources', () => {
    /** @type {RelationSchema} */
    const schema = {
      fields: [
        { id: 1, name: 'id', dataType: { type: 'number' }, nullable: false },
        { id: 2, name: 'ts', dataType: { type: 'date' }, nullable: true },
      ],
    }

    /**
     * A source that applies nothing, keeps every filter residual, and
     * records the filter it was offered.
     *
     * @returns {{ source: AsyncDataSource, filters: (ExprNode | undefined)[] }}
     */
    function preparedSource() {
      /** @type {(ExprNode | undefined)[]} */
      const filters = []
      /** @type {AsyncDataSource} */
      const source = {
        schema,
        prepareScan(request) {
          filters.push(request.filter)
          const fields = request.columns.map(demand => schema.fields[demand.field - 1])
          return {
            schema: { fields },
            residual: { filter: request.filter },
            properties: {},
            async *batches() {
              yield {
                selection: { type: 'all', length: rows.length },
                columns: fields.map(field => ({
                  type: 'values',
                  values: rows.map(row => field.name === 'id' ? row.id : row.ts),
                  length: rows.length,
                })),
              }
            },
          }
        },
      }
      return { source, filters }
    }

    it('types the literal on the batch path', async () => {
      const { source } = preparedSource()
      const result = executeSql({ tables: { t: source }, query: 'SELECT id FROM t WHERE ts >= \'2026-10-24\'' })
      expect(result.batches).toBeTypeOf('function')
      expect(await collect(result)).toEqual([{ id: 2 }])
    })

    it('offers the source a timestamp cast', async () => {
      const { source, filters } = preparedSource()
      await collect(executeSql({
        tables: { t: source },
        query: 'SELECT id FROM t WHERE ts >= \'2026-10-24\' AND ts IN (\'2026-10-24T20:00:00Z\')',
      }))
      expect(filters[0]).toEqual({
        type: 'binary',
        op: 'AND',
        left: {
          type: 'binary',
          op: '>=',
          left: { type: 'identifier', name: 'ts', positionStart: 23, positionEnd: 25 },
          right: {
            type: 'cast',
            toType: 'TIMESTAMP',
            expr: { type: 'literal', value: '2026-10-24', positionStart: 29, positionEnd: 41 },
            positionStart: 29,
            positionEnd: 41,
          },
          positionStart: 23,
          positionEnd: 41,
        },
        right: {
          type: 'in valuelist',
          expr: { type: 'identifier', name: 'ts', positionStart: 46, positionEnd: 48 },
          values: [{
            type: 'cast',
            toType: 'TIMESTAMP',
            expr: { type: 'literal', value: '2026-10-24T20:00:00Z', positionStart: 53, positionEnd: 75 },
            positionStart: 53,
            positionEnd: 75,
          }],
          positionStart: 46,
          positionEnd: 76,
        },
        positionStart: 23,
        positionEnd: 76,
      })
    })

    it('keeps an unparseable literal so the engine reports it', async () => {
      const { source } = preparedSource()
      await expect(collect(executeSql({ tables: { t: source }, query: 'SELECT id FROM t WHERE ts >= \'soon\'' })))
        .rejects.toThrow('Cannot compare TIMESTAMP to string \'soon\', expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS')
    })
  })
})
