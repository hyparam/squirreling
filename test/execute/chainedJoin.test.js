import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'
import { memorySource } from '../../src/backend/dataSource.js'
import { planSql } from '../../src/plan/plan.js'

/**
 * @import { AsyncDataSource, RelationSchema, SqlPrimitive } from '../../src/types.js'
 */

/**
 * @param {Record<string, SqlPrimitive>[]} data
 * @returns {AsyncDataSource}
 */
function preparedSource(data) {
  const source = memorySource({ data })
  /** @type {RelationSchema} */
  const schema = { fields: Object.keys(data[0]).map((name, id) => ({ id, name, dataType: { type: 'string' }, nullable: true })) }
  return {
    ...source,
    schema,
    prepareScan(request) {
      return {
        schema,
        properties: {},
        residual: { filter: request.filter, limit: request.limit, offset: request.offset },
        async *batches() {
          yield {
            selection: { type: 'all', length: data.length },
            columns: schema.fields.map(field => ({ type: /** @type {const} */ 'values', values: data.map(row => row[field.name]), length: data.length })),
          }
        },
      }
    },
  }
}

for (const mode of ['array', 'scan', 'prepareScan']) {
  describe(`chained joins with ${mode}`, () => {
    const data = [{ s: 'a', k: 'x' }, { s: 'a', k: 'y' }]
    const tables = { t: mode === 'array' ? data : mode === 'scan' ? memorySource({ data }) : preparedSource(data) }
    for (const derived of [false, true]) {
      const source = derived ? '(SELECT * FROM t)' : 't'
      it.each([
        ['c.k = r.k', 4],
        ['r.k = c.k', 4],
        ['r.s = u.s', 8],
        ['r.k = c.k AND u.s = r.s AND c.s = c.k', 0],
        ['r.k = c.k AND u.s = r.s AND r.s = r.k', 0],
        ['r.k = c.k AND u.s = r.s AND c.k = \'x\' AND r.k = \'x\'', 2],
      ])(`counts ${derived ? 'derived' : 'base'} inputs with %s`, async (on, n) => {
        for (const first of ['c.s = u.s', 'u.s = c.s']) {
          const query = `SELECT count(*) n FROM ${source} u JOIN ${source} c ON ${first} JOIN ${source} r ON ${on}`
          expect(await collect(executeSql({ query, tables }))).toEqual([{ n }])
        }
      })
    }
    it.each(['INNER', 'LEFT', 'RIGHT', 'FULL'])('preserves the correct sides of a later %s JOIN', async joinType => {
      const query = `SELECT u.k uk, c.k ck, r.k rk FROM t u JOIN t c ON c.k = u.k ${joinType} JOIN t r ON r.k = c.k AND c.k = 'x' ORDER BY uk, ck, rk`
      const matched = { uk: 'x', ck: 'x', rk: 'x' }
      /** @type {Record<string, SqlPrimitive>} */
      const left = { uk: 'y', ck: 'y', rk: null }
      /** @type {Record<string, SqlPrimitive>} */
      const right = { uk: null, ck: null, rk: 'y' }
      const expected = joinType === 'INNER' ? [matched] : joinType === 'LEFT' ? [matched, left] : joinType === 'RIGHT' ? [right, matched] : [right, matched, left]
      expect(await collect(executeSql({ query, tables }))).toEqual(expected)
    })
  })
}

it('orients unqualified columns using all source schemas', async () => {
  const tables = {
    a: memorySource({ data: [{ old: 1 }] }),
    b: memorySource({ data: [{ middle: 1 }] }),
    c: memorySource({ data: [{ fresh: 1 }, { fresh: 2 }] }),
  }
  const query = 'SELECT old, middle, fresh FROM a JOIN b ON middle = old JOIN c ON fresh = old'
  const plan = planSql({ query, tables })
  if (plan.type !== 'Project' || plan.child.type !== 'HashJoin') throw new Error('expected hash join')
  expect(plan.child.leftKeys.map(key => key.type === 'identifier' && key.name)).toEqual(['old'])
  expect(plan.child.rightKeys.map(key => key.type === 'identifier' && key.name)).toEqual(['fresh'])
  expect(await collect(executeSql({ query, tables }))).toEqual([{ old: 1, middle: 1, fresh: 1 }])
})

it('normalizes mixed keys and pushes same-side equalities into their inputs', () => {
  const tables = { t: memorySource({ data: [{ s: 'a', k: 'x' }] }) }
  const query = 'SELECT u.s FROM t u JOIN t c ON c.s = u.s JOIN t r ON r.k = c.k AND u.s = r.s AND r.s = r.k AND c.s = c.k'
  const plan = planSql({ query, tables })
  if (plan.type !== 'Project' || plan.child.type !== 'HashJoin') throw new Error('expected hash join')
  const join = plan.child
  expect(join.leftKeys.map(key => key.type === 'identifier' && `${key.prefix}.${key.name}`)).toEqual(['c.k', 'u.s'])
  expect(join.rightKeys.map(key => key.type === 'identifier' && `${key.prefix}.${key.name}`)).toEqual(['r.k', 'r.s'])
  expect(join.residual).toBeUndefined()
  if (join.right.type !== 'Scan' || join.left.type !== 'HashJoin' || join.left.right.type !== 'Scan') throw new Error('expected scans')
  expect(join.right.hints.where?.type).toEqual('binary')
  expect(join.left.right.hints.where?.type).toEqual('binary')
})

it('retains outer-join null rejection through reversed keys', async () => {
  const tables = { t: memorySource({ data: [{ s: 'a', k: 'x' }, { s: 'a', k: 'y' }] }) }
  const query = 'SELECT u.k uk, r.k rk FROM t u LEFT JOIN t c ON c.k = u.k LEFT JOIN t r ON r.k = c.k WHERE r.k = \'x\''
  const plan = planSql({ query, tables })
  if (plan.type !== 'Project' || plan.child.type !== 'HashJoin' || plan.child.left.type !== 'HashJoin') throw new Error('expected hash joins')
  expect(plan.child.joinType).toEqual('INNER')
  expect(plan.child.left.joinType).toEqual('INNER')
  expect(await collect(executeSql({ query, tables }))).toEqual([{ uk: 'x', rk: 'x' }])
})

it('projects columns from four tables with reversed keys', async () => {
  const query = 'SELECT u.s, c.k, r.k rk, v.k vk FROM t u JOIN t c ON c.s = u.s JOIN t r ON r.k = c.k JOIN t v ON v.k = r.k ORDER BY c.k'
  expect(await collect(executeSql({ query, tables: { t: [{ s: 'a', k: 'x' }, { s: 'a', k: 'y' }] } }))).toEqual([
    { s: 'a', k: 'x', rk: 'x', vk: 'x' },
    { s: 'a', k: 'x', rk: 'x', vk: 'x' },
    { s: 'a', k: 'y', rk: 'y', vk: 'y' },
    { s: 'a', k: 'y', rk: 'y', vk: 'y' },
  ])
})
