import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { evaluateExpr } from '../../src/expression/evaluate.js'
import { collect, executeSql } from '../../src/index.js'
import { scanFilter } from '../../src/execute/scanFilter.js'
import { parseSql } from '../../src/parse/parse.js'

/**
 * @import { AsyncDataSource, AsyncRow, ExprNode, RelationSchema, ScanColumnOptions, ScanOptions, ScanRequest } from '../../src/types.js'
 */

const data = [{ id: 1, flag: 1 }, { id: 2, flag: 0 }, { id: 3, flag: 1 }]

for (const mode of ['scan', 'prepareScan', 'scanColumn']) {
  for (const apply of [false, true]) {
    describe(`${mode}, applied filter: ${apply}`, () => {
      it.each(['t', 'data', '"Mixed Case"'])('strips the local qualifier %s', async alias => {
        const { source, filters } = captureSource(mode, apply)
        const from = alias === 'data' ? 'data' : `data ${alias}`
        expect(await collect(executeSql({ tables: { data: source }, query: `SELECT ${alias}.id FROM ${from} WHERE ${alias}.id > 1` })))
          .toEqual([{ id: 2 }, { id: 3 }])
        expect(filters.map(clean)).toEqual([clean(predicate('id > 1'))])
      })

      it('leaves bare columns unchanged', async () => {
        const { source, filters } = captureSource(mode, apply)
        expect(await collect(executeSql({ tables: { data: source }, query: 'SELECT id FROM data WHERE id > 1' })))
          .toEqual([{ id: 2 }, { id: 3 }])
        expect(filters.map(clean)).toEqual([clean(predicate('id > 1'))])
      })

      it('pushes each self-join side independently despite colliding column names', async () => {
        const { source, filters } = captureSource(mode, apply)
        expect(await collect(executeSql({ tables: { data: source }, query: `
          SELECT r.id FROM data r JOIN data c ON r.id = c.id
          WHERE r.id > 1 AND c.id < 3` }))).toEqual([{ id: 2 }])
        expect(filters.map(clean)).toEqual([clean(predicate('id < 3')), clean(predicate('id > 1 AND id IN (1, 2)'))])
      })

      it('keeps correlated conjuncts and ranges in the engine', async () => {
        const { source, filters, requests } = captureSource(mode, apply)
        expect(await collect(executeSql({ tables: { data: source }, query: `
          SELECT o.id, (SELECT i.id FROM data i WHERE i.id > o.id AND i.id > 1 LIMIT 1) next
          FROM data o` }))).toEqual([{ id: 1, next: 2 }, { id: 2, next: 3 }, { id: 3, next: null }])
        expect(filters.slice(1).map(clean)).toEqual(Array(3).fill(clean(predicate('id > 1'))))
        expect(requests.slice(1).map(request => request.limit)).toEqual([undefined, undefined, undefined])
      })

      it('distinguishes aliases that differ only in case', async () => {
        const { source, filters } = captureSource(mode, apply)
        expect(await collect(executeSql({ tables: { data: source }, query: `
          SELECT t.id FROM data t JOIN data T ON t.id = T.id
          WHERE t.id > 1 AND T.id < 3` }))).toEqual([{ id: 2 }])
        expect(filters.map(clean)).toEqual([clean(predicate('id < 3')), clean(predicate('id > 1 AND id IN (1, 2)'))])
      })

      it('keeps correlated ordering and offset above the residual', async () => {
        const { source, requests } = captureSource(mode, apply)
        expect(await collect(executeSql({ tables: { data: source }, query: `
          SELECT o.id, (SELECT i.id FROM data i WHERE i.id > o.id
            ORDER BY i.id DESC LIMIT 1 OFFSET 1) next FROM data o` })))
          .toEqual([{ id: 1, next: 2 }, { id: 2, next: null }, { id: 3, next: null }])
        for (const request of requests.slice(1)) {
          expect(request.limit).toBeUndefined()
          expect(request.offset).toBeUndefined()
          expect('topK' in request ? request.topK : undefined).toBeUndefined()
        }
      })

      it('normalizes the scalar aggregate column fast path', async () => {
        const { source, filters } = captureSource(mode, apply)
        expect(await collect(executeSql({ tables: { data: source }, query: 'SELECT SUM(t.id) n FROM data t WHERE t.id > 1' })))
          .toEqual([{ n: 5 }])
        expect(filters.map(clean)).toEqual([clean(predicate('id > 1'))])
      })
    })
  }
}

it('does not fold prefix case or split an unsafe OR conjunct', () => {
  const where = predicate('t.id > 1 AND (T.id = 2 OR t.id = 3)')
  const result = scanFilter({ type: 'Scan', table: 'data', alias: 't', hints: { where } }, ['id'])
  expect(clean(result.where)).toEqual(clean(predicate('id > 1')))
  expect(clean(result.residual)).toEqual(clean(predicate('T.id = 2 OR t.id = 3')))
})

it('keeps struct and physical qualified-name collisions in the engine', () => {
  for (const columns of [['t'], ['id', 't.id']]) {
    const where = predicate('t.id = 1')
    expect(scanFilter({ type: 'Scan', table: 'data', alias: 't', hints: { where } }, columns)).toEqual({ residual: where })
  }
})

/**
 * @param {string} sql
 * @returns {ExprNode}
 */
function predicate(sql) {
  const parsed = parseSql({ query: `SELECT * FROM data WHERE ${sql}` })
  if (parsed.type !== 'select') throw new Error('expected select')
  return parsed.where
}

/**
 * Remove source locations when comparing predicates written at different offsets.
 * @param {unknown} value
 * @returns {unknown}
 */
function clean(value) {
  if (value === undefined) return value
  return JSON.parse(JSON.stringify(value, (key, value) => key.startsWith('position') ? undefined : value))
}

/**
 * @param {string} mode
 * @param {boolean} apply
 * @returns {{ source: AsyncDataSource, filters: ExprNode[], requests: (ScanOptions | ScanColumnOptions | ScanRequest)[] }}
 */
function captureSource(mode, apply) {
  /** @type {ExprNode[]} */
  const filters = []
  /** @type {(ScanOptions | ScanColumnOptions | ScanRequest)[]} */
  const requests = []
  const memory = memorySource({ data })
  /** @type {RelationSchema} */
  const schema = { fields: ['id', 'flag'].map((name, id) => ({ id, name, dataType: { type: 'number' }, nullable: false })) }
  /**
   * @param {ExprNode} where
   * @returns {Promise<AsyncRow[]>}
   */
  async function selected(where) {
    const rows = []
    for await (const row of memory.scan({}).rows()) {
      if (!apply || !where || await evaluateExpr({ node: where, row, context: { tables: {} } })) rows.push(row)
    }
    return rows
  }
  /** @type {AsyncDataSource} */
  const source = {
    columns: ['id', 'flag'],
    scan(options) {
      filters.push(options.where)
      requests.push(options)
      return {
        appliedWhere: apply,
        appliedLimitOffset: false,
        async *rows() { yield* await selected(options.where) },
      }
    },
  }
  if (mode === 'scanColumn') source.scanColumn = function scanColumn(options) {
    filters.push(options.where)
    requests.push(options)
    return {
      appliedWhere: apply,
      appliedLimitOffset: false,
      async *chunks() {
        yield await Promise.all((await selected(options.where)).map(row => row.cells[options.column]()))
      },
    }
  }
  if (mode === 'prepareScan') {
    source.schema = schema
    source.prepareScan = function prepareScan(request) {
      filters.push(request.filter)
      requests.push(request)
      return {
        schema,
        properties: {},
        residual: { filter: apply ? undefined : request.filter, limit: request.limit, offset: request.offset },
        async *batches() {
          const rows = await selected(request.filter)
          yield {
            selection: { type: 'all', length: rows.length },
            columns: await Promise.all(schema.fields.map(async field => ({
              type: /** @type {const} */ 'values',
              values: await Promise.all(rows.map(row => row.cells[field.name]())),
              length: rows.length,
            }))),
          }
        },
      }
    }
  }
  return { source, filters, requests }
}
