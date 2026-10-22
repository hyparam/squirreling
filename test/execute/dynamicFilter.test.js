import { describe, expect, it, vi } from 'vitest'
import * as dynamicFilters from '../../src/execute/dynamicFilter.js'
import { memorySource } from '../../src/backend/dataSource.js'
import { evaluateExpr } from '../../src/expression/evaluate.js'
import { collect, executeSql } from '../../src/index.js'

/** @import { AsyncDataSource, ExprNode, Row, ScanOptions } from '../../src/types.js' */

/**
 * @param {Row} data
 * @param {ScanOptions[]} calls
 * @param {boolean} apply
 * @returns {AsyncDataSource}
 */
function source(data, calls, apply) {
  const base = memorySource({ data, columns: ['id', 'sid', 'keep'] })
  return {
    columns: base.columns,
    scan(options) {
      calls.push(options)
      const result = base.scan(options)
      return {
        appliedWhere: apply,
        appliedLimitOffset: false,
        async *rows() {
          for await (const row of result.rows()) {
            if (!apply || !options.where || await evaluateExpr({ node: options.where, row, context: { tables: {} } })) yield row
          }
        },
      }
    },
  }
}

/**
 * @param {ExprNode} expr
 * @returns {Record<string, unknown[]>}
 */
function lists(expr) {
  if (expr?.type === 'binary' && expr.op === 'AND') return { ...lists(expr.left), ...lists(expr.right) }
  if (expr?.type !== 'in valuelist') return {}
  expect(expr.expr.type).toBe('identifier')
  if (expr.expr.type !== 'identifier') return {}
  expect(expr.expr.prefix).toBeUndefined()
  return { [expr.expr.name]: expr.values.map(value => value?.type === 'literal' ? value.value : undefined) }
}

const small = [{ id: 1, sid: 'a', keep: 1 }, { id: 1, sid: 'a', keep: 1 }, { id: null, sid: 'b', keep: 1 }]
const large = [{ id: 1, sid: 'a', keep: 1 }, { id: 2, sid: 'b', keep: 1 }, { id: null, sid: 'c', keep: 1 }]

for (const apply of [false, true]) {
  describe(`dynamic join filters, source applies: ${apply}`, () => {
    /**
     * @param {string} query
     * @param {Row} [build]
     * @param {Row} [probe]
     * @returns {Promise<{ a: ScanOptions[], b: ScanOptions[], rows: Row }>}
     */
    async function run(query, build = small, probe = large) {
      /** @type {ScanOptions[]} */
      const a = []
      /** @type {ScanOptions[]} */
      const b = []
      const tables = { a: source(build, a, apply), b: source(probe, b, apply) }
      const rows = await collect(executeSql({ tables, query }))
      // Compare with pushdown disabled only in this test, without a public switch.
      const unpushed = vi.spyOn(dynamicFilters, 'dynamicFilter').mockReturnValue({
        add() {},
        apply(plan) { return plan },
      })
      try {
        const fallback = await collect(executeSql({
          tables: { a: source(build, [], apply), b: source(probe, [], apply) }, query,
        }))
        expect(rows).toEqual(fallback)
      } finally {
        unpushed.mockRestore()
      }
      return { a, b, rows }
    }

    it.each(['JOIN', 'LEFT JOIN'])('pushes distinct keys to the optional/probe side of %s', async join => {
      const result = await run(`SELECT a.id, b.sid FROM a ${join} b ON a.id = b.id AND a.sid = b.sid WHERE a.keep = 1`)
      expect(result.a).toHaveLength(1)
      expect(result.b).toHaveLength(1)
      expect(lists(result.a[0].where)).toEqual({})
      expect(lists(result.b[0].where)).toEqual({ id: [1], sid: ['a'] })
    })

    it('reduces null-rejecting LEFT JOIN and builds its filtered right side', async () => {
      const result = await run('SELECT a.id FROM b LEFT JOIN a ON a.id = b.id WHERE a.keep = 1')
      expect(lists(result.b[0].where)).toEqual({ id: [1] })
    })

    it('does not filter the preserved probe input', async () => {
      const result = await run('SELECT b.id FROM b LEFT JOIN a ON a.id = b.id AND a.keep = 1')
      expect(lists(result.b[0].where)).toEqual({})
      expect(result.rows).toHaveLength(4)
    })

    it('does not push for FULL JOIN', async () => {
      const result = await run('SELECT a.id FROM a FULL JOIN b ON a.id = b.id WHERE a.keep = 1 OR b.keep = 1')
      expect(lists(result.a[0].where)).toEqual({})
      expect(lists(result.b[0].where)).toEqual({})
    })

    it('releases an overflowing key set while retaining another column', async () => {
      const build = Array.from({ length: 257 }, (_, id) => ({ id, sid: 'a', keep: 1 }))
      const result = await run('SELECT a.id FROM a JOIN b ON a.id = b.id AND a.sid = b.sid WHERE a.keep = 1', build)
      expect(lists(result.b[0].where)).toEqual({ sid: ['a'] })
    })

    it.each([{ build: [] }, { build: [{ id: null, sid: 'a', keep: 1 }] }])('skips the probe with no matchable build keys: $build', async ({ build }) => {
      const result = await run('SELECT a.id FROM a JOIN b ON a.id = b.id WHERE a.keep = 1', build)
      expect(result.b).toEqual([])
      expect(result.rows).toEqual([])
    })

    it.each([
      'b.id IN (SELECT a.id FROM a WHERE a.keep = 1)',
      'EXISTS (SELECT 1 FROM a WHERE a.id = b.id AND a.keep = 1)',
    ])('pushes semi-join keys: %s', async predicate => {
      const result = await run(`SELECT b.id FROM b WHERE ${predicate}`)
      expect(lists(result.b[0].where)).toEqual({ id: [1] })
      expect((await run(`SELECT b.id FROM b WHERE ${predicate}`, [])).b).toEqual([])
    })

    it.each([
      'b.id NOT IN (SELECT a.id FROM a WHERE a.keep = 1)',
      'NOT EXISTS (SELECT 1 FROM a WHERE a.id = b.id AND a.keep = 1)',
    ])('does not push anti-join keys: %s', async predicate => {
      const result = await run(`SELECT b.id FROM b WHERE ${predicate}`, small.slice(0, 1))
      expect(lists(result.b[0].where)).toEqual({})
    })

    it('retains literal types including BIGINT and cross-type semi-join equality', async () => {
      const build = [9007199254740993n, '2', 3].map(id => ({ id, sid: 'a', keep: 1 }))
      const probe = [9007199254740993n, 9007199254740992, '2', 2, 3, '3'].map(id => ({ id, sid: 'a', keep: 1 }))
      for (const query of [
        'SELECT b.id FROM b JOIN a ON a.id = b.id WHERE a.keep = 1',
        'SELECT b.id FROM b WHERE b.id IN (SELECT id FROM a)',
      ]) {
        const result = await run(query, build, probe)
        expect(lists(result.b[0].where)).toEqual({ id: [9007199254740993n, '2', 3] })
      }
    })

    it('caps distinct keys at 256', async () => {
      const build = Array.from({ length: 257 }, (_, id) => ({ id, sid: 'a', keep: 1 }))
      const query = 'SELECT a.id FROM a JOIN b ON a.id = b.id WHERE a.keep = 1'
      expect(lists((await run(query, build)).b[0].where)).toEqual({})
      expect(lists((await run(query, build.slice(0, 256))).b[0].where).id).toHaveLength(256)
    })

    it('pushes each chained join and rebuilds filters on repeated iteration', async () => {
      /** @type {ScanOptions[]} */
      const b = []
      /** @type {ScanOptions[]} */
      const c = []
      const data = [{ id: 1, sid: 'a', keep: 1 }]
      const tables = { a: source(data, [], apply), b: source(large, b, apply), c: source(large, c, apply) }
      const result = executeSql({ tables, query: `SELECT a.id FROM a JOIN b ON a.id = b.id
        JOIN c ON b.sid = c.sid WHERE a.keep = 1` })
      expect(b).toEqual([])
      expect(c).toEqual([])
      expect(await collect(result)).toEqual([{ id: 1 }])
      expect(lists(b[0].where)).toEqual({ id: [1] })
      expect(lists(c[0].where)).toEqual({ sid: ['a'] })
      data[0].id = 2
      expect(await collect(result)).toEqual([{ id: 2 }])
      expect(lists(b[1].where)).toEqual({ id: [2] })
    })

    it('does not cross a LIMIT barrier', async () => {
      const result = await run('SELECT a.id FROM a JOIN (SELECT * FROM b LIMIT 1) b ON a.id = b.id WHERE a.keep = 1')
      expect(lists(result.b[0].where)).toEqual({})
    })

    it('preserves existing serialized tuple comparisons', async () => {
      for (const [id, sid, otherId, otherSid] of [
        ['a|b', 'c', 'a', 'b|c'], ['true', 'a', true, 'a'], ['[1]', 'a', [1], 'a'],
      ]) {
        await run('SELECT a.id FROM a JOIN b ON a.id = b.id AND a.sid = b.sid WHERE a.keep = 1',
          [{ id, sid, keep: 1 }], [{ id: otherId, sid: otherSid, keep: 1 }])
      }
    })
  })
}

it('does not open an aggregate probe column scan for an empty build', async () => {
  let scans = 0
  const tables = {
    a: memorySource({ data: [], columns: ['id'] }),
    b: {
      ...memorySource({ data: [{ id: 1 }, { id: 2 }] }),
      scanColumn() {
        scans++
        return { async *chunks() { yield [1, 2] }, appliedWhere: false, appliedLimitOffset: false }
      },
    },
  }
  expect(await collect(executeSql({ tables, query: 'SELECT a.id FROM a JOIN (SELECT sum(id) id FROM b) b ON a.id = b.id' })))
    .toEqual([])
  expect(scans).toBe(0)
})
