import { describe, expect, it } from 'vitest'
import { collect, executeSql, planSql } from '../../src/index.js'
import { memorySource } from '../../src/backend/dataSource.js'

/** @import { AsyncDataSource, ScanOptions, SqlPrimitive, SqlType } from '../../src/types.js' */

const data = {
  c: [{ k: 1, j: 1 }, { k: 2, j: 1 }, { k: 3, j: 2 }, { k: null, j: 1 }],
  r: [{ k: 1, j: 1, x: 4 }, { k: 1, j: 2, x: 6 }, { k: 2, j: 1, x: null }, { k: null, j: 1, x: 9 }],
}

/**
 * @param {Record<string, SqlPrimitive>[]} data
 * @param {SqlType} [keyType]
 * @returns {AsyncDataSource}
 */
function typedSource(data, keyType = { type: 'number' }) {
  const source = memorySource({ data })
  return {
    ...source,
    schema: { fields: (source.columns ?? []).map((name, id) => ({ id, name, dataType: keyType, nullable: true })) },
  }
}

const tables = { c: typedSource(data.c), r: typedSource(data.r) }

describe('correlated scalar aggregate joins', () => {
  it.each([
    ['count(*)', [2, 1, 0, 0]],
    ['count(x)', [2, 0, 0, 0]],
    ['count(distinct x)', [2, 0, 0, 0]],
    ['sum(x)', [10, null, null, null]],
    ['max(x)', [6, null, null, null]],
    ['count(*) + 1', [3, 2, 1, 1]],
    ['coalesce(sum(x), 0)', [10, 0, 0, 0]],
  ])('matches the per-row path for %s', async (expression, expected) => {
    const query = `select c.k, (select ${expression} from r where r.k = c.k) n from c`
    const actual = await collect(executeSql({ tables, query }))
    // LIMIT on the scalar aggregate prevents decorrelation without changing results.
    const fallback = await collect(executeSql({ tables, query: query.replace('r.k = c.k)', 'r.k = c.k limit 1)') }))
    expect(actual).toEqual(fallback)
    expect(actual.map(row => row.n)).toEqual(expected)
    expect(JSON.stringify(planSql({ query, tables }))).toContain('"SemiJoin","test":"scalar"')
  })

  it('handles multiple keys and two scalar subqueries', async () => {
    const query = 'select c.k, (select count(*) from r where r.k = c.k and r.j = c.j) n, (select max(x) from r where c.k = r.k) m from c'
    expect(await collect(executeSql({ tables, query }))).toEqual([
      { k: 1, n: 1, m: 6 }, { k: 2, n: 1, m: null }, { k: 3, n: 0, m: null }, { k: null, n: 0, m: null },
    ])
  })

  it('filters before LIMIT and supports ORDER BY', async () => {
    const query = 'select c.k from c where (select count(*) from r where r.k = c.k) = 0 order by (select count(*) from r where r.j = c.j) limit 1'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 3 }])
  })

  it('keeps synthetic columns out of star projections', async () => {
    expect(await collect(executeSql({ tables, query: 'select * from c where (select count(*) from r where r.k = c.k) > 1' }))).toEqual([{ k: 1, j: 1 }])
  })

  it('keeps synthetic columns out of scalar aggregate stars', async () => {
    const query = 'select *, count(*) n from c where (select count(*) from r where r.k = c.k) > 0'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 1, j: 1, n: 2 }])
  })

  it('keeps synthetic columns out of grouped aggregate stars', async () => {
    const query = 'select *, count(*) n from c where (select count(*) from r where r.k = c.k) > 0 group by c.k, c.j'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 1, j: 1, n: 1 }, { k: 2, j: 1, n: 1 }])
  })

  it.each(['r.k < c.k', 'r.k + 0 = c.k', 'r.k = c.k or r.j = c.j'])('retains fallback for %s', async condition => {
    const query = `select (select count(*) from r where ${condition}) n from c`
    expect(JSON.stringify(planSql({ query, tables }))).not.toContain('SemiJoin')
    expect(await collect(executeSql({ tables, query }))).toEqual(await collect(executeSql({ tables, query: query.replace(') n', ' limit 1) n') })))
  })

  it('retains correlated HAVING fallback', async () => {
    const query = 'select (select count(*) from r having count(*) > c.k) n from c'
    expect(JSON.stringify(planSql({ query, tables }))).not.toContain('SemiJoin')
    expect((await collect(executeSql({ tables, query }))).map(row => row.n)).toEqual([4, 4, 4, null])
  })

  it('rejects non-aggregate scalar results with more than one row', async () => {
    await expect(collect(executeSql({ tables, query: 'select (select r.x from r where r.k = c.k) n from c' }))).rejects.toThrow('Scalar subquery returned more than one row')
  })

  it('handles a scalar subquery nested inside another', async () => {
    const query = 'select (select (select count(*) from r where r.k = d.k) from c d where d.k = c.k) n from c'
    expect((await collect(executeSql({ tables, query }))).map(row => row.n)).toEqual([2, 1, 0, null])
  })

  it('builds once and pushes bare uncorrelated filters to the source', async () => {
    const source = tables.r
    /** @type {ScanOptions[]} */
    const scans = []
    /** @type {AsyncDataSource} */
    const r = { ...source, scan(options) { scans.push(options); return source.scan(options) } }
    const query = 'select (select count(*) from r where r.j = 1 and r.k = c.k) n from c'
    expect((await collect(executeSql({ tables: { ...tables, r }, query }))).map(row => row.n)).toEqual([1, 1, 0, 0])
    expect(scans).toHaveLength(1)
    const { where } = scans[0]
    if (where?.type !== 'binary' || where.left.type !== 'identifier') throw new Error('Expected bare filter')
    expect(where.left.name).toEqual('j')
    expect(where.left.prefix).toBeUndefined()
  })

  it('preserves composite key boundaries', async () => {
    const data = [{ k: 'a|b', j: 'c' }, { k: 'a', j: 'b|c' }, { k: 'a', j: 'b|c' }]
    const query = 'select (select count(*) from r where r.k = c.k and r.j = c.j) n from c'
    expect(await collect(executeSql({ tables: { c: typedSource(data, { type: 'string' }), r: typedSource(data, { type: 'string' }) }, query }))).toEqual([{ n: 1 }, { n: 2 }, { n: 2 }])
  })

  it('retains coercive equality across mixed key types', async () => {
    const data = [{ k: 1 }, { k: '1' }, { k: true }]
    const query = 'select (select count(*) from r where r.k = c.k) n from c'
    expect(await collect(executeSql({ tables: { c: data, r: data }, query }))).toEqual([{ n: 3 }, { n: 3 }, { n: 3 }])
  })

  it('preserves coercive matches before array keys are grouped', async () => {
    for (const keys of [[[null], [NaN]], [[NaN], [null]]]) {
      const tables = { c: [{ k: 0 }], r: keys.map(k => ({ k })) }
      for (const aggregate of ['count(*)', 'count(*) + 0', 'array_agg(k)']) {
        const query = `select (select ${aggregate} from r where r.k = c.k) n from c`
        const expected = await collect(executeSql({ tables, query: query.replace(') n', ' limit 1) n') }))
        expect(await collect(executeSql({ tables, query }))).toEqual(expected)
        if (aggregate.startsWith('count')) expect(expected).toEqual([{ n: 1 }])
      }
    }
  })

  it('preserves composite number and bigint key groups', async () => {
    const keys = [1, 1n]
    for (const values of [keys, [...keys].reverse()]) {
      const tables = { c: [{ k: '1.0', j: 1 }], r: values.map(k => ({ k, j: 1 })) }
      const query = 'select (select count(*) from r where r.k = c.k and r.j = c.j) n from c'
      expect(await collect(executeSql({ tables, query }))).toEqual([{ n: 1 }])
      expect(await collect(executeSql({ tables, query: query.replace(') n', ' limit 1) n') }))).toEqual([{ n: 1 }])
    }
  })

  it('keeps null keys distinct from their serialized strings', async () => {
    /** @type {null} */
    const key = null
    const text = 'NULL'
    for (const keys of [[key, text], [text, key]]) {
      const tables = { c: [{ k: text }], r: keys.map(k => ({ k })) }
      for (const aggregate of ['count(*)', 'count(*) + 0']) {
        const query = `select (select ${aggregate} from r where r.k = c.k) n from c`
        expect(await collect(executeSql({ tables, query }))).toEqual([{ n: 1 }])
        expect(await collect(executeSql({ tables, query: query.replace(') n', ' limit 1) n') }))).toEqual([{ n: 1 }])
      }
    }
  })

  it('keeps object keys distinct from their serialized strings', async () => {
    const key = { id: 1 }
    const text = '{"id":1}'
    for (const keys of [[key, text], [text, key]]) {
      const tables = { c: [{ k: text }], r: keys.map(k => ({ k })) }
      for (const aggregate of ['count(*)', 'count(*) + 0']) {
        const query = `select (select ${aggregate} from r where r.k = c.k) n from c`
        expect(await collect(executeSql({ tables, query }))).toEqual([{ n: 1 }])
        expect(await collect(executeSql({ tables, query: query.replace(') n', ' limit 1) n') }))).toEqual([{ n: 1 }])
      }
    }
  })

  it('keeps array keys distinct from their serialized strings', async () => {
    const key = [1]
    const text = '[1]'
    for (const keys of [[key, text], [text, key]]) {
      const tables = { c: [{ k: text }], r: keys.map(k => ({ k })) }
      for (const aggregate of ['count(*)', 'count(*) + 0']) {
        const query = `select (select ${aggregate} from r where r.k = c.k) n from c`
        expect(await collect(executeSql({ tables, query }))).toEqual([{ n: 1 }])
        expect(await collect(executeSql({ tables, query: query.replace(') n', ' limit 1) n') }))).toEqual([{ n: 1 }])
      }
    }
  })

  it.each([{ id: 1 }, [1]])('preserves identity for object correlation keys %s', async a => {
    const b = Array.isArray(a) ? [...a] : { ...a }
    const tables = { c: [{ k: a }, { k: b }], r: [{ k: a, x: 2 }, { k: b, x: 8 }] }
    const query = 'select (select sum(x) from r where r.k = c.k) n from c'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ n: 2 }, { n: 8 }])
    expect(await collect(executeSql({ tables, query: query.replace(') n', ' limit 1) n') }))).toEqual([{ n: 2 }, { n: 8 }])
  })

  it('does not collide with user columns', async () => {
    const query = 'select c.__scalar_0, (select count(*) from r where r.k = c.k) n from c'
    expect(await collect(executeSql({ tables: { ...tables, c: typedSource([{ k: 1, __scalar_0: 99 }]) }, query }))).toEqual([{ __scalar_0: 99, n: 2 }])
  })

  it('optimizes WHERE below an outer aggregate', async () => {
    const query = 'select count(*) n from c where (select count(*) from r where r.k = c.k) = 0'
    expect(JSON.stringify(planSql({ query, tables }))).toContain('"SemiJoin","test":"scalar"')
    expect(await collect(executeSql({ tables, query }))).toEqual([{ n: 2 }])
  })

  it('rebuilds lookup state for each result iteration', async () => {
    const result = executeSql({ tables, query: 'select (select count(*) from r where r.k = c.k) n from c' })
    expect(await collect(result)).toEqual(await collect(result))
  })

  it('does not shadow aggregate input columns with synthetic key aliases', async () => {
    const query = 'select (select sum(__exists_key_0) from r where r.k = c.k) n from c'
    expect(await collect(executeSql({ tables: { c: typedSource([{ k: 1 }]), r: typedSource([{ k: 1, __exists_key_0: 42 }]) }, query }))).toEqual([{ n: 42 }])
  })

  it('decorrelates unknown and differing key types', async () => {
    const query = 'select (select count(*) from r where r.k = c.k) n from c'
    const unknown = { c: memorySource({ data: [{ k: 1 }] }), r: memorySource({ data: [{ k: '1' }] }) }
    const different = { c: typedSource([{ k: 1 }]), r: typedSource([{ k: '1' }], { type: 'string' }) }
    for (const tables of [unknown, different]) {
      expect(JSON.stringify(planSql({ query, tables }))).toContain('"SemiJoin","test":"scalar"')
      expect(await collect(executeSql({ query, tables }))).toEqual([{ n: 1 }])
    }
  })

  it('evaluates per row only for keys that equal several groups', async () => {
    const c = [{ k: 1 }, { k: 2 }, { k: 3 }]
    const source = memorySource({ data: [{ k: 1 }, { k: '1' }, { k: 2 }, { k: 2 }] })
    let scans = 0
    /** @type {AsyncDataSource} */
    const r = { ...source, scan(options) { scans++; return source.scan(options) } }
    const query = 'select (select count(*) from r where r.k = c.k) n from c'
    expect(await collect(executeSql({ tables: { c, r }, query }))).toEqual([{ n: 2 }, { n: 2 }, { n: 0 }])
    // One grouped build, plus one per-row scan for the key that matches 1 and '1'.
    expect(scans).toEqual(2)
  })

  it('keeps outer row order and duplicates through the grouped join', async () => {
    const tables = { c: typedSource([{ k: 2 }, { k: 1 }, { k: 3 }, { k: 1 }]), r: typedSource(data.r) }
    const query = 'select c.k, (select count(*) from r where r.k = c.k) n from c limit 3'
    expect(await collect(executeSql({ query, tables }))).toEqual([{ k: 2, n: 1 }, { k: 1, n: 2 }, { k: 3, n: 0 }])
  })

  it('does not match NaN correlation keys', async () => {
    const tables = { c: typedSource([{ k: NaN }]), r: typedSource([{ k: NaN }]) }
    expect(await collect(executeSql({ tables, query: 'select (select count(*) from r where r.k = c.k) n from c' }))).toEqual([{ n: 0 }])
  })

  it.each([
    ['a CTE outer', 'with u as (select distinct k from c) select u.k, (select count(*) from r where r.k = u.k) n from u'],
    ['a derived outer', 'select u.k, (select count(*) from r where r.k = u.k) n from (select distinct k from c) u'],
    ['a grouped CTE', 'with u as (select k, count(*) rows from c group by k) select u.k, (select sum(x) from r where r.k = u.k) n from u'],
    ['a CTE inner', 'with s as (select k, x from r where j = 1) select c.k, (select count(*) from s where s.k = c.k) n from c'],
    ['a joined outer', 'with u as (select k from c) select u.k, (select count(*) from r where r.k = u.k) n from u join c d on d.k = u.k'],
    ['a computed column', 'with u as (select k + 0 as k from c) select (select count(*) from r where r.k = u.k) n from u'],
    ['a set operation', 'with u as (select k from c union all select k from r) select (select count(*) from r where r.k = u.k) n from u'],
  ])('decorrelates through %s', async (_, query) => {
    expect(JSON.stringify(planSql({ query, tables }))).toContain('"SemiJoin","test":"scalar"')
    // LIMIT on the scalar aggregate prevents decorrelation without changing results.
    const fallback = query.replace(/(\w\.k = \w\.\w+)\)/, '$1 limit 1)')
    expect(fallback).not.toEqual(query)
    expect(await collect(executeSql({ tables, query }))).toEqual(await collect(executeSql({ tables, query: fallback })))
  })

  it('counts through a CTE outer', async () => {
    const query = 'with u as (select distinct k from c) select u.k, (select count(*) from r where r.k = u.k) n from u'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 1, n: 2 }, { k: 2, n: 1 }, { k: 3, n: 0 }, { k: null, n: 0 }])
  })

  it('scans the inner table once for a CTE outer', async () => {
    const source = tables.r
    let scans = 0
    /** @type {AsyncDataSource} */
    const r = { ...source, scan(options) { scans++; return source.scan(options) } }
    const query = 'with u as (select k from c) select (select count(*) from r where r.k = u.k) n from u'
    expect((await collect(executeSql({ tables: { ...tables, r }, query }))).map(row => row.n)).toEqual([2, 1, 0, 0])
    expect(scans).toEqual(1)
  })


})

describe('uncorrelated scalar aggregate joins', () => {
  /**
   * @param {AsyncDataSource} source
   * @returns {{ source: AsyncDataSource, scans: ScanOptions[] }}
   */
  function counted(source) {
    /** @type {ScanOptions[]} */
    const scans = []
    return { scans, source: { ...source, scan(options) { scans.push(options); return source.scan(options) } } }
  }

  it('scans the subquery table once per subquery', async () => {
    const { source: r, scans } = counted(tables.r)
    const query = 'select c.k from c where c.k between (select min(x) from r) - 3 and (select max(x) from r) - 8'
    expect(await collect(executeSql({ tables: { ...tables, r }, query }))).toEqual([{ k: 1 }])
    expect(scans).toHaveLength(2)
  })

  it('evaluates in projections and ORDER BY', async () => {
    const query = 'select c.k, (select max(x) from r) m from c where c.k is not null order by (select count(*) from r) * c.k desc limit 2'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 3, m: 9 }, { k: 2, m: 9 }])
  })

  it('returns the empty aggregate value when the subquery matches nothing', async () => {
    const query = 'select c.k, (select count(*) from r where r.x > 100) n, (select max(x) from r where r.x > 100) m from c where c.k = 1'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 1, n: 0, m: null }])
  })

  it('filters below an outer aggregate', async () => {
    const query = 'select count(*) n from c where c.k < (select max(k) from r) + 1'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ n: 2 }])
  })

  it('works inside a correlated subquery', async () => {
    const query = 'select c.k, (select count(*) from r where r.k = c.k and r.x >= (select max(x) from r) - 3) n from c'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 1, n: 1 }, { k: 2, n: 0 }, { k: 3, n: 0 }, { k: null, n: 0 }])
  })

  it.each([
    ['select c.k from c where c.k = (select max(x) from r group by j having max(x) = 9) - 8', [{ k: 1 }]],
    ['select c.k from c where c.k < (select max(x) from r limit 1) - 6', [{ k: 1 }, { k: 2 }]],
  ])('scans grouped and limited subqueries once: %s', async (query, expected) => {
    const { source: r, scans } = counted(tables.r)
    expect(await collect(executeSql({ tables: { ...tables, r }, query }))).toEqual(expected)
    expect(scans).toHaveLength(1)
  })
})
