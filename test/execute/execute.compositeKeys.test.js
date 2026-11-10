import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

const tables = {
  t: [{ k: 'a|b', j: 'c' }, { k: 'a', j: 'b|c' }, { k: 'a', j: 'b|c' }],
  u: [{ k: 'a', j: 'b|c' }],
  v: [{ k: 1, j: 'x' }, { k: '1', j: 'x' }, { k: null, j: 'x' }, { k: 'NULL', j: 'x' }],
}

describe('composite keys', () => {
  it('groups by tuples that split differently around a delimiter', async () => {
    const query = 'select k, j, count(*) n from t group by k, j'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 'a|b', j: 'c', n: 1 }, { k: 'a', j: 'b|c', n: 2 }])
  })

  it('keeps distinct tuples that split differently around a delimiter', async () => {
    const query = 'select distinct k, j from t'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 'a|b', j: 'c' }, { k: 'a', j: 'b|c' }])
  })

  it('joins on every key of a tuple', async () => {
    const query = 'select t.k, t.j from t join u on t.k = u.k and t.j = u.j'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 'a', j: 'b|c' }, { k: 'a', j: 'b|c' }])
  })

  it.each([[1, '1'], [true, 1], [1, '01']])('joins coercively equal composite keys %s and %s', async (a, b) => {
    const tables = { t: [{ k: a, j: 'x' }], u: [{ k: b, j: 'x' }] }
    const query = 'select t.k from t join u on t.k = u.k and t.j = u.j'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: a }])
  })

  it('partitions windows by tuples that split differently around a delimiter', async () => {
    const query = 'select k, j, row_number() over (partition by k, j order by k) r from t'
    expect(await collect(executeSql({ tables, query }))).toEqual([{ k: 'a|b', j: 'c', r: 1 }, { k: 'a', j: 'b|c', r: 1 }, { k: 'a', j: 'b|c', r: 2 }])
  })

  it('separates numbers and NULL from strings with the same text', async () => {
    const query = 'select k, j, count(*) n from v group by k, j'
    expect(await collect(executeSql({ tables, query }))).toEqual([
      { k: 1, j: 'x', n: 1 }, { k: '1', j: 'x', n: 1 }, { k: null, j: 'x', n: 1 }, { k: 'NULL', j: 'x', n: 1 },
    ])
  })
})
