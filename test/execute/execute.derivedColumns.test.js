import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('derived table columns the outer query does not read', () => {
  const b = [
    { id: 1, sid: 'a', keep: 1, v: 5 },
    { id: 2, sid: 'b', keep: 1, v: 4 },
    { id: 3, sid: 'c', keep: 1, v: 3 },
    { id: 1, sid: 'z', keep: 2, v: 1 },
  ]

  /**
   * @param {string} query
   * @returns {Promise<Record<string, any>[]>}
   */
  function run(query) {
    return collect(executeSql({ tables: { b }, query }))
  }

  it('skips an unread window function', async () => {
    expect(await run('SELECT s.id FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY keep ORDER BY v) AS r FROM b) s'))
      .toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 1 }])
  })

  it('keeps a window function read through a later alias', async () => {
    expect(await run('SELECT s.q FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY v) AS r, r + 1 AS q FROM b) s'))
      .toEqual([{ q: 5 }, { q: 4 }, { q: 3 }, { q: 2 }])
  })

  it('keeps a window function read by ORDER BY', async () => {
    expect(await run('SELECT s.id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY v) AS r FROM b ORDER BY r DESC LIMIT 2) s'))
      .toEqual([{ id: 1 }, { id: 2 }])
  })

  it('reads the source columns behind an unread alias', async () => {
    expect(await run('SELECT s.k FROM (SELECT v + 1 AS x, x + 1 AS y, y + 1 AS k FROM b) s'))
      .toEqual([{ k: 8 }, { k: 7 }, { k: 6 }, { k: 4 }])
    expect(await run('SELECT s.id FROM (SELECT id, v + 1 AS w FROM b ORDER BY w LIMIT 2) s'))
      .toEqual([{ id: 1 }, { id: 3 }])
    expect(await run('SELECT s.n FROM (SELECT keep AS g, g + 1 AS h, COUNT(*) AS n FROM b GROUP BY h) s'))
      .toEqual([{ n: 3 }, { n: 1 }])
  })

  it('keeps every DISTINCT column', async () => {
    expect(await run('SELECT s.id FROM (SELECT DISTINCT id, keep FROM b) s'))
      .toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 1 }])
  })
})

describe('select aliases that share a source column name', () => {
  const tables = { b: [{ id: 1, sid: 'a', v: 5 }, { id: 2, sid: 'b', v: 4 }] }

  it('reads the source column, not the earlier alias', async () => {
    expect(await collect(executeSql({ tables, query: 'SELECT id AS sid, sid AS id FROM b' })))
      .toEqual([{ sid: 1, id: 'a' }, { sid: 2, id: 'b' }])
    expect(await collect(executeSql({ tables, query: 'SELECT v AS id, id + 1 AS k FROM b' })))
      .toEqual([{ id: 5, k: 2 }, { id: 4, k: 3 }])
  })

  it('swaps columns inside a derived table', async () => {
    expect(await collect(executeSql({ tables, query: 'SELECT s.id FROM (SELECT id AS sid, sid AS id FROM b) s' })))
      .toEqual([{ id: 'a' }, { id: 'b' }])
    expect(await collect(executeSql({ tables, query: 'SELECT t.v FROM b t JOIN (SELECT id AS sid, sid AS id FROM b) s ON t.sid = s.id' })))
      .toEqual([{ v: 5 }, { v: 4 }])
  })

  it('still orders and groups by the alias', async () => {
    expect(await collect(executeSql({ tables, query: 'SELECT -v AS id FROM b ORDER BY id' })))
      .toEqual([{ id: -5 }, { id: -4 }])
  })
})
