import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { executeSql } from '../../src/execute/execute.js'
import { collect } from '../../src/index.js'

// 10000 rows spans multiple 4000-row accumulation chunks, so these tests pin
// that group state carries across chunk boundaries in the streaming path.
const N = 10000
const data = new Array(N)
for (let i = 0; i < N; i++) {
  data[i] = { g: 'g' + i % 3, v: i % 100 }
}
const big = memorySource({ data })

describe('streaming aggregates', () => {
  it('accumulates grouped aggregates across chunk boundaries', async () => {
    const result = await collect(executeSql({
      tables: { big },
      query: 'SELECT g, COUNT(*) AS n, SUM(v) AS s, MIN(v) AS lo, MAX(v) AS hi FROM big GROUP BY g ORDER BY g',
    }))
    expect(result).toEqual([
      { g: 'g0', n: 3334, s: 165033, lo: 0, hi: 99 },
      { g: 'g1', n: 3333, s: 164967, lo: 0, hi: 99 },
      { g: 'g2', n: 3333, s: 165000, lo: 0, hi: 99 },
    ])
  })

  it('accumulates scalar aggregates across chunk boundaries', async () => {
    const result = await collect(executeSql({
      tables: { big },
      query: 'SELECT COUNT(*) AS n, AVG(v) AS a, COUNT(DISTINCT v) AS d FROM big',
    }))
    expect(result).toEqual([{ n: 10000, a: 49.5, d: 100 }])
  })

  it('applies FILTER clauses across chunk boundaries', async () => {
    const result = await collect(executeSql({
      tables: { big },
      query: 'SELECT COUNT(*) FILTER (WHERE v < 10) AS low, COUNT(*) FILTER (WHERE v >= 90) AS high FROM big',
    }))
    expect(result).toEqual([{ low: 1000, high: 1000 }])
  })

  it('evaluates HAVING and ORDER BY on aggregates not in the select list', async () => {
    const result = await collect(executeSql({
      tables: { big },
      query: 'SELECT g FROM big GROUP BY g HAVING COUNT(*) > 3333 ORDER BY SUM(v) DESC',
    }))
    expect(result).toEqual([{ g: 'g0' }])
  })

  it('evaluates expressions over aggregates from substituted values', async () => {
    const result = await collect(executeSql({
      tables: { big },
      query: 'SELECT g, SUM(v) / COUNT(*) AS mean, COUNT(*) + 1 AS n1 FROM big GROUP BY g ORDER BY g LIMIT 1',
    }))
    expect(result).toEqual([{ g: 'g0', mean: 49.5, n1: 3335 }])
  })

  it('streams GROUP BY with no aggregates in the select list', async () => {
    const result = await collect(executeSql({
      tables: { big },
      query: 'SELECT g FROM big GROUP BY g ORDER BY g',
    }))
    expect(result).toEqual([{ g: 'g0' }, { g: 'g1' }, { g: 'g2' }])
  })

  it('returns aggregate defaults for empty input', async () => {
    const empty = memorySource({ data: [], columns: ['v'] })
    const result = await collect(executeSql({
      tables: { empty },
      query: 'SELECT COUNT(*) AS n, SUM(v) AS s, MIN(v) AS lo FROM empty',
    }))
    expect(result).toEqual([{ n: 0, s: null, lo: null }])
  })

  it('falls back to buffered aggregation for non-streamable aggregates', async () => {
    const result = await collect(executeSql({
      tables: { big },
      query: 'SELECT g, MEDIAN(v) AS m, COUNT(*) AS n FROM big GROUP BY g ORDER BY g LIMIT 1',
    }))
    expect(result).toEqual([{ g: 'g0', m: 49.5, n: 3334 }])
  })

  it('falls back to buffered aggregation when a subquery appears in the select list', async () => {
    const result = await collect(executeSql({
      tables: { big, other: [{ x: 7 }] },
      query: 'SELECT (SELECT MAX(x) FROM other) AS mx, COUNT(*) AS n FROM big',
    }))
    expect(result).toEqual([{ mx: 7, n: 10000 }])
  })
})
