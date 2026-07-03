import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { executeSql } from '../../src/execute/execute.js'
import { planStreamingAggregates } from '../../src/execute/streamingAggregate.js'
import { collect, planSql } from '../../src/index.js'

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

const sales = [
  { region: 'east', product: 'apple', amount: 100, qty: 1 },
  { region: 'east', product: 'banana', amount: 200, qty: 2 },
  { region: 'west', product: 'apple', amount: 150, qty: null },
  { region: 'west', product: 'apple', amount: null, qty: 4 },
  { region: 'south', product: 'cherry', amount: 50, qty: 5 },
]
const tables = { sales: memorySource({ data: sales }) }

describe('streaming aggregate row retention', () => {
  /**
   * @param {string} query
   * @returns {ReturnType<typeof planStreamingAggregates>}
   */
  function streamingPlan(query) {
    const plan = planSql({ query, tables })
    if (plan.type !== 'HashAggregate' && plan.type !== 'ScalarAggregate') {
      throw new Error(`expected aggregate plan, got ${plan.type}`)
    }
    return planStreamingAggregates(plan)
  }

  it('retains no rows when expressions only use group keys and aggregates', () => {
    const streaming = streamingPlan('SELECT region, count(*) FROM sales GROUP BY region HAVING count(*) > 1 ORDER BY sum(amount)')
    expect(streaming?.needsRow).toBe(false)
    expect(streaming?.keyRefs.size).toBe(1)
  })

  it('retains no rows for function group keys', () => {
    const streaming = streamingPlan('SELECT upper(region) AS r, count(*) FROM sales GROUP BY upper(region)')
    expect(streaming?.needsRow).toBe(false)
    expect(streaming?.keyRefs.size).toBe(1)
  })

  it('retains a representative row for columns outside the group keys', () => {
    const streaming = streamingPlan('SELECT product, count(*) FROM sales GROUP BY region')
    expect(streaming?.needsRow).toBe(true)
  })

  it('does not stream buffering aggregates like median', () => {
    expect(streamingPlan('SELECT region, median(amount) FROM sales GROUP BY region')).toBeUndefined()
  })

  it('does not stream array_agg', () => {
    expect(streamingPlan('SELECT array_agg(product) FROM sales')).toBeUndefined()
  })
})

describe('streaming aggregate results', () => {
  it('computes grouped aggregates with nulls', async () => {
    const result = await collect(executeSql({
      tables,
      query: `SELECT region, count(*) AS c, count(amount) AS ca, sum(amount) AS s, min(amount) AS mn, max(amount) AS mx
        FROM sales GROUP BY region ORDER BY region`,
    }))
    expect(result).toEqual([
      { region: 'east', c: 2, ca: 2, s: 300, mn: 100, mx: 200 },
      { region: 'south', c: 1, ca: 1, s: 50, mn: 50, mx: 50 },
      { region: 'west', c: 2, ca: 1, s: 150, mn: 150, mx: 150 },
    ])
  })

  it('computes grouped count distinct', async () => {
    const result = await collect(executeSql({
      tables,
      query: 'SELECT region, count(DISTINCT product) AS p FROM sales GROUP BY region ORDER BY region',
    }))
    expect(result).toEqual([
      { region: 'east', p: 2 },
      { region: 'south', p: 1 },
      { region: 'west', p: 1 },
    ])
  })

  it('computes multiple scalar count distinct', async () => {
    const result = await collect(executeSql({
      tables,
      query: 'SELECT count(DISTINCT region) AS r, count(DISTINCT product) AS p, count(DISTINCT qty) AS q FROM sales',
    }))
    expect(result).toEqual([{ r: 3, p: 3, q: 4 }])
  })

  it('keeps same-column COUNT and COUNT DISTINCT separate', async () => {
    const dup = memorySource({ data: [{ v: 1 }, { v: 1 }, { v: 2 }] })
    const result = await collect(executeSql({
      tables: { dup },
      query: 'SELECT count(v) AS a, count(DISTINCT v) AS b FROM dup',
    }))
    expect(result).toEqual([{ a: 3, b: 2 }])
  })

  it('evaluates expressions over finalized aggregates', async () => {
    const result = await collect(executeSql({
      tables,
      query: 'SELECT region, count(*) * 2 AS c2, round(avg(amount), 1) AS a FROM sales GROUP BY region ORDER BY region',
    }))
    expect(result).toEqual([
      { region: 'east', c2: 4, a: 150 },
      { region: 'south', c2: 2, a: 50 },
      { region: 'west', c2: 4, a: 150 },
    ])
  })

  it('groups by function expressions', async () => {
    const result = await collect(executeSql({
      tables,
      query: 'SELECT upper(region) AS r, count(*) AS c FROM sales GROUP BY upper(region) ORDER BY r',
    }))
    expect(result).toEqual([
      { r: 'EAST', c: 2 },
      { r: 'SOUTH', c: 1 },
      { r: 'WEST', c: 2 },
    ])
  })

  it('computes grouped countif', async () => {
    const result = await collect(executeSql({
      tables,
      query: 'SELECT region, countif(amount > 100) AS c FROM sales GROUP BY region ORDER BY region',
    }))
    expect(result).toEqual([
      { region: 'east', c: 1 },
      { region: 'south', c: 0 },
      { region: 'west', c: 1 },
    ])
  })

  it('orders groups by aggregate alias', async () => {
    const result = await collect(executeSql({
      tables,
      query: 'SELECT region, count(*) AS c FROM sales GROUP BY region ORDER BY c DESC, region',
    }))
    expect(result).toEqual([
      { region: 'east', c: 2 },
      { region: 'west', c: 2 },
      { region: 'south', c: 1 },
    ])
  })

  it('projects columns outside the group keys from a representative row', async () => {
    const result = await collect(executeSql({
      tables,
      query: 'SELECT region, product, count(*) AS c FROM sales GROUP BY region, product ORDER BY region, product',
    }))
    expect(result).toEqual([
      { region: 'east', product: 'apple', c: 1 },
      { region: 'east', product: 'banana', c: 1 },
      { region: 'south', product: 'cherry', c: 1 },
      { region: 'west', product: 'apple', c: 2 },
    ])
  })

  it('aggregates an empty filter result', async () => {
    const result = await collect(executeSql({
      tables,
      query: 'SELECT count(*) AS c, count(amount) AS ca, sum(amount) AS s, min(amount) AS m FROM sales WHERE amount > 1000',
    }))
    expect(result).toEqual([{ c: 0, ca: 0, s: null, m: null }])
  })
})
