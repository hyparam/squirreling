import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'
import { planSql } from '../../src/plan/plan.js'

/**
 * @import { HashAggregateNode, QueryPlan } from '../../src/plan/types.js'
 */

/**
 * Asserts the plan is a two-stage (eager) aggregate and returns both stages.
 *
 * @param {QueryPlan} plan
 * @returns {{ outer: HashAggregateNode, inner: HashAggregateNode }}
 */
function expectEager(plan) {
  if (plan.type !== 'HashAggregate') throw new Error(`expected HashAggregate, got ${plan.type}`)
  const outer = plan
  if (outer.child.type !== 'HashAggregate') throw new Error(`expected eager rewrite, got ${outer.child.type} child`)
  const inner = outer.child
  return { outer, inner }
}

/**
 * Asserts the plan is a single-stage aggregate (the rewrite did not fire).
 *
 * @param {QueryPlan} plan
 */
function expectSingleStage(plan) {
  if (plan.type !== 'HashAggregate') throw new Error(`expected HashAggregate, got ${plan.type}`)
  expect(plan.child.type).not.toBe('HashAggregate')
}

describe('eager aggregation rewrite', () => {
  describe('plan shape', () => {
    it('rewrites GROUP BY over a wrapped string function into two stages', () => {
      const plan = planSql({
        query: 'SELECT substr(regexp_replace(text, \'\\s+\', \' \'), 1, 150) AS k, COUNT(*) AS n FROM t GROUP BY k',
      })
      const { outer, inner } = expectEager(plan)
      expect(inner.groupBy).toMatchObject([{ type: 'identifier', name: 'text' }])
      expect(inner.columns).toMatchObject([
        { alias: 'text', expr: { type: 'identifier', name: 'text' } },
        { alias: '__eager_agg_0', expr: { type: 'function', funcName: 'COUNT' } },
      ])
      expect(outer.groupBy).toMatchObject([{ type: 'function', funcName: 'substr' }])
      expect(outer.columns).toMatchObject([
        { alias: 'k' },
        { alias: 'n', expr: { type: 'function', funcName: 'SUM', args: [{ type: 'identifier', name: '__eager_agg_0' }] } },
      ])
    })

    it('rewrites a string cast of a column', () => {
      const plan = planSql({
        query: 'SELECT substr(CAST(payload AS VARCHAR), 1, 5) AS k, COUNT(*) AS n FROM t GROUP BY k',
      })
      const { inner } = expectEager(plan)
      expect(inner.groupBy).toMatchObject([{ type: 'identifier', name: 'payload' }])
    })

    it('keeps an alias-only ORDER BY on the merge stage', () => {
      const plan = planSql({
        query: 'SELECT upper(text) AS k, COUNT(*) AS n FROM t GROUP BY k ORDER BY n DESC',
      })
      const { outer } = expectEager(plan)
      expect(outer.orderBy).toMatchObject([{ expr: { type: 'identifier', name: 'n' }, direction: 'DESC' }])
    })

    it('does not rewrite a bare column key', () => {
      expectSingleStage(planSql({ query: 'SELECT text, COUNT(*) FROM t GROUP BY text' }))
    })

    it('does not rewrite a key without a string-producing construct', () => {
      expectSingleStage(planSql({ query: 'SELECT id % 10 AS k, COUNT(*) FROM t GROUP BY k' }))
    })

    it('does not rewrite when the key references two columns', () => {
      expectSingleStage(planSql({ query: 'SELECT upper(a || b) AS k, COUNT(*) FROM t GROUP BY k' }))
    })

    it('does not rewrite a qualified column', () => {
      expectSingleStage(planSql({ query: 'SELECT upper(t.text) AS k, COUNT(*) FROM t GROUP BY k' }))
    })

    it('does not rewrite with HAVING', () => {
      expectSingleStage(planSql({ query: 'SELECT upper(text) AS k, COUNT(*) AS n FROM t GROUP BY k HAVING COUNT(*) > 1' }))
    })

    it('does not rewrite unsplittable aggregates', () => {
      expectSingleStage(planSql({ query: 'SELECT upper(text) AS k, AVG(n) FROM t GROUP BY k' }))
      expectSingleStage(planSql({ query: 'SELECT upper(text) AS k, COUNT(DISTINCT id) FROM t GROUP BY k' }))
    })

    it('redirects ORDER BY over a selected aggregate to its output alias', () => {
      const plan = planSql({ query: 'SELECT upper(text) AS k, COUNT(*) AS n FROM t GROUP BY k ORDER BY COUNT(*) DESC' })
      const { outer } = expectEager(plan)
      expect(outer.orderBy).toMatchObject([{ expr: { type: 'identifier', name: 'n' }, direction: 'DESC' }])
    })

    it('does not rewrite when ORDER BY needs an unselected aggregate', () => {
      expectSingleStage(planSql({ query: 'SELECT upper(text) AS k, COUNT(*) AS n FROM t GROUP BY k ORDER BY SUM(id) DESC' }))
    })

    it('does not rewrite multi-key grouping', () => {
      expectSingleStage(planSql({ query: 'SELECT upper(text) AS k, id, COUNT(*) FROM t GROUP BY k, id' }))
    })
  })

  describe('execution equivalence', () => {
    // Repeated raw values, distinct raw values that normalize to the same
    // key, a NULL key, and NULLs under a counted column.
    const messages = [
      { id: 1, text: 'Hello  World', n: 10, v: 'x' },
      { id: 2, text: 'Hello World', n: 5, v: null },
      { id: 3, text: 'HELLO  WORLD', n: 2, v: 'y' },
      { id: 4, text: null, n: 1, v: null },
      { id: 5, text: 'Hello  World', n: 4, v: null },
    ]

    it('merges distinct raw values that normalize to one key', async () => {
      const result = await collect(executeSql({
        tables: { messages },
        query: 'SELECT lower(regexp_replace(text, \'\\s+\', \' \')) AS k, COUNT(*) AS c, SUM(n) AS total FROM messages GROUP BY k ORDER BY c DESC',
      }))
      expect(result).toEqual([
        { k: 'hello world', c: 4, total: 21 },
        { k: null, c: 1, total: 1 },
      ])
    })

    it('splits COUNT(column), MIN, and MAX correctly', async () => {
      const result = await collect(executeSql({
        tables: { messages },
        query: 'SELECT lower(regexp_replace(text, \'\\s+\', \' \')) AS k, COUNT(v) AS cv, MIN(id) AS mn, MAX(id) AS mx FROM messages GROUP BY k ORDER BY cv DESC',
      }))
      expect(result).toEqual([
        { k: 'hello world', cv: 2, mn: 1, mx: 5 },
        { k: null, cv: 0, mn: 4, mx: 4 },
      ])
    })

    it('produces the same rows through a CTE', async () => {
      const result = await collect(executeSql({
        tables: { messages },
        query: 'WITH s AS (SELECT * FROM messages) SELECT upper(text) AS k, COUNT(*) AS c FROM s GROUP BY k ORDER BY c DESC',
      }))
      expect(result).toEqual([
        { k: 'HELLO  WORLD', c: 3 },
        { k: 'HELLO WORLD', c: 1 },
        { k: null, c: 1 },
      ])
    })

    it('matches the single-stage result on an unrewritten shape', async () => {
      // HAVING blocks the rewrite; this pins the direct path as the oracle
      // for the same grouping so the two tests above compare like for like.
      const result = await collect(executeSql({
        tables: { messages },
        query: 'SELECT lower(regexp_replace(text, \'\\s+\', \' \')) AS k, COUNT(*) AS c, SUM(n) AS total FROM messages GROUP BY k HAVING COUNT(*) > 0 ORDER BY c DESC',
      }))
      expect(result).toEqual([
        { k: 'hello world', c: 4, total: 21 },
        { k: null, c: 1, total: 1 },
      ])
    })
  })
})
