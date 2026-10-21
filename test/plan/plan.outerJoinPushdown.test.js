import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { collect, executePlan, executeSql, planSql } from '../../src/index.js'
import { parseSql } from '../../src/parse/parse.js'

/**
 * @import { HashJoinNode, QueryPlan } from '../../src/plan/types.js'
 */

const tables = {
  a: memorySource({ data: [{ id: 1, flag: true }, { id: 2, flag: false }, { id: 3, flag: true }, { id: null, flag: true }] }),
  b: memorySource({ data: [{ id: 1, flag: true }, { id: 2, flag: true }, { id: 4, flag: false }, { id: null, flag: true }] }),
  c: memorySource({ data: [{ id: 1, flag: true }, { id: 2, flag: false }] }),
}

/**
 * @param {QueryPlan} plan
 * @returns {HashJoinNode}
 */
function joinOf(plan) {
  if (plan.type === 'Project' || plan.type === 'Filter' || plan.type === 'Sort') return joinOf(plan.child)
  if (plan.type !== 'HashJoin') throw new Error('expected hash join')
  return plan
}

describe('outer join predicate pushdown', () => {
  it.each(['LEFT', 'RIGHT'])('pushes preserved WHERE and nonpreserved ON for %s', joinType => {
    const preserved = joinType === 'LEFT' ? 'a' : 'b'
    const nullable = joinType === 'LEFT' ? 'b' : 'a'
    const query = `SELECT a.id, b.id FROM a ${joinType} JOIN b ON a.id = b.id AND ${nullable}.flag = true WHERE ${preserved}.flag = true`
    const ast = parseSql({ query })
    const original = structuredClone(ast)
    const plan = planSql({ tables, query: ast })
    const join = joinOf(plan)
    expect(join.joinType).toBe(joinType)
    expect(join.residual).toBeUndefined()
    if (join.left.type !== 'Scan' || join.right.type !== 'Scan') throw new Error('expected scans')
    expect(join.left.hints.where?.type).toBe('binary')
    expect(join.right.hints.where?.type).toBe('binary')
    expect(ast).toEqual(original)
    expect(planSql({ tables, query: ast })).toEqual(plan)
  })

  it.each(['LEFT', 'RIGHT'])('keeps preserved-side ON predicates for %s', joinType => {
    const side = joinType === 'LEFT' ? 'a' : 'b'
    const join = joinOf(planSql({ tables, query: `SELECT a.id FROM a ${joinType} JOIN b ON a.id = b.id AND ${side}.flag = true` }))
    expect(join.residual?.type).toBe('binary')
    const input = joinType === 'LEFT' ? join.left : join.right
    if (input.type !== 'Scan') throw new Error('expected scan')
    expect(input.hints.where).toBeUndefined()
  })

  it.each(['b.flag IS NULL', 'COALESCE(b.flag, true) = true', 'b.flag = true OR b.flag IS NULL', 'NOT (b.flag IS NOT NULL)', 'CASE WHEN b.flag IS NULL THEN true ELSE b.flag END = true', 'ABS(b.id) = 1', 'a.flag = true OR b.flag = true'])('retains non-null-rejecting WHERE: %s', where => {
    const plan = planSql({ tables, query: `SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE ${where}` })
    if (plan.type !== 'Project') throw new Error('expected project')
    expect(plan.child.type).toBe('Filter')
    const join = joinOf(plan)
    expect(join.joinType).toBe('LEFT')
    if (join.right.type !== 'Scan') throw new Error('expected scan')
    expect(join.right.hints.where).toBeUndefined()
  })

  it.each(['b.flag = true', 'b.flag IS NOT NULL', 'b.flag LIKE \'true\'', 'b.flag = true OR b.id > 1', 'b.id IN (1, 2)', 'b.id NOT IN (2, 4)', 'b.id BETWEEN 1 AND 2', 'b.flag NOT LIKE \'false\''])('reduces LEFT to INNER for null rejection: %s', where => {
    const join = joinOf(planSql({ tables, query: `SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE ${where}` }))
    expect(join.joinType).toBe('INNER')
    if (join.right.type !== 'Scan') throw new Error('expected scan')
    expect(join.right.hints.where).toBeDefined()
  })

  it.each([
    ['RIGHT', 'a.flag = true', 'INNER'],
    ['RIGHT', 'a.id IN (1, 2)', 'INNER'],
    ['FULL', 'a.id IN (1, 2)', 'LEFT'],
    ['FULL', 'b.id IN (1, 2)', 'RIGHT'],
    ['FULL', 'a.id IN (1, 2) AND b.id IN (1, 2)', 'INNER'],
    ['RIGHT', 'a.flag IS NULL', 'RIGHT'],
    ['FULL', 'a.flag IS NULL', 'FULL'],
    ['FULL', 'a.flag = true', 'LEFT'],
    ['FULL', 'b.flag = true', 'RIGHT'],
    ['FULL', 'a.flag = true AND b.flag = true', 'INNER'],
  ])('reduces %s with %s to %s', (kind, where, expected) => {
    const join = joinOf(planSql({ tables, query: `SELECT a.id FROM a ${kind} JOIN b ON a.id = b.id WHERE ${where}` }))
    expect(join.joinType).toBe(expected)
    if (expected === 'FULL') {
      if (join.left.type !== 'Scan' || join.right.type !== 'Scan') throw new Error('expected scans')
      expect(join.left.hints.where).toBeUndefined()
      expect(join.right.hints.where).toBeUndefined()
    }
  })

  it.each(['LEFT', 'RIGHT', 'FULL'])('converts %s for a comparison between sides', kind => {
    const join = joinOf(planSql({ tables, query: `SELECT a.id FROM a ${kind} JOIN b ON a.id = b.id WHERE a.flag = b.flag` }))
    expect(join.joinType).toBe('INNER')
  })

  it('cascades conversion through a newly inner join', () => {
    const query = 'SELECT a.id FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id WHERE c.flag = true'
    const top = joinOf(planSql({ tables, query }))
    expect(top.joinType).toBe('INNER')
    expect(joinOf(top.left).joinType).toBe('INNER')
  })

  it.each(['=', '>'])('matches reference rows for conversion predicates (%s)', async op => {
    for (const kind of ['LEFT', 'RIGHT', 'FULL']) {
      for (const where of [
        'b.id = 1', 'b.id <> 1', 'b.id < 2', 'b.id > 1', 'b.id <= 2', 'b.id >= 1',
        'b.flag LIKE \'true\'', 'b.flag NOT LIKE \'false\'', 'b.flag IS NOT NULL',
        'b.id IN (1, 2)', 'b.id NOT IN (2, NULL)', 'b.id BETWEEN 1 AND 2',
        'b.id NOT BETWEEN 1 AND 2', 'b.flag = true OR b.id > 1',
        'b.flag IS NULL', 'COALESCE(b.flag, true) = true',
        'CASE WHEN b.flag IS NULL THEN true ELSE b.flag END = true',
        'b.flag = true OR b.flag IS NULL', 'NOT (b.flag IS NOT NULL)',
        'a.flag = b.flag', 'a.id IN (1, 2)', 'a.id IN (1, 2) AND b.id IN (1, 2)',
        'a.flag = true OR b.flag = true', 'b.id = a.id OR b.flag = true',
      ]) {
        const query = `SELECT a.id AS aid, b.id AS bid FROM a ${kind} JOIN b ON a.id ${op} b.id WHERE ${where} ORDER BY a.id, b.id`
        expect(await collect(executeSql({ tables, query }))).toEqual(await collect(executePlan({ plan: planSql({ query }), context: { tables } })))
      }
    }
  })

  it.each([
    ['c.flag = true', 'INNER', 'INNER'],
    ['c.flag = true OR c.id = b.id', 'INNER', 'INNER'],
    ['b.flag = c.flag', 'INNER', 'INNER'],
    ['b.flag IS NULL', 'LEFT', 'LEFT'],
    ['c.flag IS NULL', 'LEFT', 'LEFT'],
    ['COALESCE(c.flag, true) = true', 'LEFT', 'LEFT'],
  ])('preserves chained join results for %s', async (where, upperKind, lowerKind) => {
    const query = `SELECT a.id AS aid, b.id AS bid, c.id AS cid FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id WHERE ${where} ORDER BY a.id, b.id, c.id`
    const upper = joinOf(planSql({ tables, query }))
    expect(upper.joinType).toBe(upperKind)
    expect(joinOf(upper.left).joinType).toBe(lowerKind)
    expect(await collect(executeSql({ tables, query }))).toEqual(await collect(executePlan({ plan: planSql({ query }), context: { tables } })))
  })

  it.each(['LEFT', 'RIGHT', 'FULL'])('keeps ON-only rejection from converting %s', async kind => {
    const query = `SELECT a.id AS aid, b.id AS bid FROM a ${kind} JOIN b ON a.id = b.id AND a.flag IS NOT NULL AND b.flag IS NOT NULL ORDER BY a.id, b.id`
    expect(joinOf(planSql({ tables, query })).joinType).toBe(kind)
    expect(await collect(executeSql({ tables, query }))).toEqual(await collect(executePlan({ plan: planSql({ query }), context: { tables } })))
  })

  it('filters derived inputs outside their LIMIT while still pushing into the other scan', () => {
    const plan = planSql({ tables, query: 'SELECT a.id FROM a LEFT JOIN (SELECT * FROM b LIMIT 1) b ON a.id = b.id AND b.flag = true WHERE a.flag = true' })
    const join = joinOf(plan)
    if (join.left.type !== 'Scan') throw new Error('expected scan')
    expect(join.left.hints.where).toBeDefined()
    expect(join.right.type).toBe('Filter')
    if (join.right.type !== 'Filter' || join.right.child.type !== 'Subquery') throw new Error('expected filtered subquery')
    const scan = join.right.child.child
    if (scan.type !== 'Scan') throw new Error('expected limited scan')
    expect(scan.hints.limit).toBe(1)
    expect(scan.hints.where).toBeUndefined()
  })

  it('pushes through a multi-join chain while keeping nullable filters above the earlier outer join', () => {
    const query = 'SELECT a.id FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id AND c.flag = true WHERE a.flag = true AND b.flag IS NULL'
    const top = joinOf(planSql({ tables, query }))
    expect(top.left.type).toBe('Filter')
    const lower = joinOf(top.left)
    if (lower.left.type !== 'Scan' || lower.right.type !== 'Scan' || top.right.type !== 'Scan') throw new Error('expected scans')
    expect(lower.joinType).toBe('LEFT')
    expect(lower.left.hints.where).toBeDefined()
    expect(lower.right.hints.where).toBeUndefined()
    expect(top.right.hints.where).toBeDefined()
  })

  it('keeps unmatched rows NULL-extended when preserved-side ON is false', async () => {
    expect(await collect(executeSql({ tables, query: 'SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN b ON a.id = b.id AND a.flag = true ORDER BY a.id' }))).toEqual([
      { aid: null, bid: null }, { aid: 1, bid: 1 }, { aid: 2, bid: null }, { aid: 3, bid: null },
    ])
  })

  it('does not mutate a shared CTE or filter before a derived LIMIT', async () => {
    expect(await collect(executeSql({ tables, query: 'WITH x AS (SELECT * FROM b) SELECT a.id AS aid, y.id AS bid FROM a LEFT JOIN x ON a.id = x.id AND x.flag = false LEFT JOIN x y ON a.id = y.id WHERE a.id = 1' }))).toEqual([{ aid: 1, bid: 1 }])
    expect(await collect(executeSql({ tables, query: 'SELECT a.id AS aid, b.id AS bid FROM a LEFT JOIN (SELECT * FROM b LIMIT 1) b ON a.id = b.id AND b.id = 2 WHERE a.id = 2' }))).toEqual([{ aid: 2, bid: null }])
  })

  it.each(['=', '>'])('retains the schema when filters empty a join input (%s)', async op => {
    for (const kind of ['LEFT', 'RIGHT', 'FULL']) {
      const query = `SELECT a.id AS aid, b.id AS bid FROM a ${kind} JOIN b ON a.id ${op} b.id AND a.id > 100 AND b.id > 100 ORDER BY a.id, b.id`
      expect(await collect(executeSql({ tables, query }))).toEqual(await collect(executePlan({ plan: planSql({ query }), context: { tables } })))
    }
  })

  it.each(['LEFT', 'RIGHT', 'FULL', 'INNER'])('matches unoptimized results for %s across ON and WHERE combinations', async kind => {
    for (const on of ['', ' AND a.flag = true', ' AND b.flag = false', ' AND a.flag = false AND b.flag = false']) {
      for (const where of ['', 'a.flag = true', 'b.flag = true', 'b.flag IS NULL', 'a.flag IS NULL', 'COALESCE(b.flag, true) = true', 'a.flag = true AND b.flag = false']) {
        const query = `SELECT a.id AS aid, b.id AS bid FROM a ${kind} JOIN b ON a.id = b.id${on}${where ? ` WHERE ${where}` : ''} ORDER BY a.id, b.id`
        // Omitting metadata disables predicate pushdown for the reference plan.
        const reference = planSql({ query })
        expect(await collect(executeSql({ tables, query }))).toEqual(await collect(executePlan({ plan: reference, context: { tables } })))
      }
    }
  })
})
