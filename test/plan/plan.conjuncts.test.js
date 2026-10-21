import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { collect, executeSql } from '../../src/index.js'
import { parseSql } from '../../src/parse/parse.js'
import { planSql } from '../../src/plan/plan.js'

describe('independent join conjunct pushdown', () => {
  const data = [
    { id: 1, text: 'command not found', flag: true },
    { id: 2, text: 'ok', flag: false },
    { id: 3, text: null, flag: true },
  ]
  const tables = { items: memorySource({ data }) }

  it.each([
    'r.text LIKE \'%command not found%\'',
    'COALESCE(r.text, \'\') = \'command not found\'',
    'LENGTH(r.text) > 1',
    'CAST(r.id AS STRING) = \'1\'',
    'r.text IS NULL',
    'r.text IN (\'command not found\', \'ok\')',
    '(r.text = \'ok\' OR r.text IS NULL)',
  ])('pushes %s independently while retaining cross-side predicates', async predicate => {
    const query = `SELECT r.id FROM items r JOIN items c ON r.id = c.id WHERE c.flag = true AND ${predicate} AND r.id <= c.id`
    const parsed = parseSql({ query })
    const plan = planSql({ query: parsed, tables })
    if (parsed.type !== 'select' || parsed.where?.type !== 'binary' || parsed.where.left.type !== 'binary') throw new Error('expected conjuncts')
    if (plan.type !== 'Project' || plan.child.type !== 'Filter' || plan.child.child.type !== 'HashJoin') throw new Error('expected filtered join')
    const join = plan.child.child
    if (join.left.type !== 'Scan' || join.right.type !== 'Scan') throw new Error('expected scans')
    expect(plan.child.condition).toEqual(parsed.where.right)
    expect(join.left.hints.where).toEqual(parsed.where.left.right)
    expect(join.right.hints.where).toEqual(parsed.where.left.left)
    // The derived-table form cannot use this optimization and is a result oracle.
    const reference = query.replace('FROM items r', 'FROM (SELECT * FROM items) r')
    expect(await collect(executeSql({ query, tables }))).toEqual(await collect(executeSql({ query: reference, tables })))
  })

  it('pushes an unqualified reference when only one input has the column', async () => {
    const tables = {
      a: memorySource({ data: [{ id: 1 }] }),
      b: memorySource({ data: [{ id: 1, text: 'ok' }] }),
    }
    const query = 'SELECT a.id FROM a JOIN b ON a.id = b.id WHERE text LIKE \'%ok%\''
    const plan = planSql({ query, tables })
    if (plan.type !== 'Project' || plan.child.type !== 'HashJoin' || plan.child.right.type !== 'Scan') throw new Error('expected join')
    const parsed = parseSql({ query })
    if (parsed.type !== 'select') throw new Error('expected select')
    expect(plan.child.right.hints.where).toEqual(parsed.where)
    expect(await collect(executeSql({ query, tables }))).toEqual([{ id: 1 }])
  })

  it('keeps an ambiguous conjunct without blocking resolved predicates', () => {
    const query = 'SELECT r.id FROM items r JOIN items c ON r.id = c.id WHERE flag = true AND c.id > 1'
    const plan = planSql({ query, tables })
    if (plan.type !== 'Project' || plan.child.type !== 'Filter' || plan.child.child.type !== 'HashJoin') throw new Error('expected filtered join')
    const join = plan.child.child
    if (join.left.type !== 'Scan' || join.right.type !== 'Scan') throw new Error('expected scans')
    expect(join.left.hints.where).toBeUndefined()
    const parsed = parseSql({ query })
    if (parsed.type !== 'select' || parsed.where?.type !== 'binary') throw new Error('expected conjuncts')
    expect(join.right.hints.where).toEqual(parsed.where.right)
    expect(plan.child.condition).toEqual(parsed.where.left)
  })

  it.each([
    '(r.flag = true OR c.flag = true)',
    'LENGTH(r.text) > c.id',
    '(SELECT LENGTH(42)) > 0',
  ])('pushes WHERE independently of ON residual %s', residual => {
    const query = `SELECT r.id FROM items r JOIN items c ON r.id = c.id AND ${residual} WHERE r.text LIKE '%command%'`
    const plan = planSql({ query, tables })
    if (plan.type !== 'Project' || plan.child.type !== 'HashJoin' || plan.child.left.type !== 'Scan') throw new Error('expected join')
    expect(plan.child.residual?.type).toBe('binary')
    expect(plan.child.left.hints.where?.type).toBe('binary')
  })
})
