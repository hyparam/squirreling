import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { planSql } from '../../src/plan/plan.js'

describe('column pushdown', () => {
  it('should push column hints through SELECT * subquery', () => {
    // Useful query pattern for efficient sampling of large tables
    const plan = planSql({ query: 'SELECT id FROM (SELECT * FROM users LIMIT 1000)' })
    expect(plan).toEqual({
      type: 'Project',
      columns: [
        {
          type: 'derived',
          expr: {
            type: 'identifier',
            name: 'id',
            positionStart: 7,
            positionEnd: 9,
          },
          positionStart: 7,
          positionEnd: 9,
        },
      ],
      child: {
        type: 'Subquery',
        scope: ['users'],
        child: {
          type: 'Scan',
          table: 'users',
          hints: {
            columns: ['id'],
            limit: 1000,
          },
        },
      },
    })
  })

  it('should prune unused subquery columns with aliases', () => {
    // Provide tables so that invalid column names would throw
    const users = memorySource({ data: [{ id: 1, name: 'Alice' }] })
    const plan = planSql({
      query: 'SELECT full_name FROM (SELECT id, name AS full_name FROM users)',
      tables: { users },
    })
    expect(plan).toEqual({
      type: 'Project',
      columns: [
        {
          type: 'derived',
          expr: {
            type: 'identifier',
            name: 'full_name',
            positionStart: 7,
            positionEnd: 16,
          },
          positionStart: 7,
          positionEnd: 16,
        },
      ],
      child: {
        type: 'Subquery',
        scope: ['users'],
        child: {
          type: 'Project',
          columns: [
            {
              type: 'derived',
              expr: {
                type: 'identifier',
                name: 'name',
                positionStart: 34,
                positionEnd: 38,
              },
              alias: 'full_name',
              positionStart: 34,
              positionEnd: 51,
            },
          ],
          child: {
            type: 'Scan',
            table: 'users',
            hints: {
              columns: ['name'],
            },
          },
        },
      },
    })
  })

  it('should not push SELECT alias references as scan columns', () => {
    const users = memorySource({ data: [{ id: 1, name: 'Alice' }] })
    const plan = planSql({
      query: 'SELECT id AS a, a + 1 AS b FROM users',
      tables: { users },
    })
    expect(plan).toEqual({
      type: 'Project',
      columns: [
        {
          type: 'derived',
          expr: {
            type: 'identifier',
            name: 'id',
            positionStart: 7,
            positionEnd: 9,
          },
          alias: 'a',
          positionStart: 7,
          positionEnd: 14,
        },
        {
          type: 'derived',
          expr: {
            type: 'binary',
            op: '+',
            left: {
              type: 'identifier',
              name: 'id',
              positionStart: 7,
              positionEnd: 9,
            },
            right: {
              type: 'literal',
              value: 1,
              positionStart: 20,
              positionEnd: 21,
            },
            positionStart: 16,
            positionEnd: 21,
          },
          alias: 'b',
          positionStart: 16,
          positionEnd: 26,
        },
      ],
      child: {
        type: 'Scan',
        table: 'users',
        hints: {
          columns: ['id'],
        },
      },
    })
  })

  it('should push column hints including subquery WHERE columns', () => {
    const plan = planSql({ query: 'SELECT id FROM (SELECT * FROM users WHERE age > 21)' })
    expect(plan).toEqual({
      type: 'Project',
      columns: [
        {
          type: 'derived',
          expr: {
            type: 'identifier',
            name: 'id',
            positionStart: 7,
            positionEnd: 9,
          },
          positionStart: 7,
          positionEnd: 9,
        },
      ],
      child: {
        type: 'Subquery',
        scope: ['users'],
        child: {
          type: 'Scan',
          table: 'users',
          hints: {
            columns: ['id', 'age'],
            where: {
              type: 'binary',
              op: '>',
              left: {
                type: 'identifier',
                name: 'age',
                positionStart: 42,
                positionEnd: 45,
              },
              right: {
                type: 'literal',
                value: 21,
                positionStart: 48,
                positionEnd: 50,
              },
              positionStart: 42,
              positionEnd: 50,
            },
          },
        },
      },
    })
  })

  it('should push per-table column hints to join scans', () => {
    const plan = planSql({ query: 'SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id' })
    expect(plan).toEqual({
      type: 'Project',
      columns: [
        {
          type: 'derived',
          expr: {
            type: 'identifier',
            name: 'name',
            prefix: 'users',
            positionStart: 7,
            positionEnd: 17,
          },
          positionStart: 7,
          positionEnd: 17,
        },
        {
          type: 'derived',
          expr: {
            type: 'identifier',
            name: 'total',
            prefix: 'orders',
            positionStart: 19,
            positionEnd: 31,
          },
          positionStart: 19,
          positionEnd: 31,
        },
      ],
      child: {
        type: 'HashJoin',
        joinType: 'INNER',
        leftAlias: 'users',
        rightAlias: 'orders',
        leftKeys: [{
          type: 'identifier',
          name: 'id',
          prefix: 'users',
          positionStart: 58,
          positionEnd: 66,
        }],
        rightKeys: [{
          type: 'identifier',
          name: 'user_id',
          prefix: 'orders',
          positionStart: 69,
          positionEnd: 83,
        }],
        left: {
          type: 'Scan',
          table: 'users',
          hints: { columns: ['name', 'id'] },
        },
        right: {
          type: 'Scan',
          table: 'orders',
          hints: { columns: ['total', 'user_id'] },
        },
      },
    })
  })

  it('should not push derived alias as scan column when parent asks for it', () => {
    // Regression: SELECT * alongside a derived alias (e.g. `*, a+b AS c`) must not
    // seed `c` as a scan hint — `c` is produced by projection, not by the source.
    const users = memorySource({ data: [{ a: 1, b: 2 }] })
    const plan = planSql({
      query: 'SELECT c FROM (SELECT *, a + b AS c FROM users)',
      tables: { users },
    })
    expect(plan).toEqual({
      type: 'Project',
      columns: [
        {
          type: 'derived',
          expr: { type: 'identifier', name: 'c', positionStart: 7, positionEnd: 8 },
          positionStart: 7,
          positionEnd: 8,
        },
      ],
      child: {
        type: 'Subquery',
        scope: ['users'],
        child: {
          type: 'Project',
          columns: [
            { type: 'star', positionStart: 22, positionEnd: 23 },
            {
              type: 'derived',
              expr: {
                type: 'binary',
                op: '+',
                left: { type: 'identifier', name: 'a', positionStart: 25, positionEnd: 26 },
                right: { type: 'identifier', name: 'b', positionStart: 29, positionEnd: 30 },
                positionStart: 25,
                positionEnd: 30,
              },
              alias: 'c',
              positionStart: 25,
              positionEnd: 35,
            },
          ],
          child: {
            type: 'Scan',
            table: 'users',
            hints: { columns: ['a', 'b'] },
          },
        },
      },
    })
  })

  it('should not add column hints for SELECT * join', () => {
    const plan = planSql({ query: 'SELECT * FROM users JOIN orders ON users.id = orders.user_id' })
    expect(plan).toEqual({
      type: 'Project',
      columns: [{ type: 'star', positionStart: 7, positionEnd: 8 }],
      child: {
        type: 'HashJoin',
        joinType: 'INNER',
        leftAlias: 'users',
        rightAlias: 'orders',
        leftKeys: [{
          type: 'identifier',
          name: 'id',
          prefix: 'users',
          positionStart: 35,
          positionEnd: 43,
        }],
        rightKeys: [{
          type: 'identifier',
          name: 'user_id',
          prefix: 'orders',
          positionStart: 46,
          positionEnd: 60,
        }],
        left: {
          type: 'Scan',
          table: 'users',
          hints: {},
        },
        right: {
          type: 'Scan',
          table: 'orders',
          hints: {},
        },
      },
    })
  })

  it('should retain correlated columns referenced by nested lateral UNNEST args', () => {
    const outers = memorySource({ data: [{ id: 1, arr: [10, 20] }] })
    const t = memorySource({ data: [{ k: 1 }] })
    const plan = planSql({
      tables: { outers, t },
      query: 'SELECT o.id, (SELECT COUNT(*) FROM t JOIN UNNEST(o.arr) AS u(x) ON TRUE) AS n FROM outers AS o',
    })

    if (plan.type !== 'Project' || plan.child.type !== 'Scan') {
      throw new Error(`expected Project over Scan, got ${plan.type}`)
    }
    expect(plan.child.table).toBe('outers')
    expect(plan.child.hints.columns).toEqual(expect.arrayContaining(['id', 'arr']))
    expect(plan.child.hints.columns).toHaveLength(2)
  })

  it('should keep pushdown for unqualified lateral UNNEST arguments', () => {
    const t = memorySource({ data: [{ id: 1, arr: [10, 20], padding: 'x' }] })
    const plan = planSql({
      tables: { t },
      query: 'SELECT t.id FROM t JOIN UNNEST(arr) AS u(x) ON TRUE',
    })

    if (plan.type !== 'Project' || plan.child.type !== 'NestedLoopJoin' || plan.child.left.type !== 'Scan') {
      throw new Error(`expected Project over NestedLoopJoin over Scan, got ${plan.type}`)
    }
    expect(plan.child.left.table).toBe('t')
    expect(plan.child.left.hints.columns).toEqual(['id', 'arr'])
  })
})

describe('INNER JOIN predicate pushdown', () => {
  const a = memorySource({ data: [{ id: 1, flag: true, value: 2 }] })
  const b = memorySource({ data: [{ id: 1, flag: true, value: 3 }] })
  const tables = { a, b }

  it('moves qualified comparisons into both scans and removes the outer filter', () => {
    const plan = planSql({ tables, query: 'SELECT a.id FROM a JOIN b ON a.id = b.id WHERE a.flag = true AND b.value > 2' })
    expect(plan).toMatchObject({ type: 'Project', child: {
      type: 'HashJoin',
      left: { type: 'Scan', hints: { columns: ['id', 'flag'], where: { op: '=', left: { prefix: 'a', name: 'flag' } } } },
      right: { type: 'Scan', hints: { where: { op: '>', left: { prefix: 'b', name: 'value' } } } },
    } })
  })

  it('retains cross-table comparisons above the join', () => {
    const plan = planSql({ tables, query: 'SELECT a.id FROM a JOIN b ON a.id = b.id WHERE a.flag = true AND a.value < b.value' })
    expect(plan).toMatchObject({ child: { type: 'Filter', condition: { op: '<' }, child: {
      type: 'HashJoin', left: { hints: { where: { op: '=' } } }, right: { hints: { columns: expect.arrayContaining(['id', 'value']) } },
    } } })
  })

  it('combines multiple predicates and handles aliases and reversed comparisons', () => {
    const plan = planSql({ tables, query: 'SELECT x.id FROM a x JOIN a y ON y.id = x.id WHERE 1 < x.value AND x.flag = true AND y.value > 0' })
    expect(plan).toMatchObject({ child: { type: 'HashJoin',
      left: { alias: 'x', hints: { where: { op: 'AND', left: { op: '<' }, right: { op: '=' } } } },
      right: { alias: 'y', hints: { where: { op: '>' } } },
    } })
  })

  it('does not move predicates across outer or non-equijoins', () => {
    for (const join of ['LEFT JOIN b ON a.id = b.id', 'RIGHT JOIN b ON a.id = b.id', 'FULL JOIN b ON a.id = b.id', 'JOIN b ON a.value > b.value']) {
      const plan = planSql({ tables, query: `SELECT a.id FROM a ${join} WHERE a.flag = true` })
      expect(plan).toMatchObject({ child: { type: 'Filter', child: { left: { hints: { columns: expect.any(Array) } } } } })
      if (plan.type !== 'Project' || plan.child.type !== 'Filter') throw new Error('expected residual filter')
      const joinPlan = plan.child.child
      if (joinPlan.type !== 'HashJoin' && joinPlan.type !== 'NestedLoopJoin') throw new Error('expected join')
      if (joinPlan.left.type !== 'Scan') throw new Error('expected scan')
      expect(joinPlan.left.hints.where).toBeUndefined()
    }
  })

  it('leaves unsafe WHERE and ON evaluation order alone', () => {
    for (const where of ['CAST(a.value AS INT) > 0 AND b.flag = true', 'a.flag = true OR b.flag = true', 'flag = true AND b.value > 0', 'a.flag = true AND b.id IN (SELECT id FROM b)']) {
      const plan = planSql({ tables, query: `SELECT a.id FROM a JOIN b ON a.id = b.id WHERE ${where}` })
      expect(plan).toMatchObject({ child: { type: 'Filter' } })
    }
    const plan = planSql({ tables, query: 'SELECT a.id FROM a JOIN b ON a.id = b.id AND CAST(b.value AS INT) > 0 WHERE a.flag = true' })
    expect(plan).toMatchObject({ child: { type: 'Filter', child: { type: 'HashJoin', residual: { op: '>' } } } })
  })

  it('does not push into CTEs or derived tables', () => {
    for (const query of [
      'WITH x AS (SELECT * FROM a) SELECT x.id FROM x JOIN b ON x.id = b.id WHERE x.flag = true',
      'WITH x AS (SELECT * FROM b) SELECT a.id FROM a JOIN x ON a.id = x.id WHERE a.flag = true',
      'SELECT a.id FROM (SELECT * FROM a LIMIT 1) a JOIN b ON a.id = b.id WHERE a.flag = true',
      'SELECT a.id FROM a JOIN (SELECT * FROM b LIMIT 1) b ON a.id = b.id WHERE b.flag = true',
    ]) {
      expect(planSql({ tables, query })).toMatchObject({ child: { type: 'Filter' } })
    }
  })

  it('does not interpret a struct column prefix as a table alias', () => {
    const structured = memorySource({ data: [{ id: 1, flag: false, a: { flag: true } }] })
    const plan = planSql({ tables: { a: structured, b }, query: 'SELECT a.id FROM a JOIN b ON a.id = b.id WHERE a.flag = true' })
    expect(plan).toMatchObject({ child: { type: 'Filter' } })
  })
})

describe('INNER JOIN ON predicate pushdown', () => {
  const tables = {
    a: memorySource({ data: [{ id: 1, flag: true, value: 2 }] }),
    b: memorySource({ data: [{ id: 1, flag: true, value: 3 }] }),
  }

  it('pushes ON comparisons to both scans without a WHERE clause', () => {
    const plan = planSql({ tables, query: 'SELECT a.id FROM a JOIN b ON b.id = a.id AND a.flag = true AND 2 < b.value' })
    expect(plan).toMatchObject({ child: { type: 'HashJoin',
      left: { hints: { where: { op: '=', left: { prefix: 'a', name: 'flag' } } } },
      right: { hints: { where: { op: '<', right: { prefix: 'b', name: 'value' } } } },
    } })
    if (plan.type !== 'Project' || plan.child.type !== 'HashJoin') throw new Error('expected hash join')
    expect(plan.child.residual).toBeUndefined()
  })

  it('combines ON and WHERE scan filters while keeping cross-table ON conditions', () => {
    const plan = planSql({ tables, query: 'SELECT a.id FROM a JOIN b ON a.id = b.id AND b.flag = true AND a.value < b.value WHERE b.value > 0 AND a.flag = true' })
    expect(plan).toMatchObject({ child: { type: 'HashJoin', residual: { op: '<', left: { prefix: 'a' }, right: { prefix: 'b' } },
      left: { hints: { where: { op: '=' } } },
      right: { hints: { where: { op: 'AND', left: { op: '=' }, right: { op: '>' } } } },
    } })
  })

  it('pushes safe ON predicates when WHERE is not eligible', () => {
    const plan = planSql({ tables, query: 'SELECT a.id FROM a JOIN b ON a.id = b.id AND b.flag = true WHERE CAST(a.value AS STRING) LIKE \'%2%\'' })
    expect(plan).toMatchObject({ child: { type: 'Filter', condition: { op: 'LIKE' }, child: {
      type: 'HashJoin', right: { hints: { where: { op: '=', left: { prefix: 'b', name: 'flag' } } } },
    } } })
  })

  it('does not push any predicates when ON has unsafe expressions', () => {
    for (const condition of ['b.flag = true AND LENGTH(b.value) > 0', 'flag = true AND b.value > 0', '(a.flag = true OR b.flag = true)']) {
      const plan = planSql({ tables, query: `SELECT a.id FROM a JOIN b ON a.id = b.id AND ${condition} WHERE a.flag = true` })
      if (plan.type !== 'Project' || plan.child.type !== 'Filter' || plan.child.child.type !== 'HashJoin') throw new Error('expected filtered hash join')
      const join = plan.child.child
      if (join.left.type !== 'Scan' || join.right.type !== 'Scan') throw new Error('expected scans')
      expect(join.left.hints.where).toBeUndefined()
      expect(join.right.hints.where).toBeUndefined()
      expect(join.residual).toBeDefined()
    }
  })

  it('does not push ON predicates across outer joins or shared CTE scans', () => {
    for (const query of [
      'SELECT a.id FROM a LEFT JOIN b ON a.id = b.id AND a.flag = true',
      'SELECT a.id FROM a RIGHT JOIN b ON a.id = b.id AND b.flag = true',
      'SELECT a.id FROM a FULL JOIN b ON a.id = b.id AND b.flag = true',
      'WITH c AS (SELECT * FROM b) SELECT a.id FROM a JOIN c ON a.id = c.id AND c.flag = true',
    ]) {
      const plan = planSql({ tables, query })
      if (plan.type !== 'Project' || plan.child.type !== 'HashJoin') throw new Error('expected hash join')
      expect(plan.child.residual).toBeDefined()
      if (plan.child.left.type !== 'Scan') throw new Error('expected left scan')
      expect(plan.child.left.hints.where).toBeUndefined()
    }
  })
})
