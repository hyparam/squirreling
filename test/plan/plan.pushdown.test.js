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
