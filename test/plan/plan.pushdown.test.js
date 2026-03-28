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
        },
      ],
      child: {
        type: 'Scan',
        table: 'users',
        hints: {
          // TODO: pushdown column id and limit hints
          // columns: ['id'],
          limit: 1000,
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
        },
      ],
      child: {
        type: 'Project',
        columns: [
          {
            type: 'derived',
            expr: {
              type: 'identifier',
              name: 'id',
              positionStart: 30,
              positionEnd: 32,
            },
          },
          {
            type: 'derived',
            expr: {
              type: 'identifier',
              name: 'name',
              positionStart: 34,
              positionEnd: 38,
            },
            alias: 'full_name',
          },
        ],
        child: {
          type: 'Scan',
          table: 'users',
          hints: {
            // TODO: prune id column
            columns: ['id', 'name'],
          },
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
        },
      ],
      child: {
        type: 'Scan',
        table: 'users',
        hints: {
          // TODO: pushdown with both id and age columns
          // columns: ['id', 'age'],
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
            name: 'users.name',
            positionStart: 7,
            positionEnd: 17,
          },
        },
        {
          type: 'derived',
          expr: {
            type: 'identifier',
            name: 'orders.total',
            positionStart: 19,
            positionEnd: 31,
          },
        },
      ],
      child: {
        type: 'HashJoin',
        joinType: 'INNER',
        leftAlias: 'users',
        rightAlias: 'orders',
        leftKey: {
          type: 'identifier',
          name: 'users.id',
          positionStart: 58,
          positionEnd: 66,
        },
        rightKey: {
          type: 'identifier',
          name: 'orders.user_id',
          positionStart: 69,
          positionEnd: 83,
        },
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

  it('should not add column hints for SELECT * join', () => {
    const plan = planSql({ query: 'SELECT * FROM users JOIN orders ON users.id = orders.user_id' })
    expect(plan).toEqual({
      type: 'HashJoin',
      joinType: 'INNER',
      leftAlias: 'users',
      rightAlias: 'orders',
      leftKey: {
        type: 'identifier',
        name: 'users.id',
        positionStart: 35,
        positionEnd: 43,
      },
      rightKey: {
        type: 'identifier',
        name: 'orders.user_id',
        positionStart: 46,
        positionEnd: 60,
      },
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
    })
  })
})
