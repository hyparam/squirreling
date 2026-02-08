import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'
import { queryPlan } from '../../src/plan/plan.js'

describe('queryPlan', () => {
  describe('basic queries', () => {
    it('should build plan for SELECT * FROM table', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT * FROM users' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'Scan',
          table: 'users',
          hints: {},
        },
      })
    })

    it('should build plan for SELECT with columns', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT name, age FROM users' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [
          {
            kind: 'derived',
            expr: {
              type: 'identifier',
              name: 'name',
              positionStart: 7,
              positionEnd: 11,
            },
          },
          {
            kind: 'derived',
            expr: {
              type: 'identifier',
              name: 'age',
              positionStart: 13,
              positionEnd: 16,
            },
          },
        ],
        child: {
          type: 'Scan',
          table: 'users',
          hints: {
            columns: ['name', 'age'],
          },
        },
      })
    })

    it('should build plan for SELECT from subquery', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT * FROM (SELECT * FROM users) AS u' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'SubqueryScan',
          subquery: {
            type: 'Project',
            columns: [{ kind: 'star' }],
            child: {
              type: 'Scan',
              table: 'users',
              hints: {},
            },
          },
          alias: 'u',
        },
      })
    })
  })

  describe('WHERE clause', () => {
    it('should pass WHERE as hint without Filter node', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT * FROM users WHERE age > 21' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'Scan',
          table: 'users',
          hints: {
            where: {
              type: 'binary',
              op: '>',
              left: {
                type: 'identifier',
                name: 'age',
                positionStart: 26,
                positionEnd: 29,
              },
              right: {
                type: 'literal',
                value: 21,
                positionStart: 32,
                positionEnd: 34,
              },
              positionStart: 26,
              positionEnd: 34,
            },
          },
        },
      })
    })
  })

  describe('ORDER BY', () => {
    it('should add SortNode for ORDER BY', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT * FROM users ORDER BY name' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'Sort',
          orderBy: [
            {
              expr: {
                type: 'identifier',
                name: 'name',
                positionStart: 29,
                positionEnd: 33,
              },
              direction: 'ASC',
            },
          ],
          child: {
            type: 'Scan',
            table: 'users',
            hints: {},
          },
        },
      })
    })

    it('should place Sort before Project', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT name FROM users ORDER BY age' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [
          {
            kind: 'derived',
            expr: {
              type: 'identifier',
              name: 'name',
              positionStart: 7,
              positionEnd: 11,
            },
          },
        ],
        child: {
          type: 'Sort',
          orderBy: [
            {
              expr: {
                type: 'identifier',
                name: 'age',
                positionStart: 32,
                positionEnd: 35,
              },
              direction: 'ASC',
            },
          ],
          child: {
            type: 'Scan',
            table: 'users',
            hints: {
              columns: ['name', 'age'],
            },
          },
        },
      })
    })
  })

  describe('DISTINCT', () => {
    it('should add DistinctNode for SELECT DISTINCT', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT DISTINCT name FROM users' }))
      expect(plan).toEqual({
        type: 'Distinct',
        child: {
          type: 'Project',
          columns: [
            {
              kind: 'derived',
              expr: {
                type: 'identifier',
                name: 'name',
                positionStart: 16,
                positionEnd: 20,
              },
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
      })
    })
  })

  describe('LIMIT/OFFSET', () => {
    it('should delegate LIMIT to scan hints without LimitNode', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT * FROM users LIMIT 10' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'Scan',
          table: 'users',
          hints: {
            limit: 10,
          },
        },
      })
    })

    it('should delegate offset and limit to scan hints without LimitNode', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT * FROM users LIMIT 10 OFFSET 5' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'Scan',
          table: 'users',
          hints: {
            limit: 10,
            offset: 5,
          },
        },
      })
    })
  })

  describe('GROUP BY', () => {
    it('should add HashAggregateNode for GROUP BY', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT department, COUNT(*) FROM users GROUP BY department' }))
      expect(plan).toEqual({
        type: 'HashAggregate',
        groupBy: [
          {
            type: 'identifier',
            name: 'department',
            positionStart: 48,
            positionEnd: 58,
          },
        ],
        columns: [
          {
            kind: 'derived',
            expr: {
              type: 'identifier',
              name: 'department',
              positionStart: 7,
              positionEnd: 17,
            },
          },
          {
            kind: 'derived',
            expr: {
              type: 'function',
              name: 'COUNT',
              args: [
                {
                  type: 'identifier',
                  name: '*',
                  positionStart: 25,
                  positionEnd: 26,
                },
              ],
              positionStart: 19,
              positionEnd: 27,
            },
          },
        ],
        child: {
          type: 'Scan',
          table: 'users',
          hints: {
            columns: ['department'],
          },
        },
      })
    })

    it('should add ScalarAggregateNode for aggregate without GROUP BY', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT COUNT(*) FROM users' }))
      expect(plan).toEqual({
        type: 'ScalarAggregate',
        columns: [
          {
            kind: 'derived',
            expr: {
              type: 'function',
              name: 'COUNT',
              args: [
                {
                  type: 'identifier',
                  name: '*',
                  positionStart: 13,
                  positionEnd: 14,
                },
              ],
              positionStart: 7,
              positionEnd: 15,
            },
          },
        ],
        child: {
          type: 'Scan',
          table: 'users',
          hints: {
            columns: [],
          },
        },
      })
    })
  })

  describe('HAVING', () => {
    it('should integrate HAVING into HashAggregateNode', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT department, COUNT(*) FROM users GROUP BY department HAVING COUNT(*) > 5' }))
      expect(plan).toEqual({
        type: 'HashAggregate',
        groupBy: [
          {
            type: 'identifier',
            name: 'department',
            positionStart: 48,
            positionEnd: 58,
          },
        ],
        columns: [
          {
            kind: 'derived',
            expr: {
              type: 'identifier',
              name: 'department',
              positionStart: 7,
              positionEnd: 17,
            },
          },
          {
            kind: 'derived',
            expr: {
              type: 'function',
              name: 'COUNT',
              args: [
                {
                  type: 'identifier',
                  name: '*',
                  positionStart: 25,
                  positionEnd: 26,
                },
              ],
              positionStart: 19,
              positionEnd: 27,
            },
          },
        ],
        having: {
          type: 'binary',
          op: '>',
          left: {
            type: 'function',
            name: 'COUNT',
            args: [
              {
                type: 'identifier',
                name: '*',
                positionStart: 72,
                positionEnd: 73,
              },
            ],
            positionStart: 66,
            positionEnd: 74,
          },
          right: {
            type: 'literal',
            value: 5,
            positionStart: 77,
            positionEnd: 78,
          },
          positionStart: 66,
          positionEnd: 78,
        },
        child: {
          type: 'Scan',
          table: 'users',
          hints: {
            columns: ['department'],
          },
        },
      })
    })
  })

  describe('JOINs', () => {
    it('should build HashJoinNode for simple equality join', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT * FROM users JOIN orders ON users.id = orders.user_id' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'HashJoin',
          joinType: 'INNER',
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
          },
        },
      })
    })

    it('should build PositionalJoinNode for POSITIONAL JOIN', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT * FROM a POSITIONAL JOIN b' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'PositionalJoin',
          left: {
            type: 'Scan',
            table: 'a',
            hints: {},
          },
          right: {
            type: 'Scan',
            table: 'b',
          },
        },
      })
    })

    it('should build NestedLoopJoinNode for complex join conditions', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT * FROM users JOIN orders ON users.id > orders.user_id' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'NestedLoopJoin',
          joinType: 'INNER',
          condition: {
            type: 'binary',
            op: '>',
            left: {
              type: 'identifier',
              name: 'users.id',
              positionStart: 35,
              positionEnd: 43,
            },
            right: {
              type: 'identifier',
              name: 'orders.user_id',
              positionStart: 46,
              positionEnd: 60,
            },
            positionStart: 35,
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
          },
        },
      })
    })

    it('should handle LEFT JOIN', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'HashJoin',
          joinType: 'LEFT',
          leftKey: {
            type: 'identifier',
            name: 'users.id',
            positionStart: 40,
            positionEnd: 48,
          },
          rightKey: {
            type: 'identifier',
            name: 'orders.user_id',
            positionStart: 51,
            positionEnd: 65,
          },
          left: {
            type: 'Scan',
            table: 'users',
            hints: {},
          },
          right: {
            type: 'Scan',
            table: 'orders',
          },
        },
      })
    })
  })

  describe('CTE resolution', () => {
    it('should resolve CTE references to SubqueryScan plans', () => {
      const plan = queryPlan(parseSql({ query: 'WITH active AS (SELECT id FROM users WHERE age > 21) SELECT * FROM active' }))
      expect(plan).toEqual({
        type: 'Project',
        columns: [{ kind: 'star' }],
        child: {
          type: 'SubqueryScan',
          subquery: {
            type: 'Project',
            columns: [
              {
                kind: 'derived',
                expr: {
                  type: 'identifier',
                  name: 'id',
                  positionStart: 23,
                  positionEnd: 25,
                },
              },
            ],
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
                    positionStart: 43,
                    positionEnd: 46,
                  },
                  right: {
                    type: 'literal',
                    value: 21,
                    positionStart: 49,
                    positionEnd: 51,
                  },
                  positionStart: 43,
                  positionEnd: 51,
                },
              },
            },
          },
          alias: 'active',
        },
      })
    })
  })

  describe('complex queries', () => {
    it('should build correct plan for query with WHERE, ORDER BY, LIMIT', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT name FROM users WHERE age > 21 ORDER BY name LIMIT 10' }))
      expect(plan).toEqual({
        type: 'Limit',
        limit: 10,
        child: {
          type: 'Project',
          columns: [
            {
              kind: 'derived',
              expr: {
                type: 'identifier',
                name: 'name',
                positionStart: 7,
                positionEnd: 11,
              },
            },
          ],
          child: {
            type: 'Sort',
            orderBy: [
              {
                expr: {
                  type: 'identifier',
                  name: 'name',
                  positionStart: 47,
                  positionEnd: 51,
                },
                direction: 'ASC',
              },
            ],
            child: {
              type: 'Scan',
              table: 'users',
              hints: {
                columns: ['name', 'age'],
                where: {
                  type: 'binary',
                  op: '>',
                  left: {
                    type: 'identifier',
                    name: 'age',
                    positionStart: 29,
                    positionEnd: 32,
                  },
                  right: {
                    type: 'literal',
                    value: 21,
                    positionStart: 35,
                    positionEnd: 37,
                  },
                  positionStart: 29,
                  positionEnd: 37,
                },
              },
            },
          },
        },
      })
    })

    it('should build correct plan for grouped query with HAVING and ORDER BY', () => {
      const plan = queryPlan(parseSql({ query: 'SELECT department, COUNT(*) as cnt FROM users GROUP BY department HAVING COUNT(*) > 5 ORDER BY cnt LIMIT 10' }))
      expect(plan).toEqual({
        type: 'Limit',
        limit: 10,
        child: {
          type: 'Sort',
          orderBy: [
            {
              expr: {
                type: 'identifier',
                name: 'cnt',
                positionStart: 95,
                positionEnd: 98,
              },
              direction: 'ASC',
            },
          ],
          child: {
            type: 'HashAggregate',
            groupBy: [
              {
                type: 'identifier',
                name: 'department',
                positionStart: 55,
                positionEnd: 65,
              },
            ],
            columns: [
              {
                kind: 'derived',
                expr: {
                  type: 'identifier',
                  name: 'department',
                  positionStart: 7,
                  positionEnd: 17,
                },
              },
              {
                kind: 'derived',
                expr: {
                  type: 'function',
                  name: 'COUNT',
                  args: [
                    {
                      type: 'identifier',
                      name: '*',
                      positionStart: 25,
                      positionEnd: 26,
                    },
                  ],
                  positionStart: 19,
                  positionEnd: 27,
                },
                alias: 'cnt',
              },
            ],
            having: {
              type: 'binary',
              op: '>',
              left: {
                type: 'function',
                name: 'COUNT',
                args: [
                  {
                    type: 'identifier',
                    name: '*',
                    positionStart: 79,
                    positionEnd: 80,
                  },
                ],
                positionStart: 73,
                positionEnd: 81,
              },
              right: {
                type: 'literal',
                value: 5,
                positionStart: 84,
                positionEnd: 85,
              },
              positionStart: 73,
              positionEnd: 85,
            },
            child: {
              type: 'Scan',
              table: 'users',
              hints: {
                columns: ['department', 'cnt'],
              },
            },
          },
        },
      })
    })
  })
})
