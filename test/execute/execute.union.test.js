import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('UNION execution', () => {
  it.each([
    '(SELECT 1 AS x, 2 AS x) LIMIT 1',
    '((SELECT 1 AS x, 2 AS x) LIMIT 1) OFFSET 0',
    'WITH c AS (SELECT 1 AS n) SELECT n AS x, n + 1 AS x FROM c',
    '(WITH c AS (SELECT 1 AS n) SELECT n AS x, n + 1 AS x FROM c) LIMIT 1',
    'WITH c AS (SELECT 1 AS n) (SELECT n AS x, n + 1 AS x FROM c) LIMIT 1',
  ])('preserves repeated right output positions through wrappers: %s', async operand => {
    const query = `SELECT 0 AS a, 0 AS b UNION ALL (${operand})`
    expect(await collect(executeSql({ tables: {}, query }))).toEqual([{ a: 0, b: 0 }, { a: 1, b: 2 }])
    expect(await collect(executeSql({ tables: {}, query: `SELECT a FROM (${query}) result` }))).toEqual([{ a: 0 }, { a: 1 }])
  })

  it.each([
    '(SELECT MIN(n) AS x, MAX(n) AS x FROM c GROUP BY g ORDER BY 1 LIMIT 2) ORDER BY 1 LIMIT 1',
    'WITH c AS (SELECT * FROM t) SELECT MIN(n) AS x, MAX(n) AS x FROM c GROUP BY g ORDER BY 1 LIMIT 1',
  ])('preserves alias and positional ordering bindings through wrappers: %s', async operand => {
    const tables = { t: [{ g: 1, n: 1 }, { g: 1, n: 100 }, { g: 2, n: 2 }, { g: 2, n: 3 }] }
    const query = `WITH c AS (SELECT * FROM t) SELECT 0 AS a, 0 AS b UNION ALL (${operand})`
    expect(await collect(executeSql({ tables, query }))).toEqual([{ a: 0, b: 0 }, { a: 1, b: 100 }])
  })

  it.each(['b + 0', 'x + b'])('should preserve original expression bindings when renaming outputs: %s', async expr => {
    const result = await collect(executeSql({
      tables: { t: [{ b: 99 }] },
      query: `SELECT 1 AS a, 2 AS b, 3 AS c UNION ALL SELECT 4 AS x, 5 AS x, ${expr} AS z FROM t`,
    }))
    expect(result).toEqual([{ a: 1, b: 2, c: 3 }, { a: 4, b: 5, c: expr === 'b + 0' ? 99 : 104 }])
  })

  it('should preserve grouped positional ordering when duplicate right aliases are renamed', async () => {
    const result = await collect(executeSql({
      tables: { t: [{ g: 1, x: 1 }, { g: 1, x: 100 }, { g: 2, x: 50 }, { g: 2, x: 60 }] },
      query: 'SELECT 0 AS a, 0 AS b UNION ALL (SELECT MIN(x) AS z, MAX(x) AS z FROM t GROUP BY g ORDER BY 2 DESC LIMIT 1)',
    }))
    expect(result).toEqual([{ a: 0, b: 0 }, { a: 1, b: 100 }])
  })

  it.each([
    'SELECT 3 AS a UNION SELECT 2 AS b INTERSECT (SELECT 2 AS c) ORDER BY 1',
    'SELECT 3 AS a UNION SELECT 2 AS b INTERSECT SELECT 2 AS c ORDER BY 1',
  ])('should resolve compound positional ordering against final output: %s', async query => {
    expect(await collect(executeSql({ tables: {}, query }))).toEqual([{ a: 2 }, { a: 3 }])
  })

  it.each([
    ['(SELECT 1 AS a);', [{ a: 1 }]],
    ['((SELECT 1 AS a, 2 AS b) LIMIT 1) ORDER BY 2', [{ a: 1, b: 2 }]],
    ['((SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS a, 1 AS b) LIMIT 2) ORDER BY 2', [{ a: 2, b: 1 }, { a: 1, b: 2 }]],
    ['(SELECT 2 AS a UNION SELECT 1 AS a) ORDER BY a;', [{ a: 1 }, { a: 2 }]],
    ['(SELECT 2 AS a UNION SELECT 1 AS a) ORDER BY 1 LIMIT 1 OFFSET 1;', [{ a: 2 }]],
    ['(SELECT 2 AS a UNION SELECT 1 AS a ORDER BY a LIMIT 1) ORDER BY a DESC LIMIT 1;', [{ a: 1 }]],
  ])('should apply suffixes outside standalone parentheses: %s', async (query, expected) => {
    expect(await collect(executeSql({ tables: {}, query }))).toEqual(expected)
  })

  describe('rewrite scope invariants', () => {
    const tables = { t: [{ x: 1, a: 0, b: 0, z_2: 0 }, { x: 3, a: 0, b: 0, z_2: 0 }] }

    it.each(['a', 'b', 'z_1'])('keeps source HAVING bindings when generating aliases: %s', async name => {
      const result = await collect(executeSql({
        tables: { t: [{ x: 1, [name]: 0 }, { x: 3, [name]: 0 }] },
        query: `SELECT 0 AS a, 0 AS b UNION ALL (SELECT MIN(x) AS z, MAX(x) AS z FROM t GROUP BY ${name} HAVING ${name} > 0)`,
      }))
      expect(result).toEqual([{ a: 0, b: 0 }])
    })

    it('keeps qualified star columns alongside computed outputs', async () => {
      expect(await collect(executeSql({
        tables: { t: [{ x: 1 }] },
        query: '(SELECT t.*, x + 1 AS next FROM t) LIMIT 1',
      }))).toEqual([{ x: 1, next: 2 }])
    })

    it.each(['UNION ALL', 'UNION', 'INTERSECT', 'INTERSECT ALL', 'EXCEPT', 'EXCEPT ALL'])(
      'preserves HAVING bindings under duplicate output renaming in %s', async operator => {
        for (const aggregate of ['MIN(x)', 'ARRAY_AGG(x)']) {
          for (const grouping of ['', 'GROUP BY a, b, z_2']) {
            for (const having of ['a > 0', 'b > 0', 'z_2 > 0', 'z > 2', 'z < 2', 'z IN (SELECT 3)', 'a IN (SELECT 0)', 'MAX(x) > 2']) {
              for (const projection of ['*', 'b']) {
                const suffix = `FROM t ${grouping} HAVING ${having}`
                const duplicate = `SELECT ${aggregate} AS z, MAX(x) AS z ${suffix}`
                const distinct = `SELECT ${aggregate} AS first_value, MAX(x) AS z ${suffix}`
                const left = 'SELECT 1 AS a, 3 AS b'
                const query = `SELECT ${projection} FROM (${left} ${operator} (${duplicate})) result`
                const reference = `SELECT ${projection} FROM (${left} ${operator} (${distinct})) result`
                expect(await collect(executeSql({ tables, query })), query).toEqual(
                  await collect(executeSql({ tables, query: reference }))
                )
              }
            }
          }
        }
      }
    )

    it.each([
      ['t.*', 't'],
      ['s.*', 't AS s'],
      ['s.*', '(SELECT x, a FROM t) AS s'],
      ['t.*, x + 1 AS next', 't'],
      ['x + 1 AS next, t.*', 't'],
      ['t.*, u.*', 't JOIN (SELECT 9 AS y) u ON true'],
    ])('preserves the output of parenthesized %s from %s', async (projection, from) => {
      for (const inner of ['', 'ORDER BY x DESC LIMIT 1']) {
        for (const outer of ['LIMIT 1', 'ORDER BY x DESC LIMIT 1', 'ORDER BY x LIMIT 1 OFFSET 1']) {
          const body = `SELECT ${projection} FROM ${from} ${inner}`
          const query = `(${body}) ${outer}`
          const reference = `SELECT * FROM (${body}) result ${outer}`
          expect(await collect(executeSql({ tables, query })), query).toEqual(
            await collect(executeSql({ tables, query: reference }))
          )
        }
      }
    })
  })

  const users = [
    { id: 1, name: 'Alice', age: 30 },
    { id: 2, name: 'Bob', age: 25 },
    { id: 3, name: 'Charlie', age: 35 },
  ]

  const orders = [
    { id: 1, user_id: 1, amount: 100 },
    { id: 2, user_id: 2, amount: 200 },
    { id: 3, user_id: 1, amount: 150 },
  ]

  describe('UNION ALL', () => {
    it.each(['UNION ALL', 'UNION', 'INTERSECT', 'EXCEPT'])(
      'should reject duplicate left operand aliases in %s before pruning', operator => {
        for (const operands of [
          `SELECT 1 AS a, 2 AS a ${operator} SELECT 3 AS x, 4 AS y`,
          `SELECT 1 AS a, 2 AS a ${operator} SELECT 3 AS a, 4 AS a`,
        ]) {
          for (const query of [operands, `SELECT a FROM (${operands}) t`]) {
            expect(() => executeSql({ tables: {}, query }))
              .toThrow('Set operation operands must have unique output column names')
          }
        }
      }
    )

    it.each([
      ['x.*, y.*', 'x JOIN y ON true'],
      ['l.*, r.*', 'x AS l JOIN y AS r ON true'],
      ['l.*, r.*', '(SELECT a FROM x) AS l JOIN (SELECT b FROM y) AS r ON true'],
      ['*', 'x JOIN y ON true'],
    ])('should infer wildcard output columns for %s from %s', async (columns, from) => {
      const result = await collect(executeSql({
        tables: { x: [{ a: 1 }], y: [{ b: 2 }] },
        query: `SELECT ${columns} FROM ${from} UNION ALL SELECT 1 AS a, 2 AS b`,
      }))
      expect(result).toEqual([{ a: 1, b: 2 }, { a: 1, b: 2 }])
    })

    it('should combine qualified wildcards on both sides', async () => {
      const result = await collect(executeSql({
        tables: { x: [{ a: 1 }], y: [{ b: 2 }] },
        query: 'SELECT x.*, y.* FROM x JOIN y ON true UNION ALL SELECT x.*, y.* FROM x JOIN y ON true',
      }))
      expect(result).toEqual([{ a: 1, b: 2 }, { a: 1, b: 2 }])
    })

    it.each([
      'SELECT * FROM (WITH c AS (SELECT 1 AS a) SELECT * FROM c) t UNION ALL SELECT 2 AS a',
      'SELECT 1 AS a UNION ALL SELECT * FROM (WITH c AS (SELECT 2 AS a) SELECT * FROM c) t',
      'SELECT a FROM (SELECT * FROM (WITH c AS (SELECT 1 AS a, 3 AS b), d AS (SELECT * FROM c) SELECT * FROM d) t UNION ALL SELECT 2 AS x, 4 AS y) u',
    ])('should resolve nested CTE columns before validating operands: %s', async query => {
      const result = await collect(executeSql({ tables: {}, query }))
      expect(result).toEqual([{ a: 1 }, { a: 2 }])
    })

    it.each([
      ['COUNT(*) AS a', 'COUNT(*) AS a', 'a', 20],
      ['COUNT(*) AS a', 'COUNT(*) AS c', 'a DESC', 10],
      ['COUNT(*) AS a', 'COUNT(*) AS c', '-a', 10],
      ['value AS a', 'value AS c', 'a', 20],
    ])('should retain omitted compound sorting columns: %s, %s, %s', async (left, right, orderBy, b) => {
      const result = await collect(executeSql({
        tables: { x: [{ value: 3 }, { value: 4 }], y: [{ value: 1 }] },
        query: `SELECT b FROM (
          SELECT ${left}, 10 AS b FROM x
          UNION ALL
          SELECT ${right}, 20 AS d FROM y
          ORDER BY ${orderBy} LIMIT 1
        ) t`,
      }))
      expect(result).toEqual([{ b }])
    })

    it.each([
      'SELECT 1 AS a, 2 AS b UNION ALL SELECT 3 AS x, COUNT(*) AS y FROM x',
      'SELECT 3 AS a, COUNT(*) AS b FROM x UNION ALL SELECT 1 AS x, 2 AS y',
      'SELECT 1 AS a, 2 AS b UNION ALL SELECT 3 AS x, COUNT(*) AS y FROM x GROUP BY value',
      'SELECT 3 AS a, COUNT(*) AS b FROM x GROUP BY value UNION ALL SELECT 1 AS x, 2 AS y',
    ])('should align pruned aggregate operands: %s', async query => {
      const result = await collect(executeSql({
        tables: { x: [{ value: 1 }, { value: 1 }] },
        query: `SELECT b FROM (${query}) t`,
      }))
      expect(result).toEqual([{ b: 2 }, { b: 2 }])
    })

    it.each([
      'SELECT 1 AS a, 2 AS b UNION SELECT 3 AS x, 2 AS y UNION ALL SELECT 4 AS z, 2 AS w',
      'SELECT 4 AS a, 2 AS b UNION ALL SELECT * FROM (SELECT 1 AS x, 2 AS y UNION SELECT 3 AS z, 2 AS w) u',
    ])('should align nested deduplicating operands without changing multiplicity: %s', async query => {
      const result = await collect(executeSql({
        tables: {},
        query: `SELECT b FROM (${query}) t`,
      }))
      expect(result).toEqual([{ b: 2 }, { b: 2 }, { b: 2 }])
    })

    it('should combine results from two queries', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age < 30
          UNION ALL
          SELECT name FROM users WHERE age > 30
        `,
      }))
      expect(result).toEqual([
        { name: 'Bob' },
        { name: 'Charlie' },
      ])
    })

    it('should include duplicates', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          UNION ALL
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice' },
        { name: 'Bob' },
        { name: 'Charlie' },
        { name: 'Alice' },
        { name: 'Charlie' },
      ])
    })

    it('should handle empty left side', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age > 100
          UNION ALL
          SELECT name FROM users WHERE age < 30
        `,
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })

    it('should handle empty right side', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age < 30
          UNION ALL
          SELECT name FROM users WHERE age > 100
        `,
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })
  })

  describe('UNION (distinct)', () => {
    it('should remove duplicates', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          UNION
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      const names = result.map(r => r.name).sort()
      expect(names).toEqual(['Alice', 'Bob', 'Charlie'])
    })

    it('should combine from different tables', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT id FROM users
          UNION
          SELECT id FROM orders
        `,
      }))
      expect(result).toHaveLength(3)
    })

    it('should take column names from the left operand by position', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT 'user' AS kind, name AS label FROM users WHERE id = 1
          UNION ALL
          SELECT 'order', CAST(amount AS STRING) FROM orders WHERE id = 2
        `,
      }))
      expect(result).toEqual([
        { kind: 'user', label: 'Alice' },
        { kind: 'order', label: '200' },
      ])
    })

    it('should dedupe by position when column names differ', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT id AS a FROM users WHERE id = 1
          UNION
          SELECT id AS b FROM users WHERE id <= 2
        `,
      }))
      expect(result).toEqual([{ a: 1 }, { a: 2 }])
    })

    it('should map outer column references onto right names by position', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT k, n FROM (
            SELECT 'users' AS k, COUNT(*) AS n FROM users
            UNION ALL
            SELECT 'orders' AS src, COUNT(*) AS total FROM orders
            UNION ALL
            SELECT 'big', amount FROM orders WHERE amount > 150
          ) t ORDER BY k
        `,
      }))
      expect(result).toEqual([
        { k: 'big', n: 200 },
        { k: 'orders', n: 3 },
        { k: 'users', n: 3 },
      ])
    })

    it('should dedupe whole rows when the outer query reads fewer columns', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT k FROM (
            SELECT 'x' AS k, id AS n FROM users
            UNION
            SELECT 'x', age FROM users
          ) t
        `,
      }))
      expect(result).toEqual([{ k: 'x' }, { k: 'x' }, { k: 'x' }, { k: 'x' }, { k: 'x' }, { k: 'x' }])
    })

    it('should match columns by position in INTERSECT and EXCEPT', async () => {
      const intersect = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT id AS a FROM users INTERSECT SELECT user_id AS b FROM orders',
      }))
      expect(intersect).toEqual([{ a: 1 }, { a: 2 }])
      const except = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT id AS a FROM users EXCEPT SELECT user_id AS b FROM orders',
      }))
      expect(except).toEqual([{ a: 3 }])
    })

    it('should reject set operations with mismatched column counts', () => {
      expect(() => executeSql({
        tables: { users },
        query: `
          SELECT id FROM users
          UNION
          SELECT id, name FROM users
        `,
      })).toThrow('Set operation operands must have the same number of columns, got left [id] and right [id, name]')
    })
  })

  describe('INTERSECT', () => {
    it('should return only common rows', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          INTERSECT
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      const names = result.map(r => r.name).sort()
      expect(names).toEqual(['Alice', 'Charlie'])
    })

    it('should return empty when no overlap', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age < 30
          INTERSECT
          SELECT name FROM users WHERE age > 30
        `,
      }))
      expect(result).toEqual([])
    })

    it('should support INTERSECT ALL', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          INTERSECT ALL
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      const names = result.map(r => r.name).sort()
      expect(names).toEqual(['Alice', 'Charlie'])
    })
  })

  describe('EXCEPT', () => {
    it('should return rows from left not in right', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          EXCEPT
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })

    it('should return all when no overlap', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age < 30
          EXCEPT
          SELECT name FROM users WHERE age > 30
        `,
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })

    it('should return empty when all overlap', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 30
          EXCEPT
          SELECT name FROM users WHERE age >= 25
        `,
      }))
      expect(result).toEqual([])
    })

    it('should support EXCEPT ALL', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          EXCEPT ALL
          SELECT name FROM users WHERE age >= 30
        `,
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })
  })

  describe('chained set operations', () => {
    it('should chain UNION ALL', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE name = 'Alice'
          UNION ALL
          SELECT name FROM users WHERE name = 'Bob'
          UNION ALL
          SELECT name FROM users WHERE name = 'Charlie'
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice' },
        { name: 'Bob' },
        { name: 'Charlie' },
      ])
    })

    it('should chain mixed operations', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users
          UNION ALL
          SELECT name FROM users
          EXCEPT
          SELECT name FROM users WHERE name = 'Bob'
        `,
      }))
      // First UNION ALL produces 6 rows, then EXCEPT removes Bob
      const names = result.map(r => r.name).sort()
      expect(names).toEqual(['Alice', 'Charlie'])
    })

    it('should give INTERSECT higher precedence than UNION', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE name = 'Alice'
          UNION
          SELECT name FROM users WHERE name = 'Bob'
          INTERSECT
          SELECT name FROM users WHERE name = 'Bob'
        `,
      }))
      const names = result.map(r => r.name).sort()
      expect(names).toEqual(['Alice', 'Bob'])
    })
  })

  describe('UNION with ORDER BY and LIMIT', () => {
    it('should apply ORDER BY to full result', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name, age FROM users WHERE age < 30
          UNION ALL
          SELECT name, age FROM users WHERE age > 30
          ORDER BY age ASC
        `,
      }))
      expect(result).toEqual([
        { name: 'Bob', age: 25 },
        { name: 'Charlie', age: 35 },
      ])
    })

    it('should apply LIMIT to full result', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name FROM users WHERE age >= 25
          UNION ALL
          SELECT name FROM users WHERE age >= 30
          LIMIT 3
        `,
      }))
      expect(result).toHaveLength(3)
    })

    it('should apply ORDER BY and LIMIT together', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name, age FROM users
          UNION ALL
          SELECT name, age FROM users
          ORDER BY age ASC
          LIMIT 2
        `,
      }))
      expect(result).toEqual([
        { name: 'Bob', age: 25 },
        { name: 'Bob', age: 25 },
      ])
    })
  })

  describe('UNION with CTE', () => {
    it('should work with CTE', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          WITH young AS (SELECT name FROM users WHERE age < 30)
          SELECT name FROM young
          UNION ALL
          SELECT name FROM users WHERE age > 30
        `,
      }))
      expect(result).toEqual([
        { name: 'Bob' },
        { name: 'Charlie' },
      ])
    })
  })

  describe('parenthesized operands', () => {
    it('should parse parenthesized operands on both sides', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: '(SELECT name FROM users WHERE id = 1) UNION ALL (SELECT name FROM users WHERE id = 3)',
      }))
      expect(result).toEqual([{ name: 'Alice' }, { name: 'Charlie' }])
    })

    it('should keep ORDER BY and LIMIT inside a parenthesized operand', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          (SELECT name FROM users ORDER BY age DESC LIMIT 1)
          UNION ALL
          (SELECT name FROM users ORDER BY age LIMIT 1)
        `,
      }))
      expect(result).toEqual([{ name: 'Charlie' }, { name: 'Bob' }])
    })

    it('should apply ORDER BY and LIMIT after the last parenthesized operand to the whole result', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: '(SELECT id FROM users) UNION ALL (SELECT amount FROM orders) ORDER BY id DESC LIMIT 2',
      }))
      expect(result).toEqual([{ id: 200 }, { id: 150 }])
    })

    it('should resolve positional ORDER BY after parenthesized operands', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: '(SELECT id AS n FROM users) UNION ALL (SELECT amount FROM orders) ORDER BY 1 LIMIT 2',
      }))
      expect(result).toEqual([{ n: 1 }, { n: 2 }])
    })

    it('should nest parenthesized set operations', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: '((SELECT id FROM users) EXCEPT (SELECT 2)) UNION ALL (SELECT 9)',
      }))
      expect(result).toEqual([{ id: 1 }, { id: 3 }, { id: 9 }])
    })

    it('should accept a lone parenthesized query', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: '(SELECT name FROM users WHERE id = 2)',
      }))
      expect(result).toEqual([{ name: 'Bob' }])
    })
  })

  describe('repeated column names', () => {
    it.each([
      ['UNION ALL', [{ a: 1, b: 2 }, { a: 3, b: 4 }]],
      ['INTERSECT', []],
      ['EXCEPT', [{ a: 1, b: 2 }]],
    ])('should match repeated right aliases by position in %s', async (operator, expected) => {
      const result = await collect(executeSql({
        tables: {},
        query: `SELECT a, b FROM (SELECT 1 AS a, 2 AS b ${operator} SELECT 3 AS x, 4 AS x) t`,
      }))
      expect(result).toEqual(expected)
    })

    it('should accept repeated names in the right operand', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT name, age AS a, id AS b FROM users WHERE id = 1
          UNION ALL
          SELECT 'none', NULL, NULL
        `,
      }))
      expect(result).toEqual([
        { name: 'Alice', a: 30, b: 1 },
        { name: 'none', a: null, b: null },
      ])
    })

    it('should keep repeated right names distinct when the left name is taken', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: `
          SELECT id AS null_3, age AS "NULL", name FROM users WHERE id = 2
          UNION ALL
          SELECT NULL, NULL, 'x'
        `,
      }))
      expect(result).toEqual([
        { null_3: 2, NULL: 25, name: 'Bob' },
        { null_3: null, NULL: null, name: 'x' },
      ])
    })

    it('should project repeated right names through an outer query', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT b FROM (SELECT id AS a, age AS b FROM users WHERE id = 3 UNION ALL SELECT MIN(id), MIN(id) FROM users) t',
      }))
      expect(result).toEqual([{ b: 35 }, { b: 1 }])
    })

    it('should reject repeated names in the left operand', () => {
      expect(() => executeSql({
        tables: { users },
        query: 'SELECT NULL, NULL UNION ALL SELECT 1, 2',
      })).toThrow('Set operation operands must have unique output column names')
    })
  })
})
