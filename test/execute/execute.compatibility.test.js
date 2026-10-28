import { describe, expect, it, vi } from 'vitest'
import { asyncRow, collect, executePlan, executeSql, parseSql, planSql } from '../../src/index.js'
import { valueAt } from '../../src/backend/batch.js'
import { compileBatchExpression } from '../../src/expression/batch.js'
import { evaluateExpr } from '../../src/expression/evaluate.js'

/** @import { AsyncBatch, ExprNode, SqlPrimitive } from '../../src/types.js' */

/**
 * @param {string} sql
 * @returns {ExprNode}
 */
function expression(sql) {
  const statement = parseSql({ query: `SELECT ${sql} AS result FROM t` })
  if (statement.type !== 'select' || statement.columns[0].type !== 'derived') throw new Error('expected expression')
  return statement.columns[0].expr
}

/**
 * Compare scalar, vector-kernel, and selection-aware batch execution.
 * @param {string} sql
 * @param {SqlPrimitive[]} values
 * @param {SqlPrimitive[]} expected
 */
async function checkPaths(sql, values, expected) {
  const node = expression(sql)
  const scalar = []
  for (const value of values) {
    scalar.push(await evaluateExpr({ node, row: asyncRow({ value }, ['value']), context: { tables: {} } }))
  }
  expect(scalar).toEqual(expected)
  // CASE forces selection-aware evaluation rather than the eager value kernel.
  for (const source of [sql, sql.replaceAll('value', '(CASE WHEN value IS NULL THEN NULL ELSE value END)')]) {
    const compiled = compileBatchExpression(expression(source), ['value'])
    if (!compiled) throw new Error('expected compiled expression')
    /** @type {AsyncBatch} */
    const batch = {
      selection: { type: 'all', length: values.length },
      columns: [{ type: 'values', values, length: values.length }],
    }
    const result = await compiled.evaluate({ batch, selection: batch.selection })
    expect(values.map((_, i) => valueAt(result, i))).toEqual(expected)
  }
}

describe('boolean IS predicates', () => {
  it.each([
    ['IS TRUE', [true, false, false]],
    ['IS NOT TRUE', [false, true, true]],
    ['IS FALSE', [false, true, false]],
    ['IS NOT FALSE', [true, false, true]],
  ])('%s preserves its truth table in all execution paths', async (predicate, expected) => {
    await checkPaths(`value ${predicate}`, [true, false, null], expected)
  })

  it('preserves null rows under NOT, WHERE and outer joins', async () => {
    const tables = { t: [{ id: 1 }, { id: 2 }], r: [{ id: 1, value: true }] }
    expect(await collect(executeSql({ tables, query: 'SELECT t.id FROM t LEFT JOIN r ON t.id = r.id WHERE r.value IS NOT TRUE' })))
      .toEqual([{ id: 2 }])
    expect(await collect(executeSql({ tables: { t: [{ value: true }, { value: false }, { value: null }] }, query: 'SELECT value FROM t WHERE NOT value IS TRUE' })))
      .toEqual([{ value: false }, { value: null }])
  })

  it('rejects unsupported IS operands', () => {
    expect(() => expression('value IS 1')).toThrow('NULL, TRUE, or FALSE')
    expect(() => expression('value IS NOT 1')).toThrow('NULL, TRUE, or FALSE')
  })
})

describe('JSON_EXTRACT_SCALAR', () => {
  it('extracts decoded scalar text and returns null for containers, missing fields and JSON null', async () => {
    await checkPaths('JSON_EXTRACT_SCALAR(value, \'$.item\')', [
      { item: 'hello "world"' }, '{"item":42}', { item: false }, { item: {} }, { item: [] }, { item: null }, {}, null,
    ], ['hello "world"', '42', 'false', null, null, null, null, null])
  })

  it('supports root scalars and nested array paths', async () => {
    await checkPaths('JSON_EXTRACT_SCALAR(value, \'$\')', ['42', 'false', '"text"', 'null', '{}', '[]', 5, true], ['42', 'false', 'text', null, null, null, '5', 'true'])
    await checkPaths('JSON_EXTRACT_SCALAR(value, \'$.items[0].name\')', [{ items: [{ name: 'yes' }] }], ['yes'])
    await checkPaths('JSON_EXTRACT_SCALAR(value, NULL)', [{ item: 1 }], [null])
  })

  it('reports invalid JSON in row and batch paths', async () => {
    const node = expression('JSON_EXTRACT_SCALAR(value, \'$.item\')')
    await expect(evaluateExpr({ node, row: asyncRow({ value: 'invalid JSON' }, ['value']), context: { tables: {} } })).rejects.toThrow('invalid JSON string')
    await expect(collect(executeSql({ tables: { t: [{ value: 'invalid JSON' }] }, query: 'SELECT JSON_EXTRACT_SCALAR(value, \'$.item\') FROM t' }))).rejects.toThrow('invalid JSON string')
  })
})

describe('CHR', () => {
  it('supports Unicode scalar values, bigint and null on all paths', async () => {
    await checkPaths('CHR(value)', [0, 65, 65n, 0x1f43f, 0x10ffff, null], ['\0', 'A', 'A', '🐿', String.fromCodePoint(0x10ffff), null])
  })

  it.each([-1, 1.5, 0xd800, 0xdfff, 0x110000, Infinity, NaN, '65', true, {}])('rejects invalid code point %s', async value => {
    await expect(evaluateExpr({ node: expression('CHR(value)'), row: asyncRow({ value }, ['value']), context: { tables: {} } })).rejects.toThrow('Unicode scalar value')
    await expect(collect(executeSql({ tables: { t: [{ value }] }, query: 'SELECT CHR(value) FROM t' }))).rejects.toThrow('Unicode scalar value')
  })
})

describe('GROUP_CONCAT', () => {
  const tables = { t: [{ g: 1, value: 'a' }, { g: 1, value: null }, { g: 1, value: 'a' }, { g: 1, value: 'b' }, { g: 2, value: null }] }

  it('uses comma by default, skips nulls, and supports DISTINCT and FILTER', async () => {
    expect(await collect(executeSql({ tables, query: 'SELECT g, GROUP_CONCAT(value) AS all_values, GROUP_CONCAT(DISTINCT value, \'|\') AS unique_values, GROUP_CONCAT(value) FILTER (WHERE value <> \'b\') AS filtered FROM t GROUP BY g ORDER BY g' })))
      .toEqual([{ g: 1, all_values: 'a,a,b', unique_values: 'a|b', filtered: 'a,a' }, { g: 2, all_values: null, unique_values: null, filtered: null }])
  })

  it('handles empty input and empty separators', async () => {
    expect(await collect(executeSql({ tables, query: 'SELECT GROUP_CONCAT(value, \'\') AS result FROM t' }))).toEqual([{ result: 'aab' }])
    expect(await collect(executeSql({ tables, query: 'SELECT GROUP_CONCAT(value, NULL) AS result FROM t' }))).toEqual([{ result: 'aab' }])
    expect(await collect(executeSql({ tables, query: 'SELECT GROUP_CONCAT(value) AS result FROM t WHERE g = 3' }))).toEqual([{ result: null }])
  })

  it('works when the aggregate only appears in HAVING or ORDER BY', async () => {
    expect(await collect(executeSql({ tables, query: 'SELECT g FROM t GROUP BY g HAVING GROUP_CONCAT(value) = \'a,a,b\' ORDER BY GROUP_CONCAT(value)' }))).toEqual([{ g: 1 }])
  })
})

describe('NOW', () => {
  it('shares one UTC instant across delayed rows, subqueries and current-time functions', async () => {
    vi.useFakeTimers()
    try {
      vi.setSystemTime(new Date('2026-10-28T23:59:59.000Z'))
      const result = executeSql({ tables: { t: [{ id: 1 }, { id: 2 }] }, query: 'SELECT NOW() AS n, CURRENT_TIMESTAMP AS ts, CURRENT_DATE AS d, CURRENT_TIME AS time, (SELECT NOW()) AS nested FROM t' })
      vi.setSystemTime(new Date('2026-10-29T00:00:05.000Z'))
      expect(await collect(result)).toEqual(Array(2).fill({ n: '2026-10-28T23:59:59.000Z', ts: '2026-10-28T23:59:59.000Z', d: '2026-10-28', time: '23:59:59.000', nested: '2026-10-28T23:59:59.000Z' }))
      expect(await collect(executeSql({ tables: {}, query: 'SELECT NOW() AS n' }))).toEqual([{ n: '2026-10-29T00:00:05.000Z' }])
    } finally {
      vi.useRealTimers()
    }
  })

  it('takes a fresh instant when reusing a plan and caller context', async () => {
    vi.useFakeTimers()
    try {
      const plan = planSql({ query: 'SELECT NOW() AS n' })
      const context = { tables: {} }
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
      const first = executePlan({ plan, context })
      vi.setSystemTime(new Date('2026-02-01T00:00:00Z'))
      const second = executePlan({ plan, context })
      expect(await collect(first)).toEqual([{ n: '2026-01-01T00:00:00.000Z' }])
      expect(await collect(second)).toEqual([{ n: '2026-02-01T00:00:00.000Z' }])
    } finally {
      vi.useRealTimers()
    }
  })
})

it.each(['NOW(1)', 'CHR()', 'CHR(1, 2)', 'JSON_EXTRACT_SCALAR(\'{}\')', 'GROUP_CONCAT()', 'GROUP_CONCAT(1, \',\', \',\')'] )('validates argument counts for %s', sql => {
  expect(() => expression(sql)).toThrow(/requires/)
})
