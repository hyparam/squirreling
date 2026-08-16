import { describe, expect, it, vi } from 'vitest'
import { compileBatchExpression } from '../../src/expression/batch.js'
import { parseSql } from '../../src/parse/parse.js'

/**
 * @import { AsyncBatch, ReadColumn, RelationSchema, RowSelection } from '../../src/internalTypes.js'
 * @import { ExprNode } from '../../src/types.js'
 */

/** @type {RelationSchema} */
const schema = {
  fields: [
    { id: 0, name: 'n', dataType: { type: 'number' }, nullable: true },
    { id: 1, name: 'text', dataType: { type: 'string' }, nullable: true },
  ],
}

describe('batch expressions', () => {
  it('resolves each dependency once for a synchronous vector kernel', () => {
    const compiled = compile('LENGTH(text) + n')
    expect(compiled?.dependencies).toEqual([1, 0])
    if (!compiled) throw new Error('expected expression to compile')
    const batch = loadedBatch([2, null, 4], ['abc', 'x', null])

    const result = compiled.evaluate({ batch, selection: batch.selection })

    expect(result).toEqual({
      type: 'values',
      values: [5, null, null],
      length: 3,
    })
    expect(result).not.toBeInstanceOf(Promise)
  })

  it('evaluates only selected rows', () => {
    const compiled = compile('UPPER(text)')
    if (!compiled) throw new Error('expected expression to compile')
    const batch = loadedBatch([1, 2, 3], ['a', 'b', 'c'])
    /** @type {RowSelection} */
    const selection = {
      type: 'indices',
      indices: new Uint32Array([2, 0]),
      length: 3,
    }

    expect(compiled.evaluate({ batch, selection })).toEqual({
      type: 'values',
      values: ['C', 'A'],
      length: 2,
    })
  })

  it('awaits an asynchronous column once per evaluation', async () => {
    /** @type {ReadColumn} */
    function readColumn() {
      return Promise.resolve({ type: 'values', values: ['a', 'bb'], length: 2 })
    }
    const read = vi.fn(readColumn)
    const compiled = compile('LENGTH(text)')
    if (!compiled) throw new Error('expected expression to compile')
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 2 },
      columns: [
        { type: 'loaded', vector: { type: 'values', values: [1, 2], length: 2 } },
        { type: 'source', read },
      ],
    }

    await expect(compiled.evaluate({ batch, selection: batch.selection })).resolves.toEqual({
      type: 'values',
      values: [1, 2],
      length: 2,
    })
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('declines expressions whose short-circuited branches read columns', () => {
    expect(compile('FALSE AND LENGTH(n) > 0')).toBeUndefined()
    expect(compile('TRUE OR LENGTH(n) > 0')).toBeUndefined()
    expect(compile('COALESCE(1, n)')).toBeUndefined()
  })

  it('compiles production JSON token expressions', () => {
    const compiled = compile('COALESCE(CAST(JSON_EXTRACT(text, \'$.usage.input_tokens\') AS BIGINT), 0)')
    if (!compiled) throw new Error('expected expression to compile')
    const batch = loadedBatch([1, 2, 3], [
      { usage: { input_tokens: 42 } },
      { usage: {} },
      null,
    ])

    expect(compiled.evaluate({ batch, selection: batch.selection })).toEqual({
      type: 'values',
      values: [42n, 0, 0],
      length: 3,
    })
  })

  it('resolves qualified identifiers against a bare scan schema', () => {
    const compiled = compile('data.n + 1')
    if (!compiled) throw new Error('expected expression to compile')
    const batch = loadedBatch([1, 2], ['a', 'b'])

    expect(compiled.evaluate({ batch, selection: batch.selection })).toEqual({
      type: 'values',
      values: [2, 3],
      length: 2,
    })
  })

  it('uses a stream row offset in execution errors', () => {
    const compiled = compile('LENGTH(n)')
    if (!compiled) throw new Error('expected expression to compile')
    const batch = loadedBatch([1], ['unused'])

    expect(() => compiled.evaluate({
      batch,
      selection: batch.selection,
      rowOffset: 2,
    })).toThrow('LENGTH(string): expected string or array, got number. Use CAST to convert to a string first. (row 3)')
  })

  it('yields during large cancellable kernel evaluation', async () => {
    const compiled = compile('n + 1')
    if (!compiled) throw new Error('expected expression to compile')
    const values = Array.from({ length: 50_000 }, function value(_item, index) { return index })
    const batch = loadedBatch(values, values)
    const controller = new AbortController()
    const reason = new Error('timeout')
    const timer = setTimeout(function abort() { controller.abort(reason) }, 0)

    try {
      await expect(compiled.evaluate({
        batch,
        selection: batch.selection,
        signal: controller.signal,
      })).rejects.toBe(reason)
    } finally {
      clearTimeout(timer)
    }
  })

  it('declines unsupported expressions and missing identifiers', () => {
    expect(compileBatchExpression({ expression: expression('CASE WHEN n > 0 THEN n END'), schema })).toBeUndefined()
    expect(compileBatchExpression({ expression: expression('missing + 1'), schema })).toBeUndefined()
  })
})

/**
 * @param {string} sql
 * @returns {ReturnType<typeof compileBatchExpression>}
 */
function compile(sql) {
  return compileBatchExpression({ expression: expression(sql), schema })
}

/**
 * @param {string} sql
 * @returns {ExprNode}
 */
function expression(sql) {
  const statement = parseSql({ query: `SELECT ${sql}` })
  if (statement.type !== 'select') throw new Error('expected select statement')
  const [column] = statement.columns
  if (column.type !== 'derived') throw new Error('expected derived column')
  return column.expr
}

/**
 * @param {import('../../src/types.js').SqlPrimitive[]} numbers
 * @param {import('../../src/types.js').SqlPrimitive[]} texts
 * @returns {AsyncBatch}
 */
function loadedBatch(numbers, texts) {
  return {
    schema,
    selection: { type: 'all', length: numbers.length },
    columns: [
      { type: 'loaded', vector: { type: 'values', values: numbers, length: numbers.length } },
      { type: 'loaded', vector: { type: 'values', values: texts, length: texts.length } },
    ],
  }
}
