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

  it('reads logical right-side columns only for undecided rows', async () => {
    /** @type {ReadColumn} */
    function readText({ selection }) {
      expect(selection).toEqual({
        type: 'indices',
        indices: new Uint32Array([0, 2]),
        length: 3,
      })
      return { type: 'values', values: ['a', ''], length: 2 }
    }
    const read = vi.fn(readText)
    const compiled = compile('n < 0 AND LENGTH(text) > 0')
    if (!compiled) throw new Error('expected expression to compile')
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 3 },
      columns: [
        { type: 'loaded', vector: { type: 'values', values: [-1, 1, -2], length: 3 } },
        { type: 'source', read },
      ],
    }

    await expect(compiled.evaluate({ batch, selection: batch.selection })).resolves.toEqual({
      type: 'values',
      values: [true, false, false],
      length: 3,
    })
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('reads later COALESCE arguments only for null rows', async () => {
    /** @type {ReadColumn} */
    function readText({ selection }) {
      expect(selection).toEqual({
        type: 'indices',
        indices: new Uint32Array([1, 2]),
        length: 3,
      })
      return { type: 'values', values: ['fallback', null], length: 2 }
    }
    const read = vi.fn(readText)
    const compiled = compile('COALESCE(n, text)')
    if (!compiled) throw new Error('expected expression to compile')
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 3 },
      columns: [
        { type: 'loaded', vector: { type: 'values', values: [1, null, null], length: 3 } },
        { type: 'source', read },
      ],
    }

    await expect(compiled.evaluate({ batch, selection: batch.selection })).resolves.toEqual({
      type: 'values',
      values: [1, 'fallback', null],
      length: 3,
    })
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('evaluates searched CASE branches over matching row selections', async () => {
    /** @type {ReadColumn} */
    function readText({ selection }) {
      expect(selection).toEqual({
        type: 'indices',
        indices: new Uint32Array([0]),
        length: 3,
      })
      return { type: 'values', values: ['yes'], length: 1 }
    }
    const read = vi.fn(readText)
    const compiled = compile(`CASE
      WHEN n > 0 THEN UPPER(text)
      WHEN n = 0 THEN 'zero'
      ELSE NULL
    END`)
    if (!compiled) throw new Error('expected expression to compile')
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 3 },
      columns: [
        { type: 'loaded', vector: { type: 'values', values: [1, 0, -1], length: 3 } },
        { type: 'source', read },
      ],
    }

    await expect(compiled.evaluate({ batch, selection: batch.selection })).resolves.toEqual({
      type: 'values',
      values: ['YES', 'zero', null],
      length: 3,
    })
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('composes CASE branch selections and preserves stream row numbers', async () => {
    /** @type {ReadColumn} */
    function readText({ selection }) {
      expect(selection).toEqual({
        type: 'indices',
        indices: new Uint32Array([3]),
        length: 4,
      })
      return { type: 'values', values: [3], length: 1 }
    }
    const read = vi.fn(readText)
    const compiled = compile('CASE WHEN n > 0 THEN LENGTH(text) ELSE 0 END')
    if (!compiled) throw new Error('expected expression to compile')
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'indices', indices: new Uint32Array([1, 3]), length: 4 },
      columns: [
        { type: 'loaded', vector: { type: 'values', values: [0, -1, 0, 1], length: 4 } },
        { type: 'source', read },
      ],
    }

    await expect(compiled.evaluate({
      batch,
      selection: batch.selection,
      rowOffset: 10,
    })).rejects.toThrow(
      'LENGTH(string): expected string or array, got number. Use CAST to convert to a string first. (row 12)'
    )
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('compiles simple CASE and NULLIF combinations', async () => {
    const compiled = compile(`CASE n
      WHEN 1 THEN COALESCE(NULLIF(text, ''), 'empty')
      WHEN 2 THEN NULLIF(text, 'same')
      ELSE 'other'
    END`)
    if (!compiled) throw new Error('expected expression to compile')
    const batch = loadedBatch([1, 2, 3], ['', 'same', 'value'])

    await expect(compiled.evaluate({ batch, selection: batch.selection })).resolves.toEqual({
      type: 'values',
      values: ['empty', null, 'other'],
      length: 3,
    })
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
    expect(compileBatchExpression({ expression: expression('CASE WHEN n > 0 THEN ABS(n) END'), schema })).toBeUndefined()
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
