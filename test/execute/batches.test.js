import { describe, expect, it, vi } from 'vitest'
import { readBatchColumn, selectedRowCount } from '../../src/backend/batch.js'
import { distinctBatches, filterBatches, limitBatches } from '../../src/execute/batches.js'
import { compileBatchExpression } from '../../src/expression/batch.js'
import { parseSql } from '../../src/parse/parse.js'

/**
 * @import { AsyncBatch, ExprNode, ReadColumn } from '../../src/types.js'
 */

const schema = ['keep', 'payload']

describe('batch operators', () => {
  it('filters on predicate columns without reading deferred payloads', async () => {
    /** @type {ReadColumn} */
    function readPayload({ selection }) {
      return { type: 'constant', value: 'large text', length: selectedRowCount(selection) }
    }
    const read = vi.fn(readPayload)
    /** @type {AsyncBatch} */
    const batch = {
      selection: { type: 'all', length: 4 },
      columns: [
        { type: 'values', values: [true, false, true, false], length: 4 },
        { read },
      ],
    }
    const expression = compileBatchExpression(parseExpression('keep'), schema)
    if (!expression) throw new Error('expected expression to compile')

    const filtered = []
    for await (const result of filterBatches(asyncValues([batch]), expression)) filtered.push(result)

    expect(read).not.toHaveBeenCalled()
    expect(filtered).toHaveLength(1)
    expect(filtered[0].selection).toEqual({
      type: 'indices',
      indices: new Uint32Array([0, 2]),
      length: 4,
    })
    expect(readBatchColumn({ batch: filtered[0], columnIndex: 1 })).toEqual({
      type: 'constant',
      value: 'large text',
      length: 2,
    })
    expect(read).toHaveBeenCalledTimes(1)
  })

  it('bounds predicate work when a downstream limit needs few matches', async () => {
    /** @type {{ length: number, rowOffset: number }[]} */
    const evaluations = []
    /** @type {import('../../src/internalTypes.js').CompiledBatchExpression} */
    const expression = {
      evaluate({ selection, rowOffset = 0 }) {
        const length = selectedRowCount(selection)
        evaluations.push({ length, rowOffset })
        return { type: 'constant', value: true, length }
      },
    }
    const batches = filterBatches(asyncValues([valueBatch(new Array(1_000).fill(true))]), expression, undefined, 1)
    const limited = []

    for await (const result of limitBatches(batches, 1)) limited.push(result)

    expect(evaluations).toEqual([{ length: 256, rowOffset: 0 }])
    expect(limited).toHaveLength(1)
    expect(limited[0].selection).toEqual({
      type: 'range',
      start: 0,
      end: 1,
      length: 1_000,
    })
  })

  it('grows predicate windows while preserving row offsets for late matches', async () => {
    /** @type {{ length: number, rowOffset: number }[]} */
    const evaluations = []
    /** @type {import('../../src/internalTypes.js').CompiledBatchExpression} */
    const expression = {
      evaluate({ selection, rowOffset = 0 }) {
        const length = selectedRowCount(selection)
        evaluations.push({ length, rowOffset })
        const values = new Array(length).fill(false)
        if (rowOffset <= 700 && rowOffset + length > 700) values[700 - rowOffset] = true
        return { type: 'values', values, length }
      },
    }
    const batches = filterBatches(asyncValues([
      valueBatch(new Array(100).fill(true)),
      valueBatch(new Array(1_000).fill(true)),
    ]), expression, undefined, 1)
    const limited = []

    for await (const result of limitBatches(batches, 1)) limited.push(result)

    expect(evaluations).toEqual([
      { length: 100, rowOffset: 0 },
      { length: 512, rowOffset: 100 },
      { length: 488, rowOffset: 612 },
    ])
    expect(limited).toHaveLength(1)
    expect(limited[0].selection).toEqual({
      type: 'indices',
      indices: new Uint32Array([600]),
      length: 1_000,
    })
  })

  it('deduplicates across batches with zero-copy selections', async () => {
    const first = valueBatch(['a', 'b', 'a'])
    const second = valueBatch(['b', 'c'])

    const distinct = []
    for await (const result of distinctBatches(asyncValues([first, second]))) distinct.push(result)

    expect(distinct).toHaveLength(2)
    expect(distinct[0].columns[0]).toBe(first.columns[0])
    expect(distinct[0].selection).toEqual({
      type: 'indices',
      indices: new Uint32Array([0, 1]),
      length: 3,
    })
    expect(distinct[1].columns[0]).toBe(second.columns[0])
    expect(distinct[1].selection).toEqual({
      type: 'indices',
      indices: new Uint32Array([1]),
      length: 2,
    })
  })

  it('yields while deduplicating a large loaded batch so timer aborts can fire', async () => {
    const controller = new AbortController()
    const batch = valueBatch(Array.from({ length: 20_000 }, function value(_item, index) {
      return index
    }))
    const timer = setTimeout(function abortDistinct() {
      controller.abort(new Error('distinct timed out'))
    }, 0)

    async function consumeDistinct() {
      for await (const result of distinctBatches(asyncValues([batch]), controller.signal)) {
        selectedRowCount(result.selection)
      }
    }

    try {
      await expect(consumeDistinct()).rejects.toThrow('distinct timed out')
    } finally {
      clearTimeout(timer)
    }
  })
})

/**
 * @param {string} sql
 * @returns {ExprNode}
 */
function parseExpression(sql) {
  const statement = parseSql({ query: `SELECT ${sql}` })
  if (statement.type !== 'select') throw new Error('expected select statement')
  const [column] = statement.columns
  if (column.type !== 'derived') throw new Error('expected derived column')
  return column.expr
}

/**
 * @template T
 * @param {T[]} values
 * @yields {T}
 */
async function* asyncValues(values) {
  yield* values
}

/**
 * @param {import('../../src/types.js').SqlPrimitive[]} values
 * @returns {AsyncBatch}
 */
function valueBatch(values) {
  return {
    selection: { type: 'all', length: values.length },
    columns: [{
      type: 'values',
      values,
      length: values.length,
    }],
  }
}
