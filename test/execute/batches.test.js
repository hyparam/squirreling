import { describe, expect, it, vi } from 'vitest'
import { readBatchColumn, selectedRowCount } from '../../src/backend/batch.js'
import { filterBatches } from '../../src/execute/batches.js'
import { compileBatchExpression } from '../../src/expression/batch.js'
import { parseSql } from '../../src/parse/parse.js'

/**
 * @import { AsyncBatch, ReadColumn, RelationSchema } from '../../src/internalTypes.js'
 * @import { ExprNode } from '../../src/types.js'
 */

/** @type {RelationSchema} */
const schema = {
  fields: [
    { id: 0, name: 'keep', dataType: { type: 'boolean' }, nullable: false },
    { id: 1, name: 'payload', dataType: { type: 'string' }, nullable: false },
  ],
}

describe('batch operators', () => {
  it('filters on predicate columns without reading deferred payloads', async () => {
    /** @type {ReadColumn} */
    function readPayload({ selection }) {
      return { type: 'constant', value: 'large text', length: selectedRowCount(selection) }
    }
    const read = vi.fn(readPayload)
    /** @type {AsyncBatch} */
    const batch = {
      schema,
      selection: { type: 'all', length: 4 },
      columns: [
        { type: 'loaded', vector: { type: 'values', values: [true, false, true, false], length: 4 } },
        { type: 'source', read },
      ],
    }
    const expression = compileBatchExpression({ expression: parseExpression('keep'), schema })
    if (!expression) throw new Error('expected expression to compile')

    const filtered = []
    for await (const result of filterBatches(asyncValues([batch]), expression)) filtered.push(result)

    expect(read).not.toHaveBeenCalled()
    expect(filtered).toHaveLength(1)
    expect(filtered[0].selection).toEqual({
      type: 'bitmap',
      values: new Uint8Array([1, 0, 1, 0]),
      length: 4,
    })
    expect(readBatchColumn({ batch: filtered[0], columnIndex: 1 })).toEqual({
      type: 'constant',
      value: 'large text',
      length: 2,
    })
    expect(read).toHaveBeenCalledTimes(1)
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
