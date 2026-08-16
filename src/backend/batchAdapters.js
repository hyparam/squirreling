import { readBatchColumn, selectVector, selectedRowCount, valueAt } from './batch.js'
import { yieldToEventLoop } from '../execute/yield.js'

/**
 * @import { AsyncBatch, ColumnResult, ColumnVector, RelationSchema, RowsToBatchesOptions } from '../internalTypes.js'
 * @import { AsyncCells, AsyncRow, SqlPrimitive } from '../types.js'
 */

const DEFAULT_BATCH_ROWS = 1024

/**
 * Materializes a legacy row stream into aligned batches. This is a source or
 * public compatibility boundary, not an operator implementation strategy.
 *
 * @param {AsyncIterable<AsyncRow>} rows
 * @param {RelationSchema} schema
 * @param {RowsToBatchesOptions} [options]
 * @yields {AsyncBatch}
 */
export async function* rowsToBatches(rows, schema, options) {
  const batchRows = options?.batchRows ?? DEFAULT_BATCH_ROWS
  if (!Number.isInteger(batchRows) || batchRows <= 0) {
    throw new RangeError(`batchRows must be a positive integer, got ${batchRows}`)
  }
  const names = schema.fields.map(function fieldName(field) { return field.name })
  let values = makeValueBuffers(names.length)
  let rowCount = 0

  for await (const row of rows) {
    options?.signal?.throwIfAborted()
    for (let columnIndex = 0; columnIndex < names.length; columnIndex++) {
      const name = names[columnIndex]
      values[columnIndex].push(await readRowCell(row, name))
    }
    rowCount++
    if (rowCount === batchRows) {
      yield loadedBatch(schema, values, rowCount)
      values = makeValueBuffers(names.length)
      rowCount = 0
    }
  }

  if (rowCount > 0) yield loadedBatch(schema, values, rowCount)
  options?.signal?.throwIfAborted()
}

/**
 * Exposes batches through the legacy lazy-cell row interface. Column reads
 * remain lazy and are memoized once per batch/selection by `readBatchColumn`.
 *
 * @param {AsyncIterable<AsyncBatch>} batches
 * @param {AbortSignal} [signal]
 * @yields {AsyncRow}
 */
export async function* batchesToRows(batches, signal) {
  for await (const batch of batches) {
    const columns = batch.schema.fields.map(function fieldName(field) { return field.name })
    const loadedVectors = batch.columns.map(function loadedVector(column) {
      return column.type === 'loaded'
        ? selectVector(column.vector, batch.selection)
        : undefined
    })
    const rowCount = selectedRowCount(batch.selection)
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      signal?.throwIfAborted()
      /** @type {AsyncCells} */
      const cells = {}
      for (let columnIndex = 0; columnIndex < columns.length; columnIndex++) {
        const currentColumn = columnIndex
        const currentRow = rowIndex
        const loadedVector = loadedVectors[columnIndex]
        if (loadedVector) {
          const value = valueAt(loadedVector, currentRow)
          cells[columns[columnIndex]] = function readLoadedCell() {
            return Promise.resolve(value)
          }
        } else {
          cells[columns[columnIndex]] = async function readCell() {
            const vector = await readBatchColumn({ batch, columnIndex: currentColumn, signal })
            return valueAt(vector, currentRow)
          }
        }
      }
      yield { columns, cells }
    }
  }
  signal?.throwIfAborted()
}

/**
 * Collects batches directly into the existing object-row result shape without
 * constructing compatibility `AsyncRow` values.
 *
 * @param {AsyncIterable<AsyncBatch>} batches
 * @param {AbortSignal} [signal]
 * @returns {Promise<Record<string, SqlPrimitive>[]>}
 */
export async function collectBatches(batches, signal) {
  /** @type {Record<string, SqlPrimitive>[]} */
  const rows = []
  for await (const batch of batches) {
    const names = batch.schema.fields.map(function fieldName(field) { return field.name })
    const results = batch.columns.map(function readColumn(_column, columnIndex) {
      return readBatchColumn({ batch, columnIndex, signal })
    })
    const vectors = await resolveColumns(results)
    const rowCount = selectedRowCount(batch.selection)
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      if (signal && rowIndex % 4000 === 0) {
        if (rowIndex > 0) await yieldToEventLoop()
        signal.throwIfAborted()
      }
      /** @type {Record<string, SqlPrimitive>} */
      const row = {}
      for (let columnIndex = 0; columnIndex < names.length; columnIndex++) {
        row[names[columnIndex]] = valueAt(vectors[columnIndex], rowIndex)
      }
      rows.push(row)
    }
  }
  signal?.throwIfAborted()
  return rows
}

/**
 * @param {AsyncRow} row
 * @param {string} name
 * @returns {Promise<SqlPrimitive>}
 */
async function readRowCell(row, name) {
  if (row.resolved && Object.prototype.hasOwnProperty.call(row.resolved, name)) {
    return row.resolved[name]
  }
  const cell = row.cells[name]
  if (!cell) throw new Error(`Row does not contain column "${name}"`)
  return await cell()
}

/**
 * @param {number} count
 * @returns {SqlPrimitive[][]}
 */
function makeValueBuffers(count) {
  /** @type {SqlPrimitive[][]} */
  const values = []
  for (let i = 0; i < count; i++) values.push([])
  return values
}

/**
 * @param {RelationSchema} schema
 * @param {SqlPrimitive[][]} values
 * @param {number} rowCount
 * @returns {AsyncBatch}
 */
function loadedBatch(schema, values, rowCount) {
  return {
    schema,
    selection: { type: 'all', length: rowCount },
    columns: values.map(function loadedColumn(columnValues) {
      return {
        type: 'loaded',
        vector: { type: 'values', values: columnValues, length: rowCount },
      }
    }),
  }
}

/**
 * Avoids a promise boundary when every column is already loaded synchronously.
 *
 * @param {ColumnResult[]} results
 * @returns {ColumnVector[] | Promise<ColumnVector[]>}
 */
function resolveColumns(results) {
  if (results.some(function isPromise(result) { return result instanceof Promise })) {
    return Promise.all(results)
  }
  /** @type {ColumnVector[]} */
  const vectors = []
  for (const result of results) {
    if (result instanceof Promise) throw new Error('Unexpected asynchronous column result')
    vectors.push(result)
  }
  return vectors
}
