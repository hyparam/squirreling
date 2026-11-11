import { readBatchColumn, resolveColumnResults, selectedRowCount, valueAt } from '../backend/batch.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { AsyncRow, QueryResults, SqlPrimitive } from '../types.js'
 */

/**
 * Streams resolved object rows in chunks, without collecting the full result.
 * Native column reads stay grouped by source batch; fallback cell reads run
 * concurrently within each chunk. Memory also includes source batches and
 * operator state. Uses the signal passed to executeSql or executePlan.
 * Breaking iteration closes the upstream iterator.
 *
 * @param {QueryResults} results
 * @param {{ batchRows?: number }} [options]
 * @yields {Record<string, SqlPrimitive>[]}
 */
export async function* streamRowBatches(results, { batchRows = 1024 } = {}) {
  const { signal } = results
  let sinceYield = 0
  if (!Number.isSafeInteger(batchRows) || batchRows < 1) throw new RangeError('batchRows must be a positive integer')
  signal?.throwIfAborted()
  if (results.batches) {
    for await (const batch of results.batches()) {
      signal?.throwIfAborted()
      const count = selectedRowCount(batch.selection)
      if (!count) continue
      const vectors = await resolveColumnResults(batch.columns.map((_, columnIndex) => readBatchColumn({ batch, columnIndex, signal })))
      for (let start = 0; start < count; start += batchRows) {
        signal?.throwIfAborted()
        const end = Math.min(start + batchRows, count)
        /** @type {Record<string, SqlPrimitive>[]} */
        const chunk = []
        for (let i = start; i < end; i++) {
          /** @type {Record<string, SqlPrimitive>} */
          const row = {}
          for (let j = 0; j < results.columns.length; j++) row[results.columns[j]] = valueAt(vectors[j], i)
          chunk.push(row)
          if (++sinceYield === 4000) {
            await yieldToEventLoop()
            signal?.throwIfAborted()
            sinceYield = 0
          }
        }
        signal?.throwIfAborted()
        yield chunk
      }
    }
  } else {
    /** @type {Record<string, SqlPrimitive>[]} */
    let chunk = []
    /** @type {{ row: AsyncRow, item: Record<string, SqlPrimitive> }[]} */
    let lazy = []
    for await (const row of results.rows()) {
      signal?.throwIfAborted()
      /** @type {Record<string, SqlPrimitive>} */
      const item = {}
      if (row.resolved) {
        for (const key of row.columns) item[key] = row.resolved[key]
      } else lazy.push({ row, item })
      chunk.push(item)
      if (++sinceYield === 4000) {
        await yieldToEventLoop()
        signal?.throwIfAborted()
        sinceYield = 0
      }
      if (chunk.length === batchRows) {
        await resolveLazy(lazy)
        signal?.throwIfAborted()
        const ready = chunk
        chunk = []
        lazy = []
        yield ready
      }
    }
    if (chunk.length) {
      await resolveLazy(lazy)
      signal?.throwIfAborted()
      yield chunk
    }
  }
  signal?.throwIfAborted()
}

/**
 * @param {{ row: AsyncRow, item: Record<string, SqlPrimitive> }[]} lazy
 * @returns {Promise<void>}
 */
async function resolveLazy(lazy) {
  await Promise.all(lazy.map(async ({ row, item }) => {
    const values = await Promise.all(row.columns.map(async key => row.cells[key]()))
    for (let i = 0; i < values.length; i++) item[row.columns[i]] = values[i]
  }))
}
