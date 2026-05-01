import { evaluateExpr } from '../expression/evaluate.js'
import { materializeRow } from './cells.js'
import { executePlan } from './execute.js'
import { compareForTerm, keyify } from './utils.js'

/**
 * @import { AsyncRow, ExecuteContext, QueryResults, SqlPrimitive } from '../types.js'
 * @import { WindowNode, WindowSpec } from '../plan/types.js'
 */

/**
 * Executes a Window plan node: buffers the child's rows, assigns each window
 * function's output per partition, and yields rows in input order with the
 * synthetic window cells attached.
 *
 * @param {WindowNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executeWindow(plan, context) {
  const child = executePlan({ plan: plan.child, context })
  const extraColumns = plan.windows.map(w => w.alias)

  // Streaming fast path: every window is OVER () with no partition/order, so
  // each row's output depends only on its position in the input stream. Avoids
  // buffering — critical for large scans (e.g. parquet).
  const streamable = plan.windows.every(w => w.partitionBy.length === 0 && w.orderBy.length === 0)

  if (streamable) {
    return {
      columns: [...child.columns, ...extraColumns],
      numRows: child.numRows,
      maxRows: child.maxRows,
      async *rows() {
        let i = 0
        for await (const row of child.rows()) {
          if (context.signal?.aborted) return
          i++
          const cells = { ...row.cells }
          for (const w of plan.windows) {
            cells[w.alias] = assignRowNumber(w.funcName, i - 1)
          }
          yield {
            columns: [...row.columns, ...extraColumns],
            cells,
          }
        }
      },
    }
  }

  return {
    columns: [...child.columns, ...extraColumns],
    numRows: child.numRows,
    maxRows: child.maxRows,
    async *rows() {
      /** @type {AsyncRow[]} */
      const rows = []
      for await (const row of child.rows()) {
        if (context.signal?.aborted) return
        rows.push(row)
      }
      if (rows.length === 0) return

      // One SqlPrimitive per window spec per row, indexed by row input position.
      /** @type {SqlPrimitive[][]} */
      const windowValues = plan.windows.map(() => new Array(rows.length))

      for (let w = 0; w < plan.windows.length; w++) {
        await computeWindow(plan.windows[w], rows, windowValues[w], context)
        if (context.signal?.aborted) return
      }

      // Materialize each buffered row before yielding so the output's cell
      // map doesn't carry source-row closures (and through them, upstream
      // page/buffer state). We've already paid the buffering cost; lazy
      // evaluation past this point would only multiply retention across the
      // window's output set.
      for (let i = 0; i < rows.length; i++) {
        if (context.signal?.aborted) return
        const slim = await materializeRow(rows[i])
        const cells = { ...slim.cells }
        for (let w = 0; w < plan.windows.length; w++) {
          cells[plan.windows[w].alias] = windowValues[w][i]
        }
        yield {
          columns: [...slim.columns, ...extraColumns],
          cells,
        }
        // Drop the original buffered row reference so downstream draining
        // can let GC reclaim the upstream page that held it.
        rows[i] = slim
      }
    },
  }
}

/**
 * Computes a single window function across all rows, writing the per-row
 * output values into `output`.
 *
 * @param {WindowSpec} spec
 * @param {AsyncRow[]} rows
 * @param {SqlPrimitive[]} output
 * @param {ExecuteContext} context
 */
async function computeWindow(spec, rows, output, context) {
  // Bucket row indices by partition key.
  /** @type {Map<string | number | bigint | boolean, number[]>} */
  const partitions = new Map()
  const partitionKeys = await Promise.all(rows.map(row =>
    Promise.all(spec.partitionBy.map(expr => evaluateExpr({ node: expr, row, context })))
  ))
  for (let i = 0; i < rows.length; i++) {
    const key = keyify(...partitionKeys[i])
    let bucket = partitions.get(key)
    if (!bucket) {
      bucket = []
      partitions.set(key, bucket)
    }
    bucket.push(i)
  }

  for (const bucket of partitions.values()) {
    if (context.signal?.aborted) return

    // Order within the partition. Empty ORDER BY → input order.
    if (spec.orderBy.length) {
      const orderValues = await Promise.all(bucket.map(idx =>
        Promise.all(spec.orderBy.map(term => evaluateExpr({ node: term.expr, row: rows[idx], context })))
      ))
      /** @type {{ idx: number, values: SqlPrimitive[], pos: number }[]} */
      const entries = bucket.map((idx, k) => ({ idx, values: orderValues[k], pos: k }))
      entries.sort((a, b) => {
        for (let i = 0; i < spec.orderBy.length; i++) {
          const cmp = compareForTerm(a.values[i], b.values[i], spec.orderBy[i])
          if (cmp !== 0) return cmp
        }
        return a.pos - b.pos
      })
      for (let k = 0; k < entries.length; k++) {
        output[entries[k].idx] = assignRowNumber(spec.funcName, k)
      }
    } else {
      for (let k = 0; k < bucket.length; k++) {
        output[bucket[k]] = assignRowNumber(spec.funcName, k)
      }
    }
  }
}

/**
 * @param {string} funcName
 * @param {number} rank - 0-based rank within the partition
 * @returns {SqlPrimitive}
 */
function assignRowNumber(funcName, rank) {
  if (funcName === 'ROW_NUMBER') return rank + 1
  throw new Error(`Unsupported window function: ${funcName}`)
}
