import { evaluateExpr } from '../expression/evaluate.js'
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

      for (let i = 0; i < rows.length; i++) {
        if (context.signal?.aborted) return
        const row = rows[i]
        const cells = { ...row.cells }
        for (let w = 0; w < plan.windows.length; w++) {
          const { alias } = plan.windows[w]
          const value = windowValues[w][i]
          cells[alias] = () => Promise.resolve(value)
        }
        yield {
          columns: [...row.columns, ...extraColumns],
          cells,
        }
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
