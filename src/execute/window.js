import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { compareForTerm, keyify } from './utils.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { AsyncRow, ExecuteContext, QueryResults, SqlPrimitive } from '../types.js'
 * @import { WindowNode, WindowSpec } from '../plan/types.js'
 */

// Yield to the event loop every 4000 iterations so that aborts can actually fire
const YIELD_INTERVAL = 4000

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

  // Streaming fast path: every window is a positional function (e.g.
  // ROW_NUMBER) with OVER () — no partition/order — so each row's output
  // depends only on its index in the input stream. Avoids buffering, which
  // matters for large scans (e.g. parquet).
  const streamable = plan.windows.every(w =>
    w.funcName === 'ROW_NUMBER' && w.partitionBy.length === 0 && w.orderBy.length === 0
  )

  if (streamable) {
    return {
      columns: [...child.columns, ...extraColumns],
      numRows: child.numRows,
      maxRows: child.maxRows,
      async *rows() {
        let i = 0
        for await (const row of child.rows()) {
          if (++i % YIELD_INTERVAL === 0) {
            await yieldToEventLoop()
            context.signal?.throwIfAborted()
          }
          const cells = { ...row.cells }
          for (const w of plan.windows) {
            const value = i
            cells[w.alias] = () => Promise.resolve(value)
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
      let collectCount = 0
      for await (const row of child.rows()) {
        if (++collectCount % YIELD_INTERVAL === 0) {
          await yieldToEventLoop()
          context.signal?.throwIfAborted()
        }
        rows.push(row)
      }
      if (rows.length === 0) return

      // One SqlPrimitive per window spec per row, indexed by row input position.
      /** @type {SqlPrimitive[][]} */
      const windowValues = plan.windows.map(() => new Array(rows.length))

      for (let w = 0; w < plan.windows.length; w++) {
        await computeWindow(plan.windows[w], rows, windowValues[w], context)
        context.signal?.throwIfAborted()
      }

      let emitCount = 0
      for (let i = 0; i < rows.length; i++) {
        if (++emitCount % YIELD_INTERVAL === 0) {
          await yieldToEventLoop()
          context.signal?.throwIfAborted()
        }
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
  for (let chunkStart = 0; chunkStart < rows.length; chunkStart += YIELD_INTERVAL) {
    if (chunkStart > 0) {
      await yieldToEventLoop()
      context.signal?.throwIfAborted()
    }
    const chunkEnd = Math.min(chunkStart + YIELD_INTERVAL, rows.length)
    const chunkKeys = await Promise.all(
      rows.slice(chunkStart, chunkEnd).map(row =>
        Promise.all(spec.partitionBy.map(expr => evaluateExpr({ node: expr, row, context })))
      )
    )
    for (let j = 0; j < chunkKeys.length; j++) {
      const key = keyify(...chunkKeys[j])
      let bucket = partitions.get(key)
      if (!bucket) {
        bucket = []
        partitions.set(key, bucket)
      }
      bucket.push(chunkStart + j)
    }
  }

  for (const bucket of partitions.values()) {
    context.signal?.throwIfAborted()

    // Order within the partition. Empty ORDER BY → input order.
    /** @type {number[]} */
    let ordered
    if (spec.orderBy.length) {
      /** @type {{ idx: number, values: SqlPrimitive[], pos: number }[]} */
      const entries = new Array(bucket.length)
      for (let chunkStart = 0; chunkStart < bucket.length; chunkStart += YIELD_INTERVAL) {
        if (chunkStart > 0) {
          await yieldToEventLoop()
          context.signal?.throwIfAborted()
        }
        const chunkEnd = Math.min(chunkStart + YIELD_INTERVAL, bucket.length)
        const chunkValues = await Promise.all(
          bucket.slice(chunkStart, chunkEnd).map(idx =>
            Promise.all(spec.orderBy.map(term => evaluateExpr({ node: term.expr, row: rows[idx], context })))
          )
        )
        for (let j = 0; j < chunkValues.length; j++) {
          const k = chunkStart + j
          entries[k] = { idx: bucket[k], values: chunkValues[j], pos: k }
        }
      }
      entries.sort((a, b) => {
        for (let i = 0; i < spec.orderBy.length; i++) {
          const cmp = compareForTerm(a.values[i], b.values[i], spec.orderBy[i])
          if (cmp !== 0) return cmp
        }
        return a.pos - b.pos
      })
      ordered = entries.map(e => e.idx)
    } else {
      ordered = bucket
    }

    await applyWindowFunction(spec, ordered, rows, output, context)
  }
}

/**
 * Computes window function values for a single partition's rows in order.
 *
 * @param {WindowSpec} spec
 * @param {number[]} ordered - row indices in window order
 * @param {AsyncRow[]} rows
 * @param {SqlPrimitive[]} output
 * @param {ExecuteContext} context
 */
async function applyWindowFunction(spec, ordered, rows, output, context) {
  if (spec.funcName === 'ROW_NUMBER') {
    for (let k = 0; k < ordered.length; k++) {
      output[ordered[k]] = k + 1
    }
    return
  }
  if (spec.funcName === 'LAG' || spec.funcName === 'LEAD') {
    const direction = spec.funcName === 'LAG' ? -1 : 1
    const [valueExpr, offsetExpr, defaultExpr] = spec.args
    let tick = 0
    for (let k = 0; k < ordered.length; k++) {
      if (++tick % YIELD_INTERVAL === 0) {
        await yieldToEventLoop()
        context.signal?.throwIfAborted()
      }
      const idx = ordered[k]
      const row = rows[idx]
      const offset = offsetExpr
        ? Number(await evaluateExpr({ node: offsetExpr, row, context }))
        : 1
      const target = k + direction * offset
      if (target >= 0 && target < ordered.length) {
        output[idx] = await evaluateExpr({ node: valueExpr, row: rows[ordered[target]], context })
      } else if (defaultExpr) {
        output[idx] = await evaluateExpr({ node: defaultExpr, row, context })
      } else {
        output[idx] = null
      }
    }
    return
  }
  throw new Error(`Unsupported window function: ${spec.funcName}`)
}
