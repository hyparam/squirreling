import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { compareForTerm } from './utils.js'

/**
 * @import { AsyncRow, ExecuteContext, QueryResults, SqlPrimitive } from '../types.js'
 * @import { SortNode, TopNNode } from '../plan/types.js'
 */

/**
 * Eagerly resolves all cell values in an AsyncRow, replacing closures with
 * plain value-returning functions. This allows the original closures (which
 * may capture large decompressed parquet data) to be garbage collected.
 *
 * @param {AsyncRow} row
 * @returns {Promise<AsyncRow>}
 */
async function materializeRow(row) {
  if (row.resolved) return row
  const { columns } = row
  /** @type {Record<string, import('../types.js').SqlPrimitive>} */
  const resolved = {}
  await Promise.all(columns.map(async col => {
    resolved[col] = await row.cells[col]()
  }))
  /** @type {import('../types.js').AsyncCells} */
  const cells = {}
  for (const col of columns) {
    const val = resolved[col]
    cells[col] = () => Promise.resolve(val)
  }
  return { columns, cells, resolved }
}

/**
 * Executes a sort operation (ORDER BY)
 *
 * @param {SortNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executeSort(plan, context) {
  const child = executePlan({ plan: plan.child, context })
  return {
    columns: child.columns,
    numRows: child.numRows,
    maxRows: child.maxRows,
    async *rows() {
      // Buffer all rows, materializing cells to release closures over parquet data
      /** @type {AsyncRow[]} */
      const rows = []
      for await (const row of child.rows()) {
        if (context.signal?.aborted) return
        rows.push(await materializeRow(row))
      }

      if (rows.length === 0) return

      // Multi-pass lazy sorting
      /** @type {(SqlPrimitive | undefined)[][]} */
      const evaluatedValues = rows.map(() => Array(plan.orderBy.length))

      /** @type {number[][]} */
      let groups = [rows.map((_, i) => i)]

      for (let orderByIdx = 0; orderByIdx < plan.orderBy.length; orderByIdx++) {
        const term = plan.orderBy[orderByIdx]
        /** @type {number[][]} */
        const nextGroups = []

        for (const group of groups) {
          if (group.length <= 1) {
            nextGroups.push(group)
            continue
          }

          // Evaluate this column for all rows in the group
          for (const idx of group) {
            if (evaluatedValues[idx][orderByIdx] === undefined) {
              evaluatedValues[idx][orderByIdx] = await evaluateExpr({
                node: term.expr,
                row: rows[idx],
                context,
              })
            }
          }

          // Sort the group by this column
          group.sort((aIdx, bIdx) => {
            const av = evaluatedValues[aIdx][orderByIdx]
            const bv = evaluatedValues[bIdx][orderByIdx]
            return compareForTerm(av, bv, term)
          })

          // Split into sub-groups based on ties
          if (orderByIdx < plan.orderBy.length - 1) {
            /** @type {number[]} */
            let currentSubGroup = [group[0]]
            for (let i = 1; i < group.length; i++) {
              const prevIdx = group[i - 1]
              const currIdx = group[i]
              const prevVal = evaluatedValues[prevIdx][orderByIdx]
              const currVal = evaluatedValues[currIdx][orderByIdx]

              if (compareForTerm(prevVal, currVal, term) === 0) {
                currentSubGroup.push(currIdx)
              } else {
                nextGroups.push(currentSubGroup)
                currentSubGroup = [currIdx]
              }
            }
            nextGroups.push(currentSubGroup)
          } else {
            nextGroups.push(group)
          }
        }

        groups = nextGroups
      }

      // Yield sorted rows
      for (const idx of groups.flat()) {
        yield rows[idx]
      }
    },
  }
}

/**
 * Compares two entries by their sort keys across all ORDER BY terms.
 *
 * @param {SqlPrimitive[]} aKeys
 * @param {SqlPrimitive[]} bKeys
 * @param {import('../types.js').OrderByItem[]} orderBy
 * @returns {number}
 */
function compareKeys(aKeys, bKeys, orderBy) {
  for (let i = 0; i < orderBy.length; i++) {
    const cmp = compareForTerm(aKeys[i], bKeys[i], orderBy[i])
    if (cmp !== 0) return cmp
  }
  return 0
}

/**
 * Executes a TopN operation (ORDER BY + LIMIT fused) using a bounded heap.
 * Memory usage is O(limit) instead of O(total rows).
 *
 * @param {TopNNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executeTopN(plan, context) {
  const child = executePlan({ plan: plan.child, context })
  return {
    columns: child.columns,
    numRows: Math.min(plan.limit, child.numRows ?? plan.limit),
    maxRows: Math.min(plan.limit, child.maxRows ?? plan.limit),
    async *rows() {
      if (plan.limit <= 0) return

      // Bounded max-heap: heap[0] is the worst (largest for ASC) entry.
      // When a new row is better than the worst, replace it.
      /** @type {{ row: AsyncRow, keys: SqlPrimitive[] }[]} */
      const heap = []
      const limit = plan.limit

      /** @param {number} i */
      function siftDown(i) {
        const n = heap.length
        while (true) {
          let worst = i
          const left = 2 * i + 1
          const right = 2 * i + 2
          if (left < n && compareKeys(heap[left].keys, heap[worst].keys, plan.orderBy) > 0) worst = left
          if (right < n && compareKeys(heap[right].keys, heap[worst].keys, plan.orderBy) > 0) worst = right
          if (worst === i) break
          const tmp = heap[i]
          heap[i] = heap[worst]
          heap[worst] = tmp
          i = worst
        }
      }

      /** @param {number} i */
      function siftUp(i) {
        while (i > 0) {
          const parent = (i - 1) >> 1
          if (compareKeys(heap[i].keys, heap[parent].keys, plan.orderBy) <= 0) break
          const tmp = heap[i]
          heap[i] = heap[parent]
          heap[parent] = tmp
          i = parent
        }
      }

      for await (const row of child.rows()) {
        if (context.signal?.aborted) return

        const keys = await Promise.all(plan.orderBy.map(term =>
          evaluateExpr({ node: term.expr, row, context })
        ))

        if (heap.length < limit) {
          heap.push({ row: await materializeRow(row), keys })
          siftUp(heap.length - 1)
        } else if (compareKeys(keys, heap[0].keys, plan.orderBy) < 0) {
          // New row sorts before the worst in heap — replace it
          heap[0] = { row: await materializeRow(row), keys }
          siftDown(0)
        }
        // Otherwise discard — worse than everything in the heap
      }

      // Extract in sorted order
      const sorted = heap.sort((a, b) => compareKeys(a.keys, b.keys, plan.orderBy))
      for (const entry of sorted) {
        yield entry.row
      }
    },
  }
}
