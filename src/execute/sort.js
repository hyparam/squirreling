import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { compareForTerm } from './utils.js'

/**
 * @import { AsyncCells, AsyncRow, ExecuteContext, OrderByItem, QueryResults, SqlPrimitive } from '../types.js'
 * @import { SortNode, TopNNode } from '../plan/types.js'
 */

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
      // Buffer all rows (cells stay lazy — see multi-pass below)
      /** @type {AsyncRow[]} */
      const rows = []
      for await (const row of child.rows()) {
        if (context.signal?.aborted) return
        rows.push(row)
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
 * @typedef {{ row: AsyncRow, keys: SqlPrimitive[] }} HeapEntry
 * `keys` grows lazily: keys[i] is populated only when the i-th ORDER BY term
 * is actually needed for a comparison involving this entry.
 */

/**
 * Resolves the i-th sort key for a heap entry, memoizing it on the entry.
 * Fills any earlier unresolved positions to keep keys.length === resolved count.
 *
 * @param {HeapEntry} entry
 * @param {number} i
 * @param {OrderByItem[]} orderBy
 * @param {ExecuteContext} context
 * @returns {Promise<SqlPrimitive>}
 */
async function resolveKey(entry, i, orderBy, context) {
  while (entry.keys.length <= i) {
    const idx = entry.keys.length
    entry.keys.push(await evaluateExpr({ node: orderBy[idx].expr, row: entry.row, context }))
  }
  return entry.keys[i]
}

/**
 * Compares two heap entries lazily across ORDER BY terms: resolves the i-th
 * key for each entry only when earlier terms have tied. Already-resolved keys
 * are reused via the entry's `keys` cache.
 *
 * @param {HeapEntry} a
 * @param {HeapEntry} b
 * @param {OrderByItem[]} orderBy
 * @param {ExecuteContext} context
 * @returns {Promise<number>}
 */
async function compareLazy(a, b, orderBy, context) {
  for (let i = 0; i < orderBy.length; i++) {
    const av = await resolveKey(a, i, orderBy, context)
    const bv = await resolveKey(b, i, orderBy, context)
    const cmp = compareForTerm(av, bv, orderBy[i])
    if (cmp !== 0) return cmp
  }
  return 0
}

/**
 * Splices already-resolved sort keys back into the row's cells so downstream
 * consumers reading the sort-key columns don't re-evaluate them. Only safe
 * for identifier terms whose name is an output column of the row.
 *
 * @param {AsyncRow} row
 * @param {OrderByItem[]} orderBy
 * @param {SqlPrimitive[]} keys
 * @returns {AsyncRow}
 */
function withResolvedKeys(row, orderBy, keys) {
  /** @type {AsyncCells | undefined} */
  let cells
  for (let i = 0; i < orderBy.length && i < keys.length; i++) {
    const { expr } = orderBy[i]
    if (expr.type === 'identifier' && row.columns.includes(expr.name)) {
      if (!cells) cells = { ...row.cells }
      const val = keys[i]
      cells[expr.name] = () => Promise.resolve(val)
    }
  }
  return cells ? { columns: row.columns, cells } : row
}

/**
 * Executes a TopN operation (ORDER BY + LIMIT fused) using a bounded heap.
 * Memory usage is O(limit) instead of O(total rows). Sort keys are evaluated
 * lazily per-entry, so multi-column ORDER BY only pays for later terms when
 * earlier terms tie. Non-sort cells are never materialized by TopN — they
 * stay lazy for the downstream consumer.
 *
 * @param {TopNNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executeTopN(plan, context) {
  const child = executePlan({ plan: plan.child, context })
  const { limit, orderBy } = plan
  const numRows = child.numRows !== undefined ? Math.min(limit, child.numRows) : undefined
  const maxRows = Math.min(limit, child.maxRows ?? limit)
  return {
    columns: child.columns,
    numRows,
    maxRows,
    async *rows() {
      if (limit <= 0) return

      // Bounded max-heap: heap[0] is the worst entry (largest for ASC).
      // When a new row beats the worst, replace it.
      /** @type {HeapEntry[]} */
      const heap = []

      /** @param {number} i */
      async function siftDown(i) {
        const n = heap.length
        while (true) {
          let worst = i
          const left = 2 * i + 1
          const right = 2 * i + 2
          if (left < n && await compareLazy(heap[left], heap[worst], orderBy, context) > 0) worst = left
          if (right < n && await compareLazy(heap[right], heap[worst], orderBy, context) > 0) worst = right
          if (worst === i) break
          const tmp = heap[i]
          heap[i] = heap[worst]
          heap[worst] = tmp
          i = worst
        }
      }

      /** @param {number} i */
      async function siftUp(i) {
        while (i > 0) {
          const parent = i - 1 >> 1
          if (await compareLazy(heap[i], heap[parent], orderBy, context) <= 0) break
          const tmp = heap[i]
          heap[i] = heap[parent]
          heap[parent] = tmp
          i = parent
        }
      }

      for await (const row of child.rows()) {
        if (context.signal?.aborted) return

        /** @type {HeapEntry} */
        const entry = { row, keys: [] }

        if (heap.length < limit) {
          heap.push(entry)
          await siftUp(heap.length - 1)
        } else if (await compareLazy(entry, heap[0], orderBy, context) < 0) {
          // New row sorts before the worst in heap — replace it
          heap[0] = entry
          await siftDown(0)
        }
        // Otherwise discard — worse than everything in the heap
      }

      // Final sort of survivors. Resolve any keys still missing so we can
      // use a synchronous comparator.
      for (const entry of heap) {
        for (let i = 0; i < orderBy.length; i++) {
          await resolveKey(entry, i, orderBy, context)
        }
      }
      heap.sort((a, b) => {
        for (let i = 0; i < orderBy.length; i++) {
          const cmp = compareForTerm(a.keys[i], b.keys[i], orderBy[i])
          if (cmp !== 0) return cmp
        }
        return 0
      })

      for (const entry of heap) {
        yield withResolvedKeys(entry.row, orderBy, entry.keys)
      }
    },
  }
}
