import { derivedAlias } from '../expression/alias.js'
import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { compareForTerm } from './utils.js'

/**
 * @import { AsyncRow, ExecuteContext, OrderByItem, QueryResults, SqlPrimitive } from '../types.js'
 * @import { SortNode } from '../plan/types.js'
 */

const MAX_CHUNK = 256

/**
 * @typedef {{
 *   row: AsyncRow,
 *   rows?: AsyncRow[],
 * }} SortEntry
 */

/**
 * Sorts rows by ORDER BY terms while evaluating async sort keys in concurrent
 * chunks and delaying later terms until earlier terms tie.
 *
 * @template {SortEntry} T
 * @param {{
 *   entries: T[],
 *   orderBy: OrderByItem[],
 *   context: ExecuteContext,
 *   cacheValues?: boolean,
 * }} options
 * @returns {Promise<T[]>}
 */
export async function sortEntriesByTerms({ entries, orderBy, context, cacheValues = false }) {
  if (entries.length === 0) return []

  /** @type {(SqlPrimitive | undefined)[][]} */
  const evaluatedValues = entries.map(() => Array(orderBy.length))

  /** @type {number[][]} */
  let groups = [entries.map((_, i) => i)]

  for (let orderByIdx = 0; orderByIdx < orderBy.length; orderByIdx++) {
    const term = orderBy[orderByIdx]
    /** @type {number[][]} */
    const nextGroups = []

    for (const group of groups) {
      if (group.length <= 1) {
        nextGroups.push(group)
        continue
      }

      const alias = derivedAlias(term.expr)
      /** @type {number[]} */
      const missing = []
      for (const idx of group) {
        if (evaluatedValues[idx][orderByIdx] === undefined) missing.push(idx)
      }
      let chunkSize = 1
      let start = 0
      while (start < missing.length) {
        if (context.signal?.aborted) return []
        const chunk = missing.slice(start, start + chunkSize)
        const values = await Promise.all(chunk.map(idx =>
          evaluateExpr({
            node: term.expr,
            row: entries[idx].row,
            rows: entries[idx].rows,
            context,
          })
        ))
        for (let i = 0; i < chunk.length; i++) {
          const idx = chunk[i]
          const value = values[i]
          evaluatedValues[idx][orderByIdx] = value
          if (cacheValues && !(alias in entries[idx].row.cells)) {
            entries[idx].row.cells[alias] = () => Promise.resolve(value)
          }
        }
        start += chunk.length
        chunkSize = Math.min(chunkSize * 2, MAX_CHUNK)
      }

      group.sort((aIdx, bIdx) => {
        const av = evaluatedValues[aIdx][orderByIdx]
        const bv = evaluatedValues[bIdx][orderByIdx]
        return compareForTerm(av, bv, term)
      })

      if (orderByIdx < orderBy.length - 1) {
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

  return groups.flat().map(idx => entries[idx])
}

/**
 * @typedef {{ row: AsyncRow, keys: SqlPrimitive[], evaluated: number }} HeapEntry
 */

/**
 * Bounded max-heap keyed by ORDER BY terms. The root is always the entry that
 * sorts LAST among the current top-K, so each new row needs at most one
 * comparison against the worst retained candidate to decide whether to evict.
 *
 * Sort keys are evaluated lazily, term by term: comparisons stop as soon as
 * an earlier term breaks the tie, matching the laziness of the full-sort
 * path (sortEntriesByTerms). This keeps later-term expressions (often
 * expensive — derived columns, async cells) unevaluated unless required.
 *
 * Memory is O(k) regardless of input size; per-row cost is O(log k).
 */
class TopKHeap {
  /**
   * @param {number} k
   * @param {OrderByItem[]} orderBy
   * @param {ExecuteContext} context
   */
  constructor(k, orderBy, context) {
    this.k = k
    this.orderBy = orderBy
    this.context = context
    /** @type {HeapEntry[]} */
    this.heap = []
  }

  /**
   * Ensures `entry` has term `i` evaluated, evaluating any earlier missing
   * terms in order so the keys array stays dense.
   *
   * @param {HeapEntry} entry
   * @param {number} i
   */
  async _ensure(entry, i) {
    while (entry.evaluated <= i) {
      const term = this.orderBy[entry.evaluated]
      const value = await evaluateExpr({ node: term.expr, row: entry.row, context: this.context })
      entry.keys.push(value)
      entry.evaluated++
    }
  }

  /**
   * @param {HeapEntry} a
   * @param {HeapEntry} b
   * @returns {Promise<number>}
   */
  async _compare(a, b) {
    for (let i = 0; i < this.orderBy.length; i++) {
      await this._ensure(a, i)
      await this._ensure(b, i)
      const cmp = compareForTerm(a.keys[i], b.keys[i], this.orderBy[i])
      if (cmp !== 0) return cmp
    }
    return 0
  }

  /**
   * Adds the entry if the heap isn't full, or replaces the worst retained
   * entry when the new one sorts earlier than it.
   *
   * @param {HeapEntry} entry
   */
  async consider(entry) {
    if (this.k === 0) return
    if (this.heap.length < this.k) {
      this.heap.push(entry)
      await this._siftUp(this.heap.length - 1)
      return
    }
    if (await this._compare(entry, this.heap[0]) < 0) {
      this.heap[0] = entry
      await this._siftDown(0)
    }
  }

  /**
   * Returns the retained entries in sorted order (best first). Implemented
   * as a heapsort so that lazy comparisons stay lazy: each pop bubbles up
   * the next-worst entry using the same async comparator, evaluating later
   * sort terms only when earlier ones tie.
   *
   * @returns {Promise<HeapEntry[]>}
   */
  async drain() {
    /** @type {HeapEntry[]} */
    const result = []
    while (this.heap.length > 0) {
      const top = this.heap[0]
      const last = this.heap[this.heap.length - 1]
      this.heap.length--
      if (this.heap.length > 0) {
        this.heap[0] = last
        await this._siftDown(0)
      }
      result.push(top)
    }
    // Heapsort produced worst-to-best; reverse for best-to-worst output.
    result.reverse()
    return result
  }

  /** @param {number} i */
  async _siftUp(i) {
    const { heap } = this
    while (i > 0) {
      const parent = i - 1 >> 1
      if (await this._compare(heap[i], heap[parent]) > 0) {
        const tmp = heap[i]
        heap[i] = heap[parent]
        heap[parent] = tmp
        i = parent
      } else {
        return
      }
    }
  }

  /** @param {number} i */
  async _siftDown(i) {
    const { heap } = this
    const n = heap.length
    while (true) {
      const l = 2 * i + 1
      const r = 2 * i + 2
      let largest = i
      if (l < n && await this._compare(heap[l], heap[largest]) > 0) largest = l
      if (r < n && await this._compare(heap[r], heap[largest]) > 0) largest = r
      if (largest === i) return
      const tmp = heap[i]
      heap[i] = heap[largest]
      heap[largest] = tmp
      i = largest
    }
  }
}

/**
 * Top-K execution path: maintains a bounded heap of size `limit` while
 * iterating the child stream. Memory stays O(limit) regardless of input size.
 *
 * @param {OrderByItem[]} orderBy
 * @param {number} limit
 * @param {QueryResults} child
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
async function* executeTopK(orderBy, limit, child, context) {
  if (limit === 0) return

  const heap = new TopKHeap(limit, orderBy, context)

  // Pre-evaluate the first sort key for buffered chunks in parallel: most
  // rows will be discarded by the first-key comparison alone, so this avoids
  // await-per-row on the hot path while preserving lazy evaluation for later
  // keys (handled inside the heap's comparator).
  /** @type {AsyncRow[]} */
  let buffer = []
  let chunkSize = 1

  async function flush() {
    if (buffer.length === 0) return
    if (context.signal?.aborted) return
    const firstTerm = orderBy[0]
    const firstKeys = await Promise.all(buffer.map(row =>
      evaluateExpr({ node: firstTerm.expr, row, context })
    ))
    if (context.signal?.aborted) return
    for (let i = 0; i < buffer.length; i++) {
      await heap.consider({ row: buffer[i], keys: [firstKeys[i]], evaluated: 1 })
    }
    buffer = []
  }

  for await (const row of child.rows()) {
    if (context.signal?.aborted) return
    buffer.push(row)
    if (buffer.length >= chunkSize) {
      await flush()
      chunkSize = Math.min(chunkSize * 2, MAX_CHUNK)
    }
  }
  await flush()
  if (context.signal?.aborted) return

  const sorted = await heap.drain()
  for (const { row, keys, evaluated } of sorted) {
    if (context.signal?.aborted) return
    // Cache any evaluated sort key values on the row so downstream operators
    // can reuse them (matches the cacheValues=true behavior of the full
    // sort). Keys that the lazy comparator never needed remain unevaluated.
    for (let i = 0; i < evaluated; i++) {
      const alias = derivedAlias(orderBy[i].expr)
      if (!(alias in row.cells)) {
        const value = keys[i]
        row.cells[alias] = () => Promise.resolve(value)
      }
    }
    yield row
  }
}

/**
 * Caps a row-count estimate by the top-K limit when present.
 *
 * @param {number | undefined} estimate
 * @param {number | undefined} limit
 * @returns {number | undefined}
 */
function capByLimit(estimate, limit) {
  if (limit === undefined) return estimate
  if (estimate === undefined) return limit
  return Math.min(estimate, limit)
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
    numRows: capByLimit(child.numRows, plan.limit),
    maxRows: capByLimit(child.maxRows, plan.limit),
    async *rows() {
      // Top-K heap path: bounded memory when LIMIT is small.
      if (plan.limit !== undefined) {
        yield* executeTopK(plan.orderBy, plan.limit, child, context)
        return
      }

      // Full sort path: buffer all rows, then sort.
      /** @type {AsyncRow[]} */
      const rows = []
      for await (const row of child.rows()) {
        if (context.signal?.aborted) return
        rows.push(row)
      }

      const sortedRows = await sortEntriesByTerms({
        entries: rows.map(row => ({ row })),
        orderBy: plan.orderBy,
        context,
        cacheValues: true,
      })

      // Yield sorted rows
      for (const { row } of sortedRows) {
        yield row
      }
    },
  }
}
