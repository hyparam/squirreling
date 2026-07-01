import { asyncRow } from '../backend/dataSource.js'
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
        context.signal?.throwIfAborted()
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
          // Cache the evaluated sort key back onto the row so the output
          // projection can reuse it instead of recomputing (e.g. an ORDER BY UDF
          // also referenced in SELECT). Skip real columns (read directly) and
          // rows with no cells map.
          const entryRow = entries[idx].row
          if (cacheValues && entryRow.cells && !entryRow.columns.includes(alias) && !(alias in entryRow.cells)) {
            entryRow.cells[alias] = () => Promise.resolve(value)
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
      // ORDER BY must buffer its whole input before it can emit. For rows that
      // are already fully materialized (`resolved` present), keep only the plain
      // object plus a fresh empty cells map, dropping the O(columns) per-column
      // cell closures: the buffer then holds row data, not N sets of closures.
      // The empty cells map still lets the sort cache derived sort keys. Rows
      // without `resolved` (e.g. derived expressions) are kept as-is so their
      // lazy cells still work.
      /** @type {AsyncRow[]} */
      const rows = []
      for await (const row of child.rows()) {
        if (context.signal?.aborted) return
        if (row.resolved) {
          rows.push({ columns: row.columns, cells: {}, resolved: row.resolved })
        } else {
          rows.push(row)
        }
      }

      const sortedRows = await sortEntriesByTerms({
        entries: rows.map(row => ({ row })),
        orderBy: plan.orderBy,
        context,
        cacheValues: true,
      })

      // Rebuild full cell closures for lean rows only at emit time, one row at a
      // time, so downstream consumers get the normal cells interface without the
      // buffer ever holding N sets of closures. A buffered row is lean exactly
      // when it carries `resolved` (non-materialized rows are kept as-is above).
      // Carry over any cached derived sort-key cells added during the sort.
      for (const { row } of sortedRows) {
        if (!row.resolved) {
          yield row
          continue
        }
        const rebuilt = asyncRow(row.resolved, row.columns)
        for (const key in row.cells) rebuilt.cells[key] = row.cells[key]
        yield rebuilt
      }
    },
  }
}
