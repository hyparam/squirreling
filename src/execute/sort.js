import { derivedAlias } from '../expression/alias.js'
import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { compareForTerm } from './utils.js'

/**
 * @import { AsyncRow, ExecuteContext, QueryResults, SqlPrimitive } from '../types.js'
 * @import { SortNode } from '../plan/types.js'
 */

const MAX_CHUNK = 256

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
      // Buffer all rows
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

          // Evaluate this column for all rows in the group, in parallel
          // chunks that double up to MAX_CHUNK so a slow UDF doesn't serialize.
          // Cache each value back into the row so downstream projection can
          // reuse it instead of re-invoking the expression.
          const alias = derivedAlias(term.expr)
          /** @type {number[]} */
          const missing = []
          for (const idx of group) {
            if (evaluatedValues[idx][orderByIdx] === undefined) missing.push(idx)
          }
          let chunkSize = 1
          let start = 0
          while (start < missing.length) {
            if (context.signal?.aborted) return
            const chunk = missing.slice(start, start + chunkSize)
            const values = await Promise.all(chunk.map(idx =>
              evaluateExpr({ node: term.expr, row: rows[idx], context })
            ))
            for (let i = 0; i < chunk.length; i++) {
              const idx = chunk[i]
              const value = values[i]
              evaluatedValues[idx][orderByIdx] = value
              if (!(alias in rows[idx].cells)) {
                rows[idx].cells[alias] = () => Promise.resolve(value)
              }
            }
            start += chunk.length
            chunkSize = Math.min(chunkSize * 2, MAX_CHUNK)
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
