import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { compareForTerm } from './utils.js'

/**
 * @import { AsyncRow, ExecuteContext, SqlPrimitive } from '../types.js'
 * @import { SortNode } from '../plan/types.js'
 */

/**
 * Executes a sort operation (ORDER BY)
 *
 * @param {SortNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
export async function* executeSort(plan, context) {
  // Buffer all rows
  /** @type {AsyncRow[]} */
  const rows = []
  for await (const row of executePlan(plan.child, context)) {
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
}
