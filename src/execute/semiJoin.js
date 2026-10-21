import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { sqlEquals } from './utils.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { ExecuteContext, QueryResults, SqlPrimitive } from '../types.js'
 * @import { SemiJoinNode } from '../plan/types.js'
 */

/**
 * Executes WHERE IN / NOT IN with one build of the right input per iteration.
 * The membership set belongs to this operator, never to the context or AST.
 *
 * @param {SemiJoinNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executeSemiJoin(plan, context) {
  const left = executePlan({ plan: plan.left, context })
  return {
    columns: left.columns,
    maxRows: left.maxRows,
    async *rows() {
      /** @type {Map<string, Set<SqlPrimitive>>} */
      const values = new Map()
      let hasNull = false
      let count = 0
      let built = false
      let rowIndex = 0
      for await (const row of left.rows()) {
        context.signal?.throwIfAborted()
        if (++rowIndex % 4000 === 0) await yieldToEventLoop()
        const value = await evaluateExpr({ node: plan.expr, row, rowIndex, context })
        if (!built) {
          for await (const inner of executePlan({ plan: plan.right, context }).rows()) {
            if (++count % 4000 === 0) {
              await yieldToEventLoop()
              context.signal?.throwIfAborted()
            }
            const value = await inner.cells[inner.columns[0]]()
            if (value == null) hasNull = true
            else {
              const type = typeof value
              let group = values.get(type)
              if (!group) values.set(type, group = new Set())
              group.add(value)
            }
          }
          built = true
        }
        const matched = count === 0 ? false : value == null ? null : contains(values, value) ? true : hasNull ? null : false
        if (matched !== null && matched !== plan.not) yield row
      }
    },
  }
}

/**
 * Same-type primitives use a Set. Cross-type values and Dates retain the
 * evaluator's SQL equality, including coercion and timestamp comparison.
 *
 * @param {Map<string, Set<SqlPrimitive>>} values
 * @param {SqlPrimitive} value
 * @returns {boolean}
 */
function contains(values, value) {
  const type = typeof value
  if (type !== 'object' && !(typeof value === 'number' && Number.isNaN(value)) && values.get(type)?.has(value)) return true
  for (const [valueType, group] of values) {
    if (valueType === type && type !== 'object') continue
    for (const candidate of group) {
      if (sqlEquals(value, candidate)) return true
    }
  }
  return false
}
