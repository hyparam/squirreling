import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { dynamicFilter } from './dynamicFilter.js'
import { sqlEquals } from './utils.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { KeyTrie } from '../internalTypes.js'
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
  if (plan.keys) return executeExistsJoin(plan, context)
  const left = executePlan({ plan: plan.left, context: { ...context, deferScans: true } })
  return {
    columns: left.columns,
    maxRows: left.maxRows,
    async *rows() {
      /** @type {Map<string, Set<SqlPrimitive>>} */
      const values = new Map()
      let hasNull = false
      let count = 0
      let rowIndex = 0
      const filters = dynamicFilter(1)
      for await (const inner of executePlan({ plan: plan.right, context }).rows()) {
        context.signal?.throwIfAborted()
        if (++count % 4000 === 0) await yieldToEventLoop()
        const value = await inner.cells[inner.columns[0]]()
        if (value == null) hasNull = true
        else {
          const type = typeof value
          let group = values.get(type)
          if (!group) values.set(type, group = new Set())
          group.add(value)
          if (!plan.not) filters.add([value])
        }
      }
      if (!plan.not && !values.size || plan.not && hasNull) return
      const probe = executePlan({
        plan: plan.not ? plan.left : filters.apply(plan.left, [plan.expr]),
        context: { ...context, deferScans: true },
      })
      for await (const row of probe.rows()) {
        context.signal?.throwIfAborted()
        if (++rowIndex % 4000 === 0) await yieldToEventLoop()
        const value = await evaluateExpr({ node: plan.expr, row, rowIndex, context })
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

/**
 * Builds a tuple membership trie once. Type partitions preserve the evaluator's
 * coercive equality without making same-type primitive lookups linear.
 *
 * @param {SemiJoinNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeExistsJoin(plan, context) {
  const left = executePlan({ plan: plan.left, context: { ...context, deferScans: true } })
  return {
    columns: left.columns,
    maxRows: left.maxRows,
    async *rows() {
      /** @type {KeyTrie} */
      const root = { groups: new Map() }
      let count = 0
      const filters = dynamicFilter(plan.keys?.length ?? 0)
      for await (const inner of executePlan({ plan: plan.right, context }).rows()) {
        context.signal?.throwIfAborted()
        if (++count % 4000 === 0) await yieldToEventLoop()
        const values = await Promise.all(inner.columns.map(col => inner.cells[col]()))
        if (values.some(value => value == null)) continue
        if (!plan.not) filters.add(values)
        let node = root
        for (const value of values) {
          const type = typeof value
          let group = node.groups.get(type)
          if (!group) node.groups.set(type, group = new Map())
          let child = group.get(value)
          if (!child) group.set(value, child = { groups: new Map() })
          node = child
        }
      }
      if (!plan.not && !root.groups.size) return
      const probe = executePlan({
        plan: plan.not ? plan.left : filters.apply(plan.left, plan.keys ?? []),
        context: { ...context, deferScans: true },
      })
      for await (const row of probe.rows()) {
        context.signal?.throwIfAborted()
        if (++count % 4000 === 0) await yieldToEventLoop()
        const values = await Promise.all((plan.keys ?? []).map(node => evaluateExpr({ node, row, context })))
        const matched = !values.some(value => value == null) && matchesKeys(root, values, 0)
        if (matched !== plan.not) yield row
      }
    },
  }
}

/**
 * @param {KeyTrie} node
 * @param {SqlPrimitive[]} values
 * @param {number} index
 * @returns {boolean}
 */
function matchesKeys(node, values, index) {
  if (index === values.length) return true
  const value = values[index]
  const type = typeof value
  if (type !== 'object' && !Number.isNaN(value)) {
    const child = node.groups.get(type)?.get(value)
    if (child && matchesKeys(child, values, index + 1)) return true
  }
  for (const [valueType, group] of node.groups) {
    if (valueType === type && type !== 'object') continue
    for (const [candidate, child] of group) {
      if (sqlEquals(value, candidate) && matchesKeys(child, values, index + 1)) return true
    }
  }
  return false
}
