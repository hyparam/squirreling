import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { dynamicFilter } from './dynamicFilter.js'
import { sqlEquals } from './utils.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { AsyncRow, ExecuteContext, ExprNode, QueryResults, SqlPrimitive } from '../types.js'
 * @import { SemiJoinNode } from '../plan/types.js'
 */

/**
 * @typedef {{ values: Map<string, Set<SqlPrimitive>>, hasNull: boolean, count: number }} Membership
 * @typedef {{ groups: Map<string, Map<SqlPrimitive, MembershipTrie>>, membership?: Membership }} MembershipTrie
 */

/**
 * Executes IN / EXISTS membership with one build of the right input per
 * iteration. Filters left rows, or with `mark`, annotates every left row with
 * its result. The trie belongs to this operator, never to the context or AST.
 *
 * @param {SemiJoinNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executeSemiJoin(plan, context) {
  const left = executePlan({ plan: plan.left, context: { ...context, deferScans: true } })
  const { mark } = plan
  // Only a positive filter can prune the left input by right-side values.
  const pruneKeys = !mark && !plan.not ? filterKeys(plan) : []
  return {
    columns: mark ? [...left.columns, mark] : left.columns,
    numRows: mark ? left.numRows : undefined,
    maxRows: left.maxRows,
    async *rows() {
      const filters = dynamicFilter(pruneKeys.length)
      const root = await buildTrie(plan, context, pruneKeys.length ? filters : undefined)
      if (!mark && neverPasses(plan, root)) return
      const probe = executePlan({
        plan: pruneKeys.length ? filters.apply(plan.left, pruneKeys) : plan.left,
        context: { ...context, deferScans: true },
      })
      let rowIndex = 0
      for await (const row of probe.rows()) {
        context.signal?.throwIfAborted()
        const index = rowIndex++
        if (rowIndex % 4000 === 0) await yieldToEventLoop()
        if (mark) {
          /** @type {Promise<SqlPrimitive> | undefined} */
          let result
          yield {
            columns: [...row.columns, mark],
            cells: { ...row.cells, [mark]: () => result ??= evaluateMembership(plan, root, row, index, context) },
          }
        } else if (await evaluateMembership(plan, root, row, index, context)) {
          yield row
        }
      }
    },
  }
}

/**
 * Left expressions whose values must appear on the right for a row to pass.
 *
 * @param {SemiJoinNode} plan
 * @returns {ExprNode[]}
 */
function filterKeys(plan) {
  const keys = plan.keys ?? []
  return plan.test === 'in' ? [plan.expr, ...keys] : keys
}

/**
 * Reads the right input once. Correlation keys index the trie; each leaf
 * holds the IN values for its key. Rows with a NULL key can never satisfy the
 * equality, so they are skipped. Uncorrelated EXISTS stops at the first row.
 *
 * @param {SemiJoinNode} plan
 * @param {ExecuteContext} context
 * @param {ReturnType<typeof dynamicFilter>} [filters]
 * @returns {Promise<MembershipTrie>}
 */
async function buildTrie(plan, context, filters) {
  /** @type {MembershipTrie} */
  const root = { groups: new Map() }
  // An uncorrelated IN over an empty set is false, not absent.
  if (plan.test === 'in' && !plan.keys) root.membership = emptyMembership()
  const offset = plan.test === 'in' ? 1 : 0
  let count = 0
  for await (const inner of executePlan({ plan: plan.right, context }).rows()) {
    context.signal?.throwIfAborted()
    if (++count % 4000 === 0) await yieldToEventLoop()
    const keys = plan.keys ? await Promise.all(inner.columns.slice(offset).map(col => inner.cells[col]())) : []
    if (keys.some(value => value == null)) continue
    let node = root
    for (const key of keys) {
      const type = typeof key
      let group = node.groups.get(type)
      if (!group) node.groups.set(type, group = new Map())
      let child = group.get(key)
      if (!child) group.set(key, child = { groups: new Map() })
      node = child
    }
    node.membership ??= emptyMembership()
    if (plan.test === 'exists') {
      filters?.add(keys)
      if (!plan.keys) break
      continue
    }
    const value = await inner.cells[inner.columns[0]]()
    addValue(node.membership, value)
    if (value != null) filters?.add([value, ...keys])
  }
  return root
}

/**
 * True when no left row can pass the filter, so the left input is never read.
 *
 * @param {SemiJoinNode} plan
 * @param {MembershipTrie} root
 * @returns {boolean}
 */
function neverPasses(plan, root) {
  if (plan.keys) return !plan.not && !root.groups.size
  const { membership } = root
  if (plan.test === 'exists') return plan.not === !!membership
  // IN needs a non-NULL value; NOT IN fails for every row once a NULL is seen.
  return plan.not ? !!membership?.hasNull : !membership?.values.size
}

/**
 * SQL result of the membership test for one left row: three-valued for IN,
 * boolean for EXISTS.
 *
 * @param {SemiJoinNode} plan
 * @param {MembershipTrie} root
 * @param {AsyncRow} row
 * @param {number} rowIndex
 * @param {ExecuteContext} context
 * @returns {Promise<boolean | null>}
 */
async function evaluateMembership(plan, root, row, rowIndex, context) {
  const keys = await Promise.all((plan.keys ?? []).map(node => evaluateExpr({ node, row, rowIndex, context })))
  const leaves = keys.some(value => value == null) ? [] : findLeaves(root, keys, 0)
  if (plan.test === 'exists') return leaves.length > 0 !== plan.not
  const value = await evaluateExpr({ node: plan.expr, row, rowIndex, context })
  /** @type {boolean | null} */
  let matched = false
  for (const leaf of leaves) {
    const found = leaf.membership && matches(leaf.membership, value)
    if (found) {
      matched = true
      break
    }
    if (found === null) matched = null
  }
  return plan.not && matched !== null ? !matched : matched
}

/**
 * Collects every leaf whose key path SQL-equals the given values.
 * Same-type primitives use a direct lookup; cross-type values and Dates fall
 * back to the evaluator's coercive equality.
 *
 * @param {MembershipTrie} node
 * @param {SqlPrimitive[]} values
 * @param {number} index
 * @returns {MembershipTrie[]}
 */
function findLeaves(node, values, index) {
  if (index === values.length) return node.membership ? [node] : []
  const value = values[index]
  const type = typeof value
  /** @type {MembershipTrie[]} */
  const leaves = []
  if (type !== 'object' && !Number.isNaN(value)) {
    const child = node.groups.get(type)?.get(value)
    if (child) leaves.push(...findLeaves(child, values, index + 1))
  }
  for (const [valueType, group] of node.groups) {
    if (valueType === type && type !== 'object') continue
    for (const [candidate, child] of group) {
      if (sqlEquals(value, candidate)) leaves.push(...findLeaves(child, values, index + 1))
    }
  }
  return leaves
}

/**
 * @returns {Membership}
 */
function emptyMembership() {
  return { values: new Map(), hasNull: false, count: 0 }
}

/**
 * @param {Membership} membership
 * @param {SqlPrimitive} value
 */
function addValue(membership, value) {
  membership.count++
  if (value == null) {
    membership.hasNull = true
    return
  }
  const type = typeof value
  let group = membership.values.get(type)
  if (!group) membership.values.set(type, group = new Set())
  group.add(value)
}

/**
 * SQL three-valued IN result for one probe value.
 *
 * @param {Membership} membership
 * @param {SqlPrimitive} value
 * @returns {boolean | null}
 */
function matches({ values, hasNull, count }, value) {
  if (count === 0) return false
  if (value == null) return null
  if (contains(values, value)) return true
  return hasNull ? null : false
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
