import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { dynamicFilter } from './dynamicFilter.js'
import { sqlEquals } from './utils.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { AsyncRow, ExecuteContext, ExprNode, QueryResults, SqlPrimitive } from '../types.js'
 * @import { QueryPlan, SemiJoinNode } from '../plan/types.js'
 */

/**
 * @typedef {{ values: Map<string, Set<SqlPrimitive>>, hasNull: boolean, count: number, firstRow?: AsyncRow }} Membership
 * @typedef {{ groups: Map<string, Map<SqlPrimitive, MembershipTrie>>, membership?: Membership, empty?: Promise<SqlPrimitive>, unsafeGrouping?: boolean }} MembershipTrie
 */

/**
 * Executes IN / EXISTS membership with one build of the right input per
 * iteration. Filters left rows, or with `mark`, annotates every left row with
 * its result. A scalar test is a single join: the mark holds the subquery's
 * only value, or for a grouped aggregate, the value of the row's group. The
 * trie belongs to this operator, never to the context or AST.
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
      // Mark joins only need membership when a consumer reads a mark cell.
      // Share the pending build across rows, including concurrent cell reads.
      /** @type {Promise<MembershipTrie> | undefined} */
      let build
      const root = mark ? undefined : await buildTrie(plan, context, pruneKeys.length ? filters : undefined)
      if (root && neverPasses(plan, root)) return
      const probe = executePlan({
        plan: pruneKeys.length ? filters.apply(plan.left, pruneKeys, undefined, context.tables) : plan.left,
        context: { ...context, deferScans: true },
      })
      let rowIndex = 0
      for await (const row of probe.rows()) {
        context.signal?.throwIfAborted()
        const index = ++rowIndex
        if (rowIndex % 4000 === 0) await yieldToEventLoop()
        if (mark) {
          /** @type {Promise<SqlPrimitive> | undefined} */
          let result
          yield {
            columns: [...row.columns, mark],
            cells: { ...row.cells, [mark]: () => result ??= (build ??= buildTrie(plan, context)).then(root => evaluateMembership(plan, root, row, index, context)) },
          }
        } else if (root && await evaluateMembership(plan, root, row, index, context)) {
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
  const offset = plan.test === 'exists' ? 0 : 1
  let count = 0
  for await (const inner of executePlan({ plan: plan.right, context }).rows()) {
    context.signal?.throwIfAborted()
    if (++count % 4000 === 0) await yieldToEventLoop()
    const keys = plan.keys ? await Promise.all(inner.columns.slice(offset).map(col => inner.cells[col]())) : []
    // Serialized object groups may already have merged unequal keys. Even a
    // primitive probe can equal one of those original objects by coercion.
    if (plan.fallback && keys.some(value => value !== null && typeof value === 'object')) root.unsafeGrouping = true
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
    if (plan.test === 'scalar') {
      if (!node.membership.count++) node.membership.firstRow = inner
      // Two rows already make an uncorrelated scalar an error.
      if (!plan.keys && node.membership.count > 1) break
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
 * boolean for EXISTS, and the single value (or NULL) for a scalar.
 *
 * @param {SemiJoinNode} plan
 * @param {MembershipTrie} root
 * @param {AsyncRow} row
 * @param {number} rowIndex
 * @param {ExecuteContext} context
 * @returns {Promise<SqlPrimitive>}
 */
async function evaluateMembership(plan, root, row, rowIndex, context) {
  const keys = await Promise.all((plan.keys ?? []).map(node => evaluateExpr({ node, row, rowIndex, context })))
  // Hash aggregation serializes objects, whereas correlation compares their
  // identity. Its grouped rows cannot answer an object-valued probe safely.
  if (plan.fallback && (root.unsafeGrouping || keys.some(value => value !== null && typeof value === 'object'))) {
    return evaluateExpr({ node: plan.fallback, row, rowIndex, context })
  }
  const leaves = keys.some(value => value == null) ? [] : findLeaves(root, keys, 0)
  if (plan.test === 'exists') return leaves.length > 0 !== plan.not
  if (plan.test === 'scalar') {
    // Keys that equal several groups by coercion cannot merge their values.
    if (plan.fallback && leaves.length > 1) return evaluateExpr({ node: plan.fallback, row, rowIndex, context })
    if (plan.empty && !leaves.length) return root.empty ??= scalarValue(plan.empty, context)
    let count = 0
    /** @type {AsyncRow | undefined} */
    let firstRow
    for (const leaf of leaves) {
      count += leaf.membership?.count ?? 0
      firstRow = leaf.membership?.firstRow ?? firstRow
    }
    if (count > 1) throw new Error('Scalar subquery returned more than one row')
    return firstRow ? firstRow.cells[firstRow.columns[0]]() : null
  }
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
 * Reads the single value of a plan that yields exactly one row.
 *
 * @param {QueryPlan} plan
 * @param {ExecuteContext} context
 * @returns {Promise<SqlPrimitive>}
 */
async function scalarValue(plan, context) {
  for await (const row of executePlan({ plan, context }).rows()) return row.cells[row.columns[0]]()
  return null
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
