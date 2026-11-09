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
  if (plan.mark) return executeMarkJoin(plan, plan.mark, context)
  if (plan.keys) return executeExistsJoin(plan, context)
  const left = executePlan({ plan: plan.left, context: { ...context, deferScans: true } })
  return {
    columns: left.columns,
    maxRows: left.maxRows,
    async *rows() {
      const filters = dynamicFilter(1)
      const membership = await buildMembership(plan, context, plan.not ? undefined : filters)
      if (!plan.not && !membership.values.size || plan.not && membership.hasNull) return
      const probe = executePlan({
        plan: plan.not ? plan.left : filters.apply(plan.left, [plan.expr]),
        context: { ...context, deferScans: true },
      })
      let rowIndex = 0
      for await (const row of probe.rows()) {
        context.signal?.throwIfAborted()
        if (++rowIndex % 4000 === 0) await yieldToEventLoop()
        const value = await evaluateExpr({ node: plan.expr, row, rowIndex, context })
        const matched = matches(membership, value)
        if (matched !== null && matched !== plan.not) yield row
      }
    },
  }
}

/**
 * Executes IN or EXISTS outside WHERE: every left row passes through with an
 * added cell holding its membership result. The right input is read once per
 * iteration, on first pull, into a trie owned by this operator. Correlation
 * keys index the trie; each leaf holds the IN values for that key.
 *
 * @param {SemiJoinNode} plan
 * @param {{ name: string, test: 'in' | 'exists' }} mark
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeMarkJoin(plan, mark, context) {
  const left = executePlan({ plan: plan.left, context })
  const keys = plan.keys ?? []
  return {
    columns: [...left.columns, mark.name],
    numRows: left.numRows,
    maxRows: left.maxRows,
    async *rows() {
      const root = await buildMarkTrie(plan, mark, context)
      let rowIndex = 0
      for await (const row of left.rows()) {
        context.signal?.throwIfAborted()
        const index = rowIndex++
        if (rowIndex % 4000 === 0) await yieldToEventLoop()
        /** @type {Promise<SqlPrimitive> | undefined} */
        let result
        /** @returns {Promise<SqlPrimitive>} */
        async function evaluate() {
          const values = await Promise.all(keys.map(node => evaluateExpr({ node, row, rowIndex: index, context })))
          const leaves = values.some(value => value == null) ? [] : findLeaves(root, values, 0)
          if (mark.test === 'exists') return leaves.length > 0 !== plan.not
          const value = await evaluateExpr({ node: plan.expr, row, rowIndex: index, context })
          /** @type {boolean | null} */
          let matched = false
          for (const leaf of leaves) {
            const found = leaf.membership && matches(leaf.membership, value)
            if (found) return true
            if (found === null) matched = null
          }
          return matched
        }
        yield {
          columns: [...row.columns, mark.name],
          cells: { ...row.cells, [mark.name]: () => result ??= evaluate() },
        }
      }
    },
  }
}

/**
 * @typedef {{ groups: Map<string, Map<SqlPrimitive, MarkTrie>>, membership?: Membership }} MarkTrie
 */

/**
 * Reads the right input once. Rows with a NULL correlation key can never
 * match an equality, so they are skipped. Uncorrelated EXISTS stops at the
 * first row.
 *
 * @param {SemiJoinNode} plan
 * @param {{ test: 'in' | 'exists' }} mark
 * @param {ExecuteContext} context
 * @returns {Promise<MarkTrie>}
 */
async function buildMarkTrie(plan, mark, context) {
  /** @type {MarkTrie} */
  const root = { groups: new Map() }
  // An uncorrelated IN over an empty set is false, not absent.
  if (mark.test === 'in' && !plan.keys) root.membership = { values: new Map(), hasNull: false, count: 0 }
  const offset = mark.test === 'in' ? 1 : 0
  let count = 0
  for await (const inner of executePlan({ plan: plan.right, context }).rows()) {
    context.signal?.throwIfAborted()
    if (++count % 4000 === 0) await yieldToEventLoop()
    const keys = await Promise.all(inner.columns.slice(offset).map(col => inner.cells[col]()))
    if (plan.keys && keys.some(value => value == null)) continue
    let node = root
    for (const key of plan.keys ? keys : []) {
      const type = typeof key
      let group = node.groups.get(type)
      if (!group) node.groups.set(type, group = new Map())
      let child = group.get(key)
      if (!child) group.set(key, child = { groups: new Map() })
      node = child
    }
    node.membership ??= { values: new Map(), hasNull: false, count: 0 }
    if (mark.test === 'exists') {
      if (!plan.keys) break
      continue
    }
    addValue(node.membership, await inner.cells[inner.columns[0]]())
  }
  return root
}

/**
 * Collects every leaf whose key path SQL-equals the given values.
 *
 * @param {MarkTrie} node
 * @param {SqlPrimitive[]} values
 * @param {number} index
 * @returns {MarkTrie[]}
 */
function findLeaves(node, values, index) {
  if (index === values.length) return node.membership ? [node] : []
  const value = values[index]
  const type = typeof value
  /** @type {MarkTrie[]} */
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
 * @typedef {{ values: Map<string, Set<SqlPrimitive>>, hasNull: boolean, count: number }} Membership
 */

/**
 * Reads the right input once into type-partitioned value sets.
 *
 * @param {SemiJoinNode} plan
 * @param {ExecuteContext} context
 * @param {ReturnType<typeof dynamicFilter>} [filters]
 * @returns {Promise<Membership>}
 */
async function buildMembership(plan, context, filters) {
  /** @type {Membership} */
  const membership = { values: new Map(), hasNull: false, count: 0 }
  let count = 0
  for await (const inner of executePlan({ plan: plan.right, context }).rows()) {
    context.signal?.throwIfAborted()
    if (++count % 4000 === 0) await yieldToEventLoop()
    const value = await inner.cells[inner.columns[0]]()
    addValue(membership, value)
    if (value != null) filters?.add([value])
  }
  return membership
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
