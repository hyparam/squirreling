import { collectColumnsFromExpr } from '../plan/columns.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { ExecuteContext, ExistsNode, IdentifierNode, InSubqueryNode, SubqueryNode } from '../types.js'
 * @import { QueryExecution, SubqueryExecution } from '../internalTypes.js'
 */

const MAX_CORRELATED_SCANS = 4
const CHECK_STRIDE = 64
const YIELD_AFTER_MS = 8

/**
 * @returns {QueryExecution}
 */
export function createQueryExecution() {
  return { subqueries: new WeakMap(), checks: 0, yieldedAt: performance.now() }
}

/**
 * Share a work clock across nested short scans. Per-loop row counters cannot
 * yield when thousands of individually short scans exhaust a microtask turn.
 * Most checks are synchronous; concurrent callers share one macrotask yield.
 *
 * @param {ExecuteContext} context
 * @returns {Promise<void> | undefined}
 */
export function checkExecution(context) {
  context.signal?.throwIfAborted()
  const state = context.execution
  if (!state) return
  if (state.yielding) return state.yielding
  if (++state.checks < CHECK_STRIDE) return
  state.checks = 0
  if (performance.now() - state.yieldedAt < YIELD_AFTER_MS) return
  state.yielding = yieldToEventLoop().then(() => {
    state.yieldedAt = performance.now()
    state.yielding = undefined
    context.signal?.throwIfAborted()
  })
  return state.yielding
}

/**
 * Cache an independent subquery's in-flight result, not just its settled
 * value. Correlated evaluations keep their outer rows and share four slots
 * per expression. Separate expressions have separate slots so a nested read
 * cannot deadlock behind the outer reads waiting for it.
 *
 * @template T
 * @param {InSubqueryNode | SubqueryNode | ExistsNode} node
 * @param {ExecuteContext} context
 * @param {(shared: boolean) => Promise<T>} read
 * @returns {Promise<T>}
 */
export async function runSubquery(node, context, read) {
  const pause = checkExecution(context)
  if (pause) await pause
  const state = context.execution
  if (!state) return read(false)
  let entry = state.subqueries.get(node)
  if (!entry) {
    /** @type {IdentifierNode[]} */
    const correlated = []
    // The IN operand belongs to the outer row even when its right side is
    // independent. Inspect only the subquery body for escaped references.
    collectColumnsFromExpr({ ...node, type: 'subquery' }, correlated, undefined, {
      tables: context.tables,
      cteColumns: context.cteColumns,
      outerAliases: new Set([...context.scope ?? [], ...context.outerAliases ?? []]),
    })
    entry = { correlated: correlated.length > 0, active: 0, waiters: [] }
    state.subqueries.set(node, entry)
  }
  if (!entry.correlated) {
    entry.result ??= Promise.resolve().then(() => {
      context.signal?.throwIfAborted()
      return read(true)
    })
    const { result } = entry
    // eslint-disable-next-line no-extra-parens
    return /** @type {Promise<T>} */ (result)
  }
  return runCorrelated(entry, context, read)
}

/**
 * Hand a released slot directly to its oldest waiter, including on abort.
 * The waiters are bounded by the expression materialization chunk, rather
 * than growing with every row collected by the query.
 *
 * @template T
 * @param {SubqueryExecution} entry
 * @param {ExecuteContext} context
 * @param {(shared: boolean) => Promise<T>} read
 * @returns {Promise<T>}
 */
async function runCorrelated(entry, context, read) {
  if (entry.active < MAX_CORRELATED_SCANS) entry.active++
  else await new Promise(resolve => { entry.waiters.push(() => resolve(undefined)) })
  try {
    context.signal?.throwIfAborted()
    const value = await read(false)
    context.signal?.throwIfAborted()
    return value
  } finally {
    const next = entry.waiters.shift()
    if (next) next()
    else entry.active--
  }
}
