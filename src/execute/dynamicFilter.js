/**
 * @import { ExprNode, SqlPrimitive } from '../types.js'
 * @import { QueryPlan } from '../plan/types.js'
 */

// Bound filter memory and match the session-index pruning limit.
const MAX_DYNAMIC_FILTER_KEYS = 256

/**
 * Bounded, per-column key sets. Once a column exceeds the cap, release it and
 * stop collecting that column. Unsupported values disable that column too.
 *
 * @param {number} count
 * @param {boolean} [hash] - preserve the hash join's serialized key equivalences
 * @returns {{ add(values: SqlPrimitive[]): void, apply(plan: QueryPlan, keys: ExprNode[]): QueryPlan }}
 */
export function dynamicFilter(count, hash = false) {
  /** @type {(Set<SqlPrimitive> | undefined)[]} */
  const sets = Array.from({ length: count }, () => new Set())
  let active = true
  return {
    /**
     * @param {SqlPrimitive[]} values
     * @returns {void}
     */
    add(values) {
      if (!active) return
      // Hash tuple serialization uses delimiters and serializes objects. Do
      // not prune when those encodings can collide across column boundaries.
      if (hash && values.some(value => typeof value === 'object' && value !== null ||
          typeof value === 'string' && (value.includes('|') || ['[', '{', '"'].includes(value[0]) || value === 'null'))) {
        sets.fill(undefined)
        active = false
        return
      }
      for (let i = 0; i < sets.length; i++) {
        const set = sets[i]
        const value = values[i]
        if (!set || value == null) continue
        if (!['string', 'number', 'bigint', 'boolean'].includes(typeof value) ||
            typeof value === 'number' && !Number.isFinite(value)) {
          sets[i] = undefined
          continue
        }
        set.add(value)
        if (hash && count > 1) {
          if (typeof value === 'boolean') set.add(String(value))
          if (value === 'true' || value === 'false') set.add(value === 'true')
        }
        if (set.size > MAX_DYNAMIC_FILTER_KEYS) sets[i] = undefined
      }
      active = sets.some(Boolean)
    },
    /**
     * @param {QueryPlan} plan
     * @param {ExprNode[]} keys
     * @returns {QueryPlan}
     */
    apply(plan, keys) {
      for (let i = 0; i < keys.length; i++) {
        const set = sets[i]
        if (set?.size) plan = pushFilter(plan, keys[i], [...set])
      }
      return plan
    },
  }
}

/**
 * Only traverse operators that preserve the meaning of a physical column.
 * Limits, aggregates, windows and projections are deliberate barriers.
 *
 * @param {QueryPlan} plan
 * @param {ExprNode} key
 * @param {SqlPrimitive[]} values
 * @returns {QueryPlan}
 */
function pushFilter(plan, key, values) {
  if (key?.type !== 'identifier') return plan
  if (plan.type === 'Scan') {
    if (key.prefix && key.prefix !== (plan.alias ?? plan.table) ||
        plan.hints.limit !== undefined || plan.hints.offset || plan.topK) return plan
    const position = { positionStart: key.positionStart, positionEnd: key.positionEnd }
    /** @type {ExprNode} */
    const filter = {
      type: 'in valuelist', expr: key, ...position,
      values: values.map(value => ({ type: 'literal', value, ...position })),
    }
    /** @type {ExprNode} */
    const where = plan.hints.where
      ? { type: 'binary', op: 'AND', left: plan.hints.where, right: filter, ...position }
      : filter
    return { ...plan, hints: { ...plan.hints, where } }
  }
  if (plan.type === 'Filter' || plan.type === 'Sort' && !plan.topK || plan.type === 'Distinct') {
    return { ...plan, child: pushFilter(plan.child, key, values) }
  }
  if (plan.type === 'HashJoin' && key.prefix) {
    // Restrict only preserved inputs of outer joins; restricting an optional
    // input could turn a formerly matched row into a new null-extended row.
    const left = plan.joinType === 'INNER' || plan.joinType === 'LEFT'
      ? pushFilter(plan.left, key, values) : plan.left
    const right = plan.joinType === 'INNER' || plan.joinType === 'RIGHT'
      ? pushFilter(plan.right, key, values) : plan.right
    return { ...plan, left, right }
  }
  return plan
}

/**
 * Selectivity heuristic when exact filtered cardinalities are unavailable.
 * Count conjuncts so a date plus a LIKE wins over a lone part-type predicate.
 *
 * @param {QueryPlan} plan
 * @returns {number}
 */
export function filterScore(plan) {
  if (plan.type === 'Scan') return conjuncts(plan.hints.where)
  if (plan.type === 'Filter') return conjuncts(plan.condition) + filterScore(plan.child)
  if (plan.type === 'HashJoin' && plan.joinType === 'INNER') return Math.max(filterScore(plan.left), filterScore(plan.right))
  return 0
}

/**
 * @param {ExprNode} expr
 * @returns {number}
 */
function conjuncts(expr) {
  return !expr ? 0 : expr.type === 'binary' && expr.op === 'AND'
    ? conjuncts(expr.left) + conjuncts(expr.right) : 1
}
