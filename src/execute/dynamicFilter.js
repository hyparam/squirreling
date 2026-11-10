import { derivedAlias } from '../expression/alias.js'

/**
 * @import { ExprNode, IdentifierNode, SqlPrimitive } from '../types.js'
 * @import { HashAggregateNode, ProjectNode, QueryPlan } from '../plan/types.js'
 */

// Bound filter memory and match the session-index pruning limit.
const MAX_DYNAMIC_FILTER_KEYS = 256

/**
 * Bounded, per-column key sets. Once a column exceeds the cap, release it and
 * stop collecting that column. Unsupported values disable that column too.
 *
 * @param {number} count
 * @param {boolean} [hash] - preserve the hash join's serialized key equivalences
 * @returns {{ add(values: SqlPrimitive[]): void, apply(plan: QueryPlan, keys: ExprNode[], alias?: string): QueryPlan }}
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
     * @param {string} [alias] - name the keys use for the plan's output
     * @returns {QueryPlan}
     */
    apply(plan, keys, alias) {
      for (let i = 0; i < keys.length; i++) {
        const set = sets[i]
        if (set?.size) plan = pushFilter(plan, keys[i], [...set], alias)
      }
      return plan
    },
  }
}

/**
 * Only traverse operators that preserve the meaning of a physical column.
 * A derived table is crossed when the key names a column it passes through
 * unchanged, or a column it groups by. Limits, windows and computed columns
 * are deliberate barriers.
 *
 * @param {QueryPlan} plan
 * @param {ExprNode} key
 * @param {SqlPrimitive[]} values
 * @param {string} [alias] - name the key uses for a derived table's output
 * @returns {QueryPlan}
 */
function pushFilter(plan, key, values, alias) {
  if (key?.type !== 'identifier') return plan
  if (plan.type === 'Scan') {
    if (key.prefix && key.prefix !== (plan.alias ?? plan.table) ||
        plan.hints.columns && !plan.hints.columns.includes(key.name) ||
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
    return { ...plan, child: pushFilter(plan.child, key, values, alias) }
  }
  if (plan.type === 'Subquery' || plan.type === 'Project' || plan.type === 'HashAggregate') {
    if (key.prefix && key.prefix !== alias) return plan
    const source = plan.type === 'Subquery' ? { ...key, prefix: undefined } : sourceColumn(plan, key)
    return source ? { ...plan, child: pushFilter(plan.child, source, values) } : plan
  }
  if (plan.type === 'HashJoin' && key.prefix) {
    // Restrict only preserved inputs of outer joins; restricting an optional
    // input could turn a formerly matched row into a new null-extended row.
    const left = plan.joinType === 'INNER' || plan.joinType === 'LEFT'
      ? pushFilter(plan.left, key, values, plan.leftAlias) : plan.left
    const right = plan.joinType === 'INNER' || plan.joinType === 'RIGHT'
      ? pushFilter(plan.right, key, values, plan.rightAlias) : plan.right
    return { ...plan, left, right }
  }
  return plan
}

/**
 * The child column behind an output column, when filtering the child on it
 * keeps or drops exactly the output rows holding the same value: a plain
 * column reference, which under an aggregate must also be a grouping key.
 *
 * @param {ProjectNode | HashAggregateNode} plan
 * @param {IdentifierNode} key
 * @returns {IdentifierNode | undefined}
 */
function sourceColumn(plan, key) {
  const position = { positionStart: key.positionStart, positionEnd: key.positionEnd }
  const stars = plan.columns.filter(col => col.type === 'star')
  const named = plan.columns.filter(col => col.type === 'derived' && (col.alias ?? derivedAlias(col.expr)) === key.name)
  // Without a named match the column can only come from a lone star
  if (!named.length && stars.length === 1 && plan.type === 'Project') {
    return { type: 'identifier', name: key.name, prefix: stars[0].table, ...position }
  }
  const [col] = named
  if (stars.length || named.length !== 1 || col.type !== 'derived' || col.expr.type !== 'identifier') return
  const { name, prefix } = col.expr
  if (plan.type === 'HashAggregate' && !plan.groupBy.some(expr => expr.type === 'identifier' &&
      expr.name === name && (expr.prefix === prefix || !expr.prefix || !prefix))) return
  return { type: 'identifier', name, prefix, ...position }
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
  // A derived table is as selective as the filters beneath it
  if (plan.type === 'Subquery' || plan.type === 'Project' || plan.type === 'Distinct' ||
      plan.type === 'Sort' || plan.type === 'HashAggregate') return filterScore(plan.child)
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
