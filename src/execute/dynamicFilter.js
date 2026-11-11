import { derivedAlias } from '../expression/alias.js'

/**
 * @import { AsyncDataSource, ExprNode, IdentifierNode, SqlPrimitive } from '../types.js'
 * @import { KeyRange } from '../internalTypes.js'
 * @import { HashAggregateNode, ProjectNode, QueryPlan } from '../plan/types.js'
 */

// Bound filter memory and match the session-index pruning limit.
const MAX_DYNAMIC_FILTER_KEYS = 256

/**
 * Bounded, per-column key sets. Once a column exceeds the cap, release its set
 * and fall back to the [min, max] range of its keys, which still lets a source
 * skip row groups whose statistics fall outside it. Unsupported values disable
 * that column.
 *
 * @param {number} count
 * @param {boolean} [hash] - preserve the hash join's serialized key equivalences
 * @returns {{ add(values: SqlPrimitive[]): void, apply(plan: QueryPlan, keys: ExprNode[], alias?: string, tables?: Record<string, AsyncDataSource>): QueryPlan }}
 */
export function dynamicFilter(count, hash = false) {
  /** @type {(Set<SqlPrimitive> | undefined)[]} */
  const sets = Array.from({ length: count }, () => new Set())
  /** @type {(KeyRange | undefined)[]} */
  const ranges = Array.from({ length: count }, () => /** @type {KeyRange} */ ({ kind: undefined, min: undefined, max: undefined }))
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
        ranges.fill(undefined)
        active = false
        return
      }
      for (let i = 0; i < count; i++) {
        const value = values[i]
        if (value == null || !sets[i] && !ranges[i]) continue
        if (!['string', 'number', 'bigint', 'boolean'].includes(typeof value) ||
            typeof value === 'number' && !Number.isFinite(value)) {
          sets[i] = undefined
          ranges[i] = undefined
          continue
        }
        ranges[i] = widen(ranges[i], value)
        const set = sets[i]
        if (!set) continue
        set.add(value)
        if (hash && count > 1) {
          if (typeof value === 'boolean') set.add(String(value))
          if (value === 'true' || value === 'false') set.add(value === 'true')
        }
        if (set.size > MAX_DYNAMIC_FILTER_KEYS) sets[i] = undefined
      }
      active = sets.some(Boolean) || ranges.some(Boolean)
    },
    /**
     * @param {QueryPlan} plan
     * @param {ExprNode[]} keys
     * @param {string} [alias] - name the keys use for the plan's output
     * @param {Record<string, AsyncDataSource>} [tables] - sources, to check a string range's column type
     * @returns {QueryPlan}
     */
    apply(plan, keys, alias, tables) {
      for (let i = 0; i < keys.length; i++) {
        const set = sets[i]
        const range = ranges[i]
        if (set?.size) plan = pushFilter(plan, keys[i], [...set], alias)
        else if (!set && range?.kind) plan = pushFilter(plan, keys[i], range, alias, tables)
      }
      return plan
    },
  }
}

/**
 * Extend a key range by one value. Keys compare with JavaScript's loose
 * equality (sqlEquals), so a range is sound only within one comparison
 * domain: numbers and bigints order numerically against any probe value that
 * equals them, and strings order lexicographically only against strings.
 * Booleans and mixed domains give up the range.
 *
 * @param {KeyRange | undefined} range
 * @param {SqlPrimitive} value
 * @returns {KeyRange | undefined}
 */
function widen(range, value) {
  if (!range) return
  if (typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'bigint') return
  const kind = typeof value === 'string' ? 'string' : 'number'
  if (range.kind && range.kind !== kind) return
  const { min, max } = range
  return {
    kind,
    min: min === undefined || value < min ? value : min,
    max: max === undefined || value > max ? value : max,
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
 * @param {SqlPrimitive[] | KeyRange} values - key list, or a range once the list overflowed
 * @param {string} [alias] - name the key uses for a derived table's output
 * @param {Record<string, AsyncDataSource>} [tables]
 * @returns {QueryPlan}
 */
function pushFilter(plan, key, values, alias, tables) {
  if (key?.type !== 'identifier') return plan
  if (plan.type === 'Scan') {
    if (key.prefix && key.prefix !== (plan.alias ?? plan.table) ||
        plan.hints.columns && !plan.hints.columns.includes(key.name) ||
        plan.hints.limit !== undefined || plan.hints.offset || plan.topK) return plan
    const position = { positionStart: key.positionStart, positionEnd: key.positionEnd }
    /** @type {ExprNode | undefined} */
    const filter = Array.isArray(values)
      ? { type: 'in valuelist', expr: key, ...position, values: values.map(value => ({ type: 'literal', value, ...position })) }
      : rangeFilter(key, values, tables?.[plan.table], position)
    if (!filter) return plan
    /** @type {ExprNode} */
    const where = plan.hints.where
      ? { type: 'binary', op: 'AND', left: plan.hints.where, right: filter, ...position }
      : filter
    return { ...plan, hints: { ...plan.hints, where } }
  }
  if (plan.type === 'Filter' || plan.type === 'Sort' && !plan.topK || plan.type === 'Distinct') {
    return { ...plan, child: pushFilter(plan.child, key, values, alias, tables) }
  }
  if (plan.type === 'Subquery' || plan.type === 'Project' || plan.type === 'HashAggregate') {
    if (key.prefix && key.prefix !== alias) return plan
    const source = plan.type === 'Subquery' ? { ...key, prefix: undefined } : sourceColumn(plan, key)
    return source ? { ...plan, child: pushFilter(plan.child, source, values, undefined, tables) } : plan
  }
  if (plan.type === 'HashJoin' && key.prefix) {
    // Restrict only preserved inputs of outer joins; restricting an optional
    // input could turn a formerly matched row into a new null-extended row.
    const left = plan.joinType === 'INNER' || plan.joinType === 'LEFT'
      ? pushFilter(plan.left, key, values, plan.leftAlias, tables) : plan.left
    const right = plan.joinType === 'INNER' || plan.joinType === 'RIGHT'
      ? pushFilter(plan.right, key, values, plan.rightAlias, tables) : plan.right
    return { ...plan, left, right }
  }
  return plan
}

/**
 * `key >= min AND key <= max`. A string range needs the scanned column to be
 * declared a string: a number or date column would compare against string
 * bounds numerically or as a timestamp, not in the order the bounds were taken.
 *
 * @param {IdentifierNode} key
 * @param {KeyRange} range
 * @param {AsyncDataSource | undefined} source
 * @param {{ positionStart: number, positionEnd: number }} position
 * @returns {ExprNode | undefined}
 */
function rangeFilter(key, { kind, min, max }, source, position) {
  if (kind === 'string') {
    const field = source?.schema?.fields.find(field => field.name === key.name)
    if (field?.dataType.type !== 'string') return
  }
  return {
    type: 'binary', op: 'AND', ...position,
    left: { type: 'binary', op: '>=', left: key, right: { type: 'literal', value: min ?? null, ...position }, ...position },
    right: { type: 'binary', op: '<=', left: key, right: { type: 'literal', value: max ?? null, ...position }, ...position },
  }
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
