import { derivedAlias } from '../expression/alias.js'
import { findAggregate } from '../validation/aggregates.js'

/**
 * @import { DerivedColumn, ExprNode, FunctionNode, IdentifierNode, OrderByItem } from '../types.js'
 * @import { HashAggregateNode, QueryPlan } from './types.js'
 */

/**
 * Eager aggregation: rewrite `GROUP BY f(col)` into a two-stage aggregate
 * that deduplicates on the raw column first and applies `f` only to the
 * distinct survivors.
 *
 *   HashAggregate(groupBy: [f(col)], columns: [key, COUNT(*)], child)
 * becomes
 *   HashAggregate(groupBy: [f(col)], columns: [key, SUM(partial)],
 *     child: HashAggregate(groupBy: [col], columns: [col, COUNT(*) AS partial], child))
 *
 * Why: a string-producing function over a dictionary-encoded column
 * manufactures a fresh full-length copy per input row, so grouping directly
 * on `f(col)` scales its allocations with row count even when the column
 * holds few distinct values. Grouping on the raw column first costs only
 * reference comparisons, and `f` then runs once per distinct value. The
 * chunked key evaluation cannot bound this on its own: it sizes chunks from
 * the evaluated key bytes, and a wrapper like `substr(f(col), 1, 150)` makes
 * the keys look tiny while every row still materializes the full-length
 * intermediate inside the expression.
 *
 * The rewrite fires only when it is provably answer-preserving and aimed at
 * the hazard:
 * - a single GROUP BY key, built from deterministic scalar constructs over
 *   exactly one unqualified column, containing at least one string-producing
 *   function or cast to a string type (the per-row-copy hazard);
 * - every output is either the key expression itself or a splittable
 *   aggregate (COUNT/COUNTIF/SUM/MIN/MAX, no DISTINCT, no FILTER);
 * - no HAVING, and any aggregate-level ORDER BY refers to output aliases
 *   only, so it can move to the merge stage unchanged.
 * Anything else returns the node untouched.
 *
 * @param {HashAggregateNode} node
 * @returns {QueryPlan}
 */
export function rewriteEagerAggregate(node) {
  if (node.groupBy.length !== 1 || node.having) return node
  const key = node.groupBy[0]
  const keyShape = analyzeKey(key)
  if (!keyShape) return node

  /** @type {OrderByItem[] | undefined} */
  let mergedOrderBy
  if (node.orderBy) {
    mergedOrderBy = mergeStageOrderBy(node.orderBy, node.columns)
    if (!mergedOrderBy) return node
  }

  const keySig = exprSig(key)
  const columnName = keyShape.column
  const at = { positionStart: key.positionStart, positionEnd: key.positionEnd }
  /** @type {IdentifierNode} */
  const rawColumn = { type: 'identifier', name: columnName, ...at }

  /** @type {DerivedColumn[]} */
  const innerColumns = [{ type: 'derived', expr: rawColumn, alias: columnName, ...at }]
  /** @type {DerivedColumn[]} */
  const outerColumns = []
  for (const col of node.columns) {
    if (col.type === 'star') return node
    if (exprSig(col.expr) === keySig) {
      outerColumns.push(col)
      continue
    }
    if (col.expr.type !== 'function' || !isSplittableAggregate(col.expr)) return node
    const partial = `__eager_agg_${innerColumns.length - 1}`
    innerColumns.push({ type: 'derived', expr: col.expr, alias: partial, ...at })
    const funcName = col.expr.funcName.toUpperCase()
    // COUNT partials merge by summation; SUM/MIN/MAX merge with themselves.
    const merge = funcName === 'COUNT' || funcName === 'COUNTIF' ? 'SUM' : funcName
    outerColumns.push({
      type: 'derived',
      expr: { type: 'function', funcName: merge, args: [{ type: 'identifier', name: partial, ...at }], ...at },
      alias: col.alias ?? derivedAlias(col.expr),
      ...at,
    })
  }

  /** @type {HashAggregateNode} */
  const inner = {
    type: 'HashAggregate',
    groupBy: [rawColumn],
    columns: innerColumns,
    child: node.child,
  }
  /** @type {HashAggregateNode} */
  const outer = {
    type: 'HashAggregate',
    groupBy: node.groupBy,
    columns: outerColumns,
    child: inner,
  }
  if (mergedOrderBy) outer.orderBy = mergedOrderBy
  return outer
}

/** String-producing scalar functions: each output row is a fresh allocation. */
const STRING_FUNCS = new Set([
  'CONCAT', 'UPPER', 'LOWER', 'SUBSTRING', 'SUBSTR', 'TRIM', 'REPLACE',
  'LEFT', 'RIGHT', 'SPLIT_PART', 'REGEXP_REPLACE', 'REGEXP_SUBSTR', 'REGEXP_EXTRACT',
])

const STRING_CAST_TYPES = new Set(['TEXT', 'STRING', 'VARCHAR'])

/** Aggregates whose per-group partials merge losslessly across groups. */
const SPLITTABLE_AGGREGATES = new Set(['COUNT', 'COUNTIF', 'SUM', 'MIN', 'MAX'])

/**
 * Validates a GROUP BY key for the eager rewrite: deterministic scalar
 * constructs only, exactly one unqualified column, and at least one
 * string-producing function or string cast (the reason to bother). Returns
 * the column name, or undefined when the key does not qualify.
 *
 * @param {ExprNode} key
 * @returns {{ column: string } | undefined}
 */
function analyzeKey(key) {
  if (key.type === 'identifier') return undefined
  /** @type {Set<string>} */
  const columns = new Set()
  let expanding = false

  /**
   * @param {ExprNode} node
   * @returns {boolean}
   */
  function walk(node) {
    switch (node.type) {
    case 'literal':
    case 'interval':
      return true
    case 'identifier':
      if (node.prefix) return false
      columns.add(node.name)
      return true
    case 'function': {
      if (node.distinct || node.filter) return false
      if (!STRING_FUNCS.has(node.funcName.toUpperCase())) return false
      expanding = true
      return node.args.every(walk)
    }
    case 'cast':
      if (STRING_CAST_TYPES.has(node.toType)) expanding = true
      return walk(node.expr)
    case 'binary':
      return walk(node.left) && walk(node.right)
    case 'unary':
      return walk(node.argument)
    default:
      return false
    }
  }

  if (!walk(key) || !expanding || columns.size !== 1) return undefined
  const [column] = columns
  return { column }
}

/**
 * Is this select expression a bare aggregate whose partials merge exactly?
 * The aggregate's arguments are evaluated unchanged by the dedup stage, so
 * they may be any scalar expression; only nested aggregates, windows, and
 * subquery forms are rejected.
 *
 * @param {ExprNode} expr
 * @returns {boolean}
 */
function isSplittableAggregate(expr) {
  if (expr.type !== 'function' || expr.distinct || expr.filter) return false
  if (!SPLITTABLE_AGGREGATES.has(expr.funcName.toUpperCase())) return false
  return expr.args.every(arg => arg.type === 'star' || argIsScalar(arg))
}

/**
 * @param {ExprNode} node
 * @returns {boolean}
 */
function argIsScalar(node) {
  if (node.type === 'window' || node.type === 'subquery' ||
    node.type === 'exists' || node.type === 'not exists' ||
    node.type === 'in' || node.type === 'star') return false
  if (findAggregate(node)) return false
  return true
}

/**
 * Maps an aggregate-level ORDER BY onto the merge stage. A term evaluated
 * there would see merge-stage groups (one row per distinct raw value), so an
 * aggregate expression like `COUNT(*)` must not be re-evaluated: each term is
 * redirected to the output alias of the select column it structurally
 * matches, where the correctly merged value already lives. Alias resolution
 * has already run, so `ORDER BY n` and `ORDER BY COUNT(*)` arrive as the
 * same expression. Returns undefined when a term matches no output column.
 *
 * @param {OrderByItem[]} orderBy
 * @param {HashAggregateNode['columns']} columns
 * @returns {OrderByItem[] | undefined}
 */
function mergeStageOrderBy(orderBy, columns) {
  /** @type {Map<string, string>} */
  const aliasBySig = new Map()
  /** @type {Set<string>} */
  const aliases = new Set()
  for (const col of columns) {
    if (col.type === 'star') continue
    const alias = col.alias ?? derivedAlias(col.expr)
    aliases.add(alias)
    aliasBySig.set(exprSig(col.expr), alias)
  }
  /** @type {OrderByItem[]} */
  const terms = []
  for (const term of orderBy) {
    if (term.expr.type === 'identifier' && !term.expr.prefix && aliases.has(term.expr.name)) {
      terms.push(term)
      continue
    }
    const alias = aliasBySig.get(exprSig(term.expr))
    if (alias === undefined) return undefined
    terms.push({
      ...term,
      expr: { type: 'identifier', name: alias, positionStart: term.expr.positionStart, positionEnd: term.expr.positionEnd },
    })
  }
  return terms
}

/**
 * Structural signature of an expression, ignoring source positions. Twin of
 * the private helper in execute/streamingAggregate.js; both must treat
 * repeated SELECT and GROUP BY expressions as equal.
 *
 * @param {ExprNode} node
 * @returns {string}
 */
function exprSig(node) {
  return JSON.stringify(node, (key, value) => {
    if (key === 'positionStart' || key === 'positionEnd') return undefined
    if (typeof value === 'bigint') return { bigint: value.toString() }
    return value
  })
}
