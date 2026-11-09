import { derivedAlias } from '../expression/alias.js'
import { findAggregate } from '../validation/aggregates.js'
import { isAggregateFunc } from '../validation/functions.js'
import { findWindow } from '../validation/windows.js'
import { collectColumnsFromExpr, statementScope } from './columns.js'
import { andPredicate, collectConjuncts } from './predicates.js'

/**
 * @import { AsyncDataSource, DerivedColumn, ExprNode, IdentifierNode, OrderByItem, SelectColumn, SelectStatement, Statement } from '../types.js'
 * @import { QueryPlan, SemiJoinNode } from './types.js'
 */

/**
 * Replaces eligible scalar aggregates with grouped LEFT JOINs and exact
 * empty-group values. Returns correlated WHERE terms for filtering after joins.
 *
 * @param {object} options
 * @param {SelectStatement} options.select
 * @param {SelectColumn[]} options.columns
 * @param {OrderByItem[]} options.orderBy
 * @param {string[]} options.scopeAliases
 * @param {Set<string>} options.scopeColumns
 * @param {string | undefined} options.sourceAlias
 * @param {boolean} options.rewriteProjection
 * @param {(query: SelectStatement) => QueryPlan} options.planQuery
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @param {Map<string, string[]>} [options.cteColumns]
 * @returns {{ select: SelectStatement, columns: SelectColumn[], orderBy: OrderByItem[], scalarJoins: ((left: QueryPlan) => QueryPlan)[], scalarWhere: ExprNode | undefined }}
 */
export function decorrelateScalars({ select, columns, orderBy, scopeAliases, scopeColumns, sourceAlias, rewriteProjection, planQuery, tables, cteColumns }) {
  columns = [...columns]
  // Keep original expressions for source column demand; synthetic scalar cells
  // are computed above the scan and must never appear in pushdown hints.
  /** @type {((left: QueryPlan) => QueryPlan)[]} */
  const scalarJoins = []
  /**
   * @param {ExprNode} expr
   * @returns {ExprNode}
   */
  function rewriteScalar(expr) {
    if (expr.type !== 'subquery' || select.limit === 0) return expr
    const query = expr.subquery
    if (query.type !== 'select' || query.columns.length !== 1) return expr
    const column = query.columns[0]
    if (column.type !== 'derived' || !findAggregate(column.expr) || findWindow(column.expr)) return expr
    // An uncorrelated bare aggregate is exactly one row: cross join it once.
    if (isSingleRowAggregate(query, expr, scopeAliases, tables, cteColumns)) {
      let name = `__scalar_${scalarJoins.length}`
      while ([...scopeColumns, ...scopeAliases].some(col => col === name || col.startsWith(name + '_'))) name += '_'
      const position = { positionStart: expr.positionStart, positionEnd: expr.positionEnd }
      const valueName = name + '_value'
      const right = planQuery({ ...query, columns: [{ ...column, alias: valueName }], orderBy: [] })
      scalarJoins.push(left => ({
        type: 'NestedLoopJoin', joinType: 'INNER', leftAlias: sourceAlias ?? '', rightAlias: name,
        right: { type: 'Subquery', child: right, scope: statementScope(query) ?? [] },
        condition: { ...position, type: 'literal', value: true }, left,
      }))
      return { ...position, type: 'identifier', prefix: name, name: valueName }
    }
    // Reuse EXISTS' residual-reference check without discarding the aggregate.
    const keys = decorrelateExists(query, scopeAliases, tables, cteColumns, true)
    if (!keys) return expr
    const inner = keys.query.columns.map(col => col.type === 'derived' ? col.expr : column.expr)
    // Hash equality is only equivalent to scalar equality for matching known
    // primitive types. Unknown schemas and coercive comparisons stay per-row.
    if (inner.some((key, i) => {
      if (key.type !== 'identifier') return true
      const type = correlationType(query, key, tables, cteColumns)
      return !type || type !== correlationType(select, keys.left[i], tables, cteColumns)
    })) return expr
    let name = `__scalar_${scalarJoins.length}`
    while ([...scopeColumns, ...scopeAliases].some(col => col === name || col.startsWith(name + '_'))) name += '_'
    const position = { positionStart: expr.positionStart, positionEnd: expr.positionEnd }
    const valueName = name + '_value'
    const keyName = name + '_key'
    const emptyAlias = name + '_empty'
    const innerKey = scalarCorrelationKey(inner)
    const outerKey = scalarCorrelationKey(keys.left)
    /** @type {IdentifierNode} */
    const joinedKey = { ...position, type: 'identifier', prefix: name, name: keyName }
    /** @type {SelectStatement} */
    const grouped = {
      ...keys.query,
      groupBy: [innerKey],
      // Evaluate the original projection before introducing synthetic aliases.
      columns: [
        { ...column, alias: valueName },
        { ...position, type: 'derived', expr: innerKey, alias: keyName },
      ],
    }
    const right = planQuery(grouped)
    const scope = statementScope(query) ?? []
    // A scalar aggregate over zero rows computes the exact empty-group value,
    // including COUNT arithmetic and aggregate expressions with lazy branches.
    /** @type {QueryPlan} */
    const empty = {
      type: 'Subquery', scope,
      child: {
        type: 'ScalarAggregate', columns: [{ ...column, alias: valueName }],
        child: { type: 'Limit', limit: 0, child: { type: 'SingleRow' } },
      },
    }
    /** @type {ExprNode | undefined} */
    let residual
    for (const key of keys.left) {
      // Map treats NaN as equal to itself; SQL equality does not.
      residual = andPredicate(residual, { ...position, type: 'binary', op: '=', left: key, right: key })
    }
    scalarJoins.push(left => ({
      type: 'HashJoin', joinType: 'LEFT', leftAlias: sourceAlias ?? '', rightAlias: name,
      leftKeys: [outerKey], rightKeys: [{ ...joinedKey, prefix: undefined }], residual,
      right: { type: 'Subquery', child: right, scope },
      left: {
        type: 'NestedLoopJoin', joinType: 'INNER', leftAlias: sourceAlias ?? '', rightAlias: emptyAlias, right: empty,
        condition: { ...position, type: 'literal', value: true }, left,
      },
    }))
    return {
      ...position, type: 'case',
      whenClauses: [{
        ...position,
        condition: { ...position, type: 'unary', op: 'IS NULL', argument: joinedKey },
        result: { ...position, type: 'identifier', prefix: emptyAlias, name: valueName },
      }],
      elseResult: { ...position, type: 'identifier', prefix: name, name: valueName },
    }
  }
  // Outer aggregates need group-aware placement, so retain their existing path.
  if (rewriteProjection) {
    for (let i = 0; i < columns.length; i++) {
      const col = columns[i]
      if (col.type === 'derived') {
        const before = scalarJoins.length
        const expr = mapScalarExpressions(col.expr, rewriteScalar)
        if (scalarJoins.length > before) columns[i] = { ...col, alias: col.alias ?? derivedAlias(col.expr), expr }
      }
    }
    orderBy = orderBy.map(term => ({ ...term, expr: mapScalarExpressions(term.expr, rewriteScalar) }))
  }
  /** @type {ExprNode | undefined} */
  let scalarWhere
  if (select.where) {
    /** @type {ExprNode[]} */
    const terms = []
    collectConjuncts(select.where, terms)
    /** @type {ExprNode | undefined} */
    let scanWhere
    for (const term of terms) {
      const before = scalarJoins.length
      const rewritten = mapScalarExpressions(term, rewriteScalar)
      if (scalarJoins.length > before) scalarWhere = andPredicate(scalarWhere, rewritten)
      else scanWhere = andPredicate(scanWhere, term)
    }
    select = { ...select, where: scanWhere }
  }
  return { select, columns, orderBy, scalarJoins, scalarWhere }
}

/**
 * Rewrites IN, EXISTS, and scalar subqueries into identifiers bound to mark
 * joins, so each subquery runs once per execution instead of once per row.
 * The caller places `markJoins` below the clauses it rewrote. Uncorrelated
 * subqueries and plain equality correlations qualify; anything else, and IN
 * operands that read aggregate or window results, stay on the per-row path.
 *
 * @param {object} options
 * @param {string} options.prefix - mark column name prefix
 * @param {string[]} options.scopeAliases
 * @param {Set<string>} options.scopeColumns
 * @param {(query: Statement) => QueryPlan} options.planQuery
 * @param {boolean} [options.uncorrelatedOnly] - for marks above an aggregate
 * @param {boolean} [options.scalarAggregates] - also take aggregate scalars
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @param {Map<string, string[]>} [options.cteColumns]
 * @returns {{ markJoins: Omit<SemiJoinNode, 'left'>[], rewrite: (expr: ExprNode, aggregateOnly?: boolean) => ExprNode }}
 */
export function markJoinPlanner({ prefix, scopeAliases, scopeColumns, planQuery, uncorrelatedOnly, scalarAggregates, tables, cteColumns }) {
  /** @type {Omit<SemiJoinNode, 'left'>[]} */
  const markJoins = []
  // The same node can appear in several clauses (GROUP BY 1, aliases).
  /** @type {Map<ExprNode, IdentifierNode>} */
  const marks = new Map()
  let rewrites = 0
  /**
   * @param {ExprNode} expr
   * @returns {ExprNode}
   */
  function rewriteMembership(expr) {
    const existing = marks.get(expr)
    if (existing) {
      rewrites++
      return existing
    }
    if (expr.type === 'in' && (findAggregate(expr.expr) || findWindow(expr.expr))) return expr
    // Uncorrelated aggregate scalars are already single-row cross joins.
    if (expr.type === 'subquery' && !scalarAggregates && expr.subquery.type === 'select' &&
        expr.subquery.columns.some(col => col.type === 'derived' && findAggregate(col.expr))) return expr
    const join = planMembership({ expr, scopeAliases, tables, cteColumns, planQuery })
    if (!join || uncorrelatedOnly && join.keys) return expr
    let name = `${prefix}${markJoins.length}`
    while ([...scopeColumns, ...scopeAliases].some(col => col === name || col.startsWith(name + '_'))) name += '_'
    markJoins.push({ ...join, not: expr.type === 'not exists', mark: name })
    /** @type {IdentifierNode} */
    const mark = { type: 'identifier', name, positionStart: expr.positionStart, positionEnd: expr.positionEnd }
    marks.set(expr, mark)
    rewrites++
    return mark
  }
  return {
    markJoins,
    rewrite(expr, aggregateOnly = false) {
      const before = rewrites
      const rewritten = mapScalarExpressions(expr, rewriteMembership, aggregateOnly)
      return rewrites === before ? expr : rewritten
    },
  }
}

/**
 * Replaces aggregate calls and plain column references with identifiers for
 * hidden aggregate output columns, so an expression from HAVING can be
 * evaluated above the aggregate. Output aliases are already available there.
 *
 * @param {ExprNode} node
 * @param {Set<string>} outputs
 * @param {(expr: ExprNode) => IdentifierNode} hide
 * @returns {ExprNode}
 */
export function hoistAggregateInputs(node, outputs, hide) {
  if (node.type === 'identifier') return !node.prefix && outputs.has(node.name) ? node : hide(node)
  if (node.type === 'function' && isAggregateFunc(node.funcName.toUpperCase())) return hide(node)
  if (node.type === 'in') return { ...node, expr: hoistAggregateInputs(node.expr, outputs, hide) }
  if (node.type === 'unary') return { ...node, argument: hoistAggregateInputs(node.argument, outputs, hide) }
  if (node.type === 'binary') {
    return { ...node, left: hoistAggregateInputs(node.left, outputs, hide), right: hoistAggregateInputs(node.right, outputs, hide) }
  }
  if (node.type === 'function') return { ...node, args: node.args.map(arg => hoistAggregateInputs(arg, outputs, hide)) }
  if (node.type === 'cast') return { ...node, expr: hoistAggregateInputs(node.expr, outputs, hide) }
  if (node.type === 'in valuelist') {
    return { ...node, expr: hoistAggregateInputs(node.expr, outputs, hide), values: node.values.map(v => hoistAggregateInputs(v, outputs, hide)) }
  }
  if (node.type === 'subscript') {
    return { ...node, expr: hoistAggregateInputs(node.expr, outputs, hide), index: hoistAggregateInputs(node.index, outputs, hide) }
  }
  if (node.type === 'case') {
    return {
      ...node,
      caseExpr: node.caseExpr && hoistAggregateInputs(node.caseExpr, outputs, hide),
      whenClauses: node.whenClauses.map(w => ({
        ...w,
        condition: hoistAggregateInputs(w.condition, outputs, hide),
        result: hoistAggregateInputs(w.result, outputs, hide),
      })),
      elseResult: node.elseResult && hoistAggregateInputs(node.elseResult, outputs, hide),
    }
  }
  return node
}

/**
 * True when an expression still evaluates a subquery per row.
 *
 * @param {ExprNode} expr
 * @returns {boolean}
 */
export function hasSubqueryExpression(expr) {
  let found = false
  mapScalarExpressions(expr, node => {
    if (node.type === 'in' || node.type === 'exists' || node.type === 'not exists' || node.type === 'subquery') found = true
    return node
  })
  return found
}

/**
 * Plans the right input of an IN, EXISTS, or scalar subquery so it is read
 * once per execution. Uncorrelated subqueries are planned as written. Plain
 * equality correlations become keys, and the right input yields [value (IN,
 * scalar), ...keys]. Returns undefined for anything else, which stays per-row.
 *
 * @param {object} options
 * @param {ExprNode} options.expr
 * @param {string[]} options.scopeAliases
 * @param {(query: Statement) => QueryPlan} options.planQuery
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @param {Map<string, string[]>} [options.cteColumns]
 * @returns {Omit<SemiJoinNode, 'left' | 'not'> | undefined}
 */
export function planMembership({ expr, scopeAliases, planQuery, tables, cteColumns }) {
  if (expr.type !== 'in' && expr.type !== 'exists' && expr.type !== 'not exists' && expr.type !== 'subquery') return
  const query = expr.subquery
  /** @type {SemiJoinNode['test']} */
  const test = expr.type === 'in' ? 'in' : expr.type === 'subquery' ? 'scalar' : 'exists'
  // Only IN has an operand; keep a placeholder so the node shape is uniform.
  /** @type {ExprNode} */
  const operand = expr.type === 'in' ? expr.expr : { type: 'literal', value: null, positionStart: expr.positionStart, positionEnd: expr.positionEnd }
  /** @type {IdentifierNode[]} */
  const correlated = []
  collectColumnsFromExpr({ type: 'subquery', subquery: query, positionStart: expr.positionStart, positionEnd: expr.positionEnd }, correlated, undefined, {
    tables, cteColumns, outerAliases: new Set(scopeAliases),
  })
  if (!correlated.length) {
    const scope = statementScope(query)
    const right = planQuery(query)
    return { type: 'SemiJoin', test, expr: operand, right: scope ? { type: 'Subquery', child: right, scope } : right }
  }
  const value = query.type === 'select' && query.columns.length === 1 ? query.columns[0] : undefined
  if (test !== 'exists' && value?.type !== 'derived') return
  // Decorrelation drops DISTINCT, which decides whether a scalar is one row.
  if (test === 'scalar' && query.type === 'select' && query.distinct) return
  const keys = decorrelateExists(query, scopeAliases, tables, cteColumns)
  if (!keys) return
  const columns = test !== 'exists' && value?.type === 'derived'
    ? [{ ...value, alias: '__membership_value' }, ...keys.query.columns]
    : keys.query.columns
  const right = planQuery({ ...keys.query, columns })
  return {
    type: 'SemiJoin', test, expr: operand, keys: keys.left,
    right: { type: 'Subquery', child: right, scope: statementScope(keys.query) ?? [] },
  }
}

/**
 * True when a scalar subquery produces exactly one row and reads nothing from
 * an enclosing query.
 *
 * @param {SelectStatement} query
 * @param {ExprNode} expr
 * @param {string[]} outerScope
 * @param {Record<string, AsyncDataSource>} [tables]
 * @param {Map<string, string[]>} [cteColumns]
 * @returns {boolean}
 */
function isSingleRowAggregate(query, expr, outerScope, tables, cteColumns) {
  if (query.groupBy.length || query.having || query.limit !== undefined || query.offset) return false
  /** @type {IdentifierNode[]} */
  const correlated = []
  collectColumnsFromExpr(expr, correlated, undefined, { tables, cteColumns, outerAliases: new Set(outerScope) })
  return !correlated.length
}

/**
 * Pulls plain equality correlations out of EXISTS or scalar aggregate queries.
 * Limits, HAVING, and existing grouping stay on the per-outer-row path.
 *
 * @param {Statement} query
 * @param {string[]} outerScope
 * @param {Record<string, AsyncDataSource>} [tables]
 * @param {Map<string, string[]>} [cteColumns]
 * @param {boolean} [aggregate]
 * @returns {{ query: SelectStatement, left: IdentifierNode[] } | undefined}
 */
export function decorrelateExists(query, outerScope, tables, cteColumns, aggregate = false) {
  if (query.type !== 'select' || query.limit !== undefined || query.offset ||
      query.groupBy.length || query.having ||
      !aggregate && query.columns.some(col => col.type === 'derived' && (findAggregate(col.expr) || findWindow(col.expr)))) return
  const scope = statementScope(query) ?? []
  /** @type {IdentifierNode[]} */
  const left = []
  /** @type {DerivedColumn[]} */
  const columns = []
  /** @type {ExprNode[]} */
  const conjuncts = []
  if (query.where) collectConjuncts(query.where, conjuncts)
  /** @type {ExprNode | undefined} */
  let where
  for (const term of conjuncts) {
    const previousKeys = columns.length
    if (term.type === 'binary' && term.op === '=' && term.left.type === 'identifier' && term.right.type === 'identifier') {
      for (const [inner, outer] of [[term.left, term.right], [term.right, term.left]]) {
        if (inner.prefix && scope.includes(inner.prefix) && outer.prefix &&
            outerScope.includes(outer.prefix) && !scope.includes(outer.prefix)) {
          left.push(outer)
          columns.push({ type: 'derived', expr: inner, alias: `__exists_key_${columns.length}`, positionStart: inner.positionStart, positionEnd: inner.positionEnd })
          break
        }
      }
      if (columns.length > previousKeys) continue
    }
    where = andPredicate(where, term)
  }
  if (!left.length) return
  // Check every remaining clause, including nested subqueries and projections,
  // before discarding the projection and ordering that EXISTS does not read.
  /** @type {IdentifierNode[]} */
  const remaining = []
  collectColumnsFromExpr({ type: 'subquery', subquery: { ...query, where }, positionStart: query.positionStart, positionEnd: query.positionEnd }, remaining, undefined, {
    tables, cteColumns, outerAliases: new Set(outerScope),
  })
  if (remaining.length) return
  return { query: { ...query, columns, where, distinct: false, orderBy: [] }, left }
}

/**
 * Rewrites scalar expressions without entering a nested lexical query scope.
 * @param {ExprNode} node
 * @param {(node: ExprNode) => ExprNode} rewrite
 * @param {boolean} [aggregateOnly]
 * @returns {ExprNode}
 */
function mapScalarExpressions(node, rewrite, aggregateOnly = false) {
  if (!node) return node
  // Only aggregate arguments read input rows. Other expressions must still
  // evaluate on the synthesized row when a scalar aggregate has no input.
  if (aggregateOnly && node.type === 'function' && isAggregateFunc(node.funcName.toUpperCase())) {
    return mapScalarExpressions(node, rewrite)
  }
  if (node.type === 'unary') {
    return { ...node, argument: mapScalarExpressions(node.argument, rewrite, aggregateOnly) }
  }
  if (node.type === 'binary') {
    return { ...node, left: mapScalarExpressions(node.left, rewrite, aggregateOnly), right: mapScalarExpressions(node.right, rewrite, aggregateOnly) }
  }
  if (node.type === 'function') {
    return { ...node, args: node.args.map(arg => mapScalarExpressions(arg, rewrite, aggregateOnly)) }
  }
  if (node.type === 'cast') {
    return { ...node, expr: mapScalarExpressions(node.expr, rewrite, aggregateOnly) }
  }
  if (node.type === 'in valuelist') {
    return {
      ...node,
      expr: mapScalarExpressions(node.expr, rewrite, aggregateOnly),
      values: node.values.map(v => mapScalarExpressions(v, rewrite, aggregateOnly)),
    }
  }
  if (node.type === 'subscript') {
    return {
      ...node,
      expr: mapScalarExpressions(node.expr, rewrite, aggregateOnly),
      index: mapScalarExpressions(node.index, rewrite, aggregateOnly),
    }
  }
  if (node.type === 'case') {
    return {
      ...node,
      caseExpr: mapScalarExpressions(node.caseExpr, rewrite, aggregateOnly),
      whenClauses: node.whenClauses.map(w => ({
        ...w,
        condition: mapScalarExpressions(w.condition, rewrite, aggregateOnly),
        result: mapScalarExpressions(w.result, rewrite, aggregateOnly),
      })),
      elseResult: mapScalarExpressions(node.elseResult, rewrite, aggregateOnly),
    }
  }
  return aggregateOnly ? node : rewrite(node)
}

/**
 * Looks up a correlation column's declared primitive type in its lexical scope.
 * Derived relations and CTEs need type propagation before they can use this rewrite.
 * @param {SelectStatement} query
 * @param {IdentifierNode} key
 * @param {Record<string, AsyncDataSource>} [tables]
 * @param {Map<string, string[]>} [cteColumns]
 * @returns {string | undefined}
 */
function correlationType(query, key, tables, cteColumns) {
  const { from } = query
  const table = from?.type === 'table' && (from.alias ?? from.table) === key.prefix ? from.table
    : query.joins.find(join => !join.subquery && !join.fromFunction && (join.alias ?? join.table) === key.prefix)?.table
  if (!table || cteColumns?.has(table)) return
  const type = tables?.[table]?.schema?.fields.find(field => field.name === key.name)?.dataType.type
  return type && ['string', 'number', 'bigint', 'boolean'].includes(type) ? type : undefined
}

/**
 * Encodes composite keys with length-prefixed components so ordinary hash
 * grouping and joins cannot confuse tuple boundaries. CONCAT propagates NULL.
 * @param {ExprNode[]} keys
 * @returns {ExprNode}
 */
function scalarCorrelationKey(keys) {
  if (keys.length === 1) return keys[0]
  const { positionStart, positionEnd } = keys[0]
  const position = { positionStart, positionEnd }
  return {
    ...position, type: 'function', funcName: 'CONCAT',
    args: keys.flatMap(key => {
      /** @type {ExprNode} */
      const text = { ...position, type: 'cast', expr: key, toType: 'TEXT' }
      return [
        /** @type {ExprNode} */ { ...position, type: 'function', funcName: 'LENGTH', args: [text] },
        /** @type {ExprNode} */ { ...position, type: 'literal', value: ':' },
        text,
      ]
    }),
  }
}
