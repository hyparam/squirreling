import { parseSql } from '../parse/parse.js'
import { findAggregate } from '../validation.js'
import { extractColumns } from './columns.js'

/**
 * @import { ExprNode, JoinClause, PlanSqlOptions, ScanOptions, SelectStatement } from '../types.js'
 * @import { QueryPlan } from './types.d.ts'
 */

/**
 * Builds a query plan from a SELECT statement AST.
 * Resolves CTEs at plan time so no planning occurs during execution.
 *
 * @param {PlanSqlOptions} options
 * @returns {QueryPlan} the root of the query plan tree
 */
export function planSql({ query, functions }) {
  const select = typeof query === 'string' ? parseSql({ query, functions }) : query

  // Build CTE plans in order (each CTE can reference preceding CTEs)
  /** @type {Map<string, QueryPlan>} */
  const ctePlans = new Map()
  if (select.with) {
    for (const cte of select.with.ctes) {
      const ctePlan = planSelect({ select: cte.query, ctePlans })
      ctePlans.set(cte.name.toLowerCase(), ctePlan)
    }
  }

  return planSelect({ select, ctePlans })
}

/**
 * Builds a plan for a SELECT statement with CTEs pre-resolved.
 *
 * @param {object} options
 * @param {SelectStatement} options.select
 * @param {Map<string, QueryPlan>} options.ctePlans
 * @returns {QueryPlan}
 */
function planSelect({ select, ctePlans }) {
  // Check for aggregation
  const hasAggregate = select.columns.some(col =>
    col.kind === 'derived' && findAggregate(col.expr)
  )
  const useGrouping = hasAggregate || select.groupBy.length > 0
  const needsBuffering = useGrouping || select.orderBy.length > 0

  // Source alias for FROM clause
  const sourceAlias = select.from.kind === 'table'
    ? select.from.alias ?? select.from.table
    : select.from.alias

  // Determine per-table column hints for pushdown
  /** @type {ScanOptions} */
  const hints = {}
  const perTableColumns = extractColumns(select)
  hints.columns = perTableColumns.get(sourceAlias)

  // Start with the data source (FROM clause)
  /** @type {QueryPlan} */
  let plan = planFrom({ select, ctePlans, hints })

  // Add JOINs
  if (select.joins.length) {
    plan = planJoin({ left: plan, joins: select.joins, leftTable: sourceAlias, ctePlans, perTableColumns })
  }

  // Delegate WHERE and LIMIT/OFFSET to scan when plan is a direct table scan
  if (plan.type === 'Scan') {
    plan.hints.where = select.where
    if (!needsBuffering && !select.distinct) {
      plan.hints.limit = select.limit
      plan.hints.offset = select.offset
    }
  }

  // Add WHERE filter when scan can't handle it (JOINs, subqueries, CTEs)
  const isScan = plan.type === 'Scan'
  if (select.where && !isScan) {
    plan = { type: 'Filter', condition: select.where, child: plan }
  }

  if (useGrouping) {
    // Aggregation path: GROUP BY or scalar aggregate
    // HAVING is integrated into aggregate nodes for access to group context
    plan = select.groupBy.length
      ? { type: 'HashAggregate', groupBy: select.groupBy, columns: select.columns, having: select.having, child: plan }
      : { type: 'ScalarAggregate', columns: select.columns, having: select.having, child: plan }

    // ORDER BY (after aggregation)
    if (select.orderBy.length) {
      plan = { type: 'Sort', orderBy: select.orderBy, child: plan }
    }

    // DISTINCT
    if (select.distinct) {
      plan = { type: 'Distinct', child: plan }
    }

    // LIMIT/OFFSET
    if (select.limit !== undefined || select.offset !== undefined) {
      plan = { type: 'Limit', limit: select.limit, offset: select.offset, child: plan }
    }
  } else {
    // Non-aggregation path

    // ORDER BY (before projection so it can access all columns)
    // Resolve SELECT aliases in ORDER BY expressions at plan time
    if (select.orderBy.length) {
      /** @type {Map<string, ExprNode>} */
      const aliases = new Map()
      for (const col of select.columns) {
        if (col.kind === 'derived' && col.alias) {
          aliases.set(col.alias, col.expr)
        }
      }
      const orderBy = aliases.size > 0
        ? select.orderBy.map(term => ({ ...term, expr: resolveAliases(term.expr, aliases) }))
        : select.orderBy
      plan = { type: 'Sort', orderBy, child: plan }
    }

    // DISTINCT needs to come after projection but before LIMIT
    // However, for streaming distinct we need to project first
    // So the order is: Sort -> Project -> Distinct -> Limit
    plan = { type: 'Project', columns: select.columns, child: plan }

    if (select.distinct) {
      plan = { type: 'Distinct', child: plan }
    }

    if (!(isScan && !needsBuffering && !select.distinct) && (select.limit !== undefined || select.offset !== undefined)) {
      plan = { type: 'Limit', limit: select.limit, offset: select.offset, child: plan }
    }
  }

  return plan
}

/**
 * @param {object} options
 * @param {SelectStatement} options.select
 * @param {Map<string, QueryPlan>} options.ctePlans
 * @param {ScanOptions} options.hints
 * @returns {QueryPlan}
 */
function planFrom({ select, ctePlans, hints }) {
  if (select.from.kind === 'table') {
    const ctePlan = ctePlans.get(select.from.table.toLowerCase())
    if (ctePlan) {
      return ctePlan
    }
    return { type: 'Scan', table: select.from.table, hints }
  } else {
    if (select.from.query.with) {
      throw new Error('WITH clause is not supported inside subqueries')
    }
    return planSelect({ select: select.from.query, ctePlans })
  }
}

/**
 * @param {object} options
 * @param {QueryPlan} options.left - the left side of the join (FROM or previous joins)
 * @param {JoinClause[]} options.joins - array of join clauses
 * @param {string} options.leftTable - name/alias of the left table
 * @param {Map<string, QueryPlan>} options.ctePlans
 * @param {Map<string, string[] | undefined>} options.perTableColumns
 * @returns {QueryPlan}
 */
function planJoin({ left, joins, leftTable, ctePlans, perTableColumns }) {
  let plan = left
  let currentLeftTable = leftTable

  for (const join of joins) {
    const rightTable = join.alias ?? join.table

    const ctePlan = ctePlans.get(join.table.toLowerCase())
    /** @type {ScanOptions} */
    const rightHints = {}
    if (!ctePlan) {
      rightHints.columns = perTableColumns?.get(rightTable)
    }
    /** @type {QueryPlan} */
    const rightScan = ctePlan ?? { type: 'Scan', table: join.table, hints: rightHints }

    if (join.joinType === 'POSITIONAL') {
      plan = { type: 'PositionalJoin', leftAlias: currentLeftTable, rightAlias: rightTable, left: plan, right: rightScan }
    } else {
      const keys = join.on && extractSimpleJoinKeys({ condition: join.on, leftTable: currentLeftTable, rightTable })
      if (keys) {
        plan = {
          type: 'HashJoin',
          joinType: join.joinType,
          leftAlias: currentLeftTable,
          rightAlias: rightTable,
          leftKey: keys.leftKey,
          rightKey: keys.rightKey,
          left: plan,
          right: rightScan,
        }
      } else {
        plan = {
          type: 'NestedLoopJoin',
          joinType: join.joinType,
          leftAlias: currentLeftTable,
          rightAlias: rightTable,
          condition: join.on,
          left: plan,
          right: rightScan,
        }
      }
    }

    // Update left table name for next join
    currentLeftTable = `${currentLeftTable}_${rightTable}`
  }

  return plan
}

/**
 * Recursively replaces identifier nodes that match SELECT aliases
 * with their aliased expressions.
 *
 * @param {ExprNode} node
 * @param {Map<string, ExprNode>} aliases
 * @returns {ExprNode}
 */
function resolveAliases(node, aliases) {
  if (node.type === 'identifier') {
    const resolved = aliases.get(node.name)
    if (resolved) return resolved
    return node
  }
  if (node.type === 'unary') {
    const argument = resolveAliases(node.argument, aliases)
    return argument === node.argument ? node : { ...node, argument }
  }
  if (node.type === 'binary') {
    const left = resolveAliases(node.left, aliases)
    const right = resolveAliases(node.right, aliases)
    return left === node.left && right === node.right ? node : { ...node, left, right }
  }
  if (node.type === 'function') {
    const args = node.args.map(arg => resolveAliases(arg, aliases))
    const changed = args.some((arg, i) => arg !== node.args[i])
    return changed ? { ...node, args } : node
  }
  if (node.type === 'cast') {
    const expr = resolveAliases(node.expr, aliases)
    return expr === node.expr ? node : { ...node, expr }
  }
  if (node.type === 'in valuelist') {
    const expr = resolveAliases(node.expr, aliases)
    const values = node.values.map(v => resolveAliases(v, aliases))
    const changed = expr !== node.expr || values.some((v, i) => v !== node.values[i])
    return changed ? { ...node, expr, values } : node
  }
  if (node.type === 'case') {
    const caseExpr = node.caseExpr ? resolveAliases(node.caseExpr, aliases) : node.caseExpr
    const whenClauses = node.whenClauses.map(w => {
      const condition = resolveAliases(w.condition, aliases)
      const result = resolveAliases(w.result, aliases)
      return condition === w.condition && result === w.result ? w : { ...w, condition, result }
    })
    const elseResult = node.elseResult ? resolveAliases(node.elseResult, aliases) : node.elseResult
    const changed = caseExpr !== node.caseExpr || elseResult !== node.elseResult || whenClauses.some((w, i) => w !== node.whenClauses[i])
    return changed ? { ...node, caseExpr, whenClauses, elseResult } : node
  }
  // literal, interval, subquery, in, exists: no identifiers to resolve
  return node
}

/**
 * Extracts left and right key expressions from a simple equality join condition.
 * Returns undefined if the condition is not a simple equality between identifiers.
 *
 * @param {object} options
 * @param {ExprNode} options.condition
 * @param {string} options.leftTable
 * @param {string} options.rightTable
 * @returns {{ leftKey: ExprNode, rightKey: ExprNode } | undefined}
 */
function extractSimpleJoinKeys({ condition, leftTable, rightTable }) {
  if (condition.type !== 'binary' || condition.op !== '=') {
    return undefined
  }
  const { left, right } = condition
  if (left.type !== 'identifier' || right.type !== 'identifier') {
    return undefined
  }

  // Check if keys are in swapped order (right table ref on left side)
  const leftRefsRight = left.name.startsWith(`${rightTable}.`)
  const rightRefsLeft = right.name.startsWith(`${leftTable}.`)

  if (leftRefsRight && rightRefsLeft) {
    return { leftKey: right, rightKey: left }
  }

  return { leftKey: left, rightKey: right }
}
