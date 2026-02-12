import { extractColumns } from '../execute/columns.js'
import { findAggregate } from '../validation.js'

/**
 * @import { ExprNode, JoinClause, ScanOptions, SelectStatement } from '../types.js'
 * @import { QueryPlan } from './types.d.ts'
 */

/**
 * Builds a query plan from a SELECT statement AST.
 * Resolves CTEs at plan time so no planning occurs during execution.
 *
 * @param {SelectStatement} select - the SELECT statement AST
 * @returns {QueryPlan} the root of the query plan tree
 */
export function queryPlan(select) {
  // Build CTE plans in order (each CTE can reference preceding CTEs)
  /** @type {Map<string, QueryPlan>} */
  const ctePlans = new Map()
  if (select.with) {
    for (const cte of select.with.ctes) {
      const ctePlan = buildSelectPlan({ select: cte.query, ctePlans })
      ctePlans.set(cte.name.toLowerCase(), ctePlan)
    }
  }

  return buildSelectPlan({ select, ctePlans })
}

/**
 * Builds a plan for a SELECT statement with CTE resolution.
 *
 * @param {object} options
 * @param {SelectStatement} options.select
 * @param {Map<string, QueryPlan>} options.ctePlans
 * @returns {QueryPlan}
 */
function buildSelectPlan({ select, ctePlans }) {
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
  let plan = buildFromPlan({ select, ctePlans, hints })

  // Add JOINs
  if (select.joins.length) {
    plan = buildJoinPlan({ left: plan, joins: select.joins, leftTable: sourceAlias, ctePlans, perTableColumns })
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
    // Pass aliases so ORDER BY can reference SELECT column aliases
    if (select.orderBy.length) {
      /** @type {Map<string, ExprNode>} */
      const aliases = new Map()
      for (const col of select.columns) {
        if (col.kind === 'derived' && col.alias) {
          aliases.set(col.alias, col.expr)
        }
      }
      plan = { type: 'Sort', orderBy: select.orderBy, aliases: aliases.size > 0 ? aliases : undefined, child: plan }
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
 * Builds a plan for the FROM clause
 *
 * @param {object} options
 * @param {SelectStatement} options.select
 * @param {Map<string, QueryPlan>} options.ctePlans
 * @param {ScanOptions} options.hints
 * @returns {QueryPlan}
 */
function buildFromPlan({ select, ctePlans, hints }) {
  if (select.from.kind === 'table') {
    const ctePlan = ctePlans.get(select.from.table.toLowerCase())
    if (ctePlan) {
      return ctePlan
    }
    return {
      type: 'Scan',
      table: select.from.table,
      hints,
    }
  } else {
    return queryPlan(select.from.query)
  }
}

/**
 * Builds join plan nodes for all joins
 *
 * @param {object} options
 * @param {QueryPlan} options.left - the left side of the join (FROM or previous joins)
 * @param {JoinClause[]} options.joins - array of join clauses
 * @param {string} options.leftTable - name/alias of the left table
 * @param {Map<string, QueryPlan>} options.ctePlans
 * @param {Map<string, string[] | undefined>} [options.perTableColumns]
 * @returns {QueryPlan}
 */
function buildJoinPlan({ left, joins, leftTable, ctePlans, perTableColumns }) {
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
