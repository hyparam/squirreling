import { extractColumns } from '../execute/columns.js'
import { findAggregate } from '../validation.js'

/**
 * @import { BinaryNode, ExprNode, JoinClause, ScanOptions, SelectStatement } from '../types.js'
 * @import { QueryPlan, ScanNode } from './types.d.ts'
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
      const ctePlan = buildSelectPlan(cte.query, ctePlans)
      ctePlans.set(cte.name.toLowerCase(), ctePlan)
    }
  }

  return buildSelectPlan(select, ctePlans)
}

/**
 * Builds a plan for a SELECT statement with CTE resolution.
 *
 * @param {SelectStatement} select - the SELECT statement AST
 * @param {Map<string, QueryPlan>} ctePlans
 * @returns {QueryPlan} the root of the query plan tree
 */
function buildSelectPlan(select, ctePlans) {
  // Check for aggregation
  const hasAggregate = select.columns.some(col =>
    col.kind === 'derived' && findAggregate(col.expr)
  )
  const useGrouping = hasAggregate || select.groupBy.length > 0
  const needsBuffering = useGrouping || select.orderBy.length > 0
  const hasJoins = select.joins.length > 0
  // Only delegate offset/limit when no grouping, no joins, and simple FROM table (no CTE or subquery)
  const fromIsBaseTable = select.from.kind === 'table' && !ctePlans.has(select.from.table.toLowerCase())
  const canDelegateOffset = fromIsBaseTable && !needsBuffering && !select.distinct && !hasJoins

  // Compute query hints for data source optimization
  /** @type {ScanOptions} */
  const hints = {
    columns: extractColumns(select),
    where: hasJoins ? undefined : select.where,
  }
  // Only pass limit/offset when safe to delegate to data source
  if (canDelegateOffset) {
    hints.limit = select.limit
    hints.offset = select.offset
  }

  // Start with the data source (FROM clause)
  /** @type {QueryPlan} */
  let plan = buildFromPlan(select, ctePlans, hints)

  // Whether the WHERE can be handled by executeScan (direct table scan, no JOINs)
  const scanHandlesWhere = plan.type === 'Scan' && !hasJoins

  // Add JOINs
  if (select.joins.length) {
    const sourceAlias = select.from.kind === 'table'
      ? select.from.alias ?? select.from.table
      : select.from.alias
    plan = buildJoinPlan(plan, select.joins, sourceAlias, ctePlans)
  }

  // Add WHERE filter when executeScan can't handle it (JOINs, subqueries, CTEs)
  if (select.where && !scanHandlesWhere) {
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

    const effectiveOffset = canDelegateOffset ? undefined : select.offset
    const effectiveLimit = canDelegateOffset ? undefined : select.limit
    if (effectiveLimit !== undefined || effectiveOffset !== undefined) {
      plan = { type: 'Limit', limit: effectiveLimit, offset: effectiveOffset, child: plan }
    }
  }

  return plan
}

/**
 * Builds a plan for the FROM clause
 *
 * @param {SelectStatement} select
 * @param {Map<string, QueryPlan>} ctePlans
 * @param {ScanOptions} hints - scan options to pass to data source
 * @returns {QueryPlan}
 */
function buildFromPlan(select, ctePlans, hints) {
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
 * @param {QueryPlan} left - the left side of the join (FROM or previous joins)
 * @param {JoinClause[]} joins - array of join clauses
 * @param {string} leftTable - name/alias of the left table
 * @param {Map<string, QueryPlan>} ctePlans
 * @returns {QueryPlan}
 */
function buildJoinPlan(left, joins, leftTable, ctePlans) {
  let plan = left
  let currentLeftTable = leftTable

  for (const join of joins) {
    const rightTable = join.alias ?? join.table

    const ctePlan = ctePlans.get(join.table.toLowerCase())
    /** @type {QueryPlan} */
    const rightScan = ctePlan ?? { type: 'Scan', table: join.table } // TODO: pass hints

    if (join.joinType === 'POSITIONAL') {
      plan = { type: 'PositionalJoin', leftAlias: currentLeftTable, rightAlias: rightTable, left: plan, right: rightScan }
    } else if (join.on && canExtractEqualityKeys(join.on)) {
      const keys = extractJoinKeyPair(join.on, currentLeftTable, rightTable)
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

    // Update left table name for next join
    currentLeftTable = `${currentLeftTable}_${rightTable}`
  }

  return plan
}

/**
 * Checks if a join condition can be used for hash join (simple equality)
 *
 * @param {ExprNode} condition
 * @returns {condition is BinaryNode}
 */
function canExtractEqualityKeys(condition) {
  if (condition.type !== 'binary' || condition.op !== '=') {
    return false
  }
  // Check that both sides are identifiers (potentially qualified)
  return condition.left.type === 'identifier' && condition.right.type === 'identifier'
}

/**
 * Extracts left and right key expressions from an equality join condition
 *
 * @param {BinaryNode} condition
 * @param {string} leftTable
 * @param {string} rightTable
 * @returns {{ leftKey: ExprNode, rightKey: ExprNode }}
 */
function extractJoinKeyPair(condition, leftTable, rightTable) {
  const { left, right } = condition

  // Check if keys are in swapped order (right table ref on left side)
  const leftRefsRight = left.type === 'identifier' && left.name.startsWith(`${rightTable}.`)
  const rightRefsLeft = right.type === 'identifier' && right.name.startsWith(`${leftTable}.`)

  if (leftRefsRight && rightRefsLeft) {
    return { leftKey: right, rightKey: left }
  }

  return { leftKey: left, rightKey: right }
}
