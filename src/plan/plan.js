import { derivedAlias } from '../expression/alias.js'
import { parseSql } from '../parse/parse.js'
import { findAggregate } from '../validation/aggregates.js'
import { ColumnNotFoundError, TableNotFoundError } from '../validation/planErrors.js'
import { validateScan, validateTableRefs } from '../validation/tables.js'
import { extractColumns, fromAlias, inferStatementColumns } from './columns.js'

/**
 * @import { AsyncDataSource, ExprNode, DerivedColumn, IdentifierNode, JoinClause, PlanSqlOptions, ScanOptions, SelectColumn, SelectStatement, SetOperationStatement, Statement } from '../types.js'
 * @import { QueryPlan } from './types.d.ts'
 */

/**
 * Builds a query plan from a statement AST.
 * Resolves CTEs at plan time so no planning occurs during execution.
 *
 * @param {PlanSqlOptions} options
 * @returns {QueryPlan} the root of the query plan tree
 */
export function planSql({ query, functions, tables }) {
  /** @type {Statement} */
  const stmt = typeof query === 'string' ? parseSql({ query, functions }) : query
  return planStatement({ stmt, tables })
}

/**
 * Plans a Statement (SelectStatement, SetOperationStatement, or WithStatement).
 *
 * @param {object} options
 * @param {Statement} options.stmt
 * @param {Map<string, QueryPlan>} [options.ctePlans]
 * @param {Map<string, string[]>} [options.cteColumns]
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @param {IdentifierNode[]} [options.parentColumns] - columns needed by the parent query (for subquery pushdown)
 * @returns {QueryPlan}
 */
function planStatement({ stmt, ctePlans, cteColumns, tables, parentColumns }) {
  if (stmt.type === 'with') {
    // Build CTE plans in order (each CTE can reference preceding CTEs)
    ctePlans ??= new Map()
    cteColumns ??= new Map()
    for (const cte of stmt.ctes) {
      const ctePlan = planStatement({ stmt: cte.query, ctePlans, cteColumns, tables })
      ctePlans.set(cte.name.toLowerCase(), ctePlan)
      cteColumns.set(cte.name.toLowerCase(), inferStatementColumns({ stmt: cte.query, cteColumns, tables }))
    }
    return planStatement({ stmt: stmt.query, ctePlans, cteColumns, tables, parentColumns })
  }
  if (stmt.type === 'compound') {
    return planSetOperation({ compound: stmt, ctePlans, cteColumns, tables })
  }
  return planSelect({ select: stmt, ctePlans, cteColumns, tables, parentColumns })
}

/**
 * Plans a SetOperationStatement (UNION/INTERSECT/EXCEPT).
 *
 * @param {object} options
 * @param {SetOperationStatement} options.compound
 * @param {Map<string, QueryPlan>} [options.ctePlans]
 * @param {Map<string, string[]>} [options.cteColumns]
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @returns {QueryPlan}
 */
function planSetOperation({ compound, ctePlans, cteColumns, tables }) {
  const left = planStatement({ stmt: compound.left, ctePlans, cteColumns, tables })
  const right = planStatement({ stmt: compound.right, ctePlans, cteColumns, tables })
  const leftColumns = inferStatementColumns({ stmt: compound.left, cteColumns, tables })
  const rightColumns = inferStatementColumns({ stmt: compound.right, cteColumns, tables })

  if (leftColumns.length !== rightColumns.length || leftColumns.some((col, idx) => col !== rightColumns[idx])) {
    throw new Error(`Set operation operands must have identical columns, got left [${leftColumns.join(', ')}] and right [${rightColumns.join(', ')}]`)
  }

  /** @type {QueryPlan} */
  let plan = {
    type: 'SetOperation',
    operator: compound.operator,
    all: compound.all,
    left,
    right,
  }

  if (compound.orderBy.length) {
    plan = { type: 'Sort', orderBy: compound.orderBy, child: plan }
  }
  if (compound.limit !== undefined || compound.offset) {
    plan = { type: 'Limit', limit: compound.limit, offset: compound.offset, child: plan }
  }

  return plan
}

/**
 * Builds a plan for a SELECT statement with CTEs pre-resolved.
 *
 * @param {object} options
 * @param {SelectStatement} options.select
 * @param {Map<string, QueryPlan>} [options.ctePlans]
 * @param {Map<string, string[]>} [options.cteColumns]
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @param {IdentifierNode[]} [options.parentColumns] - columns needed by the parent query (for subquery pushdown)
 * @returns {QueryPlan}
 */
function planSelect({ select, ctePlans, cteColumns, tables, parentColumns }) {
  // Check for aggregation
  const hasAggregate = select.columns.some(col =>
    col.type === 'derived' && findAggregate(col.expr)
  )
  const useGrouping = hasAggregate || select.groupBy.length > 0
  const needsBuffering = useGrouping || select.orderBy.length > 0

  // Source alias for FROM clause
  const sourceAlias = fromAlias(select.from)

  // Validate qualified references and resolve aliases
  const scopeTables = Object.fromEntries([sourceAlias, ...select.joins.map(j => j.alias ?? j.table)].map(a => [a, true]))
  /** @type {Map<string, ExprNode>} */
  const aliases = new Map()
  const columns = select.columns.map(col => {
    if (col.type === 'derived') {
      validateTableRefs(col.expr, scopeTables)
      const expr = resolveAliases(col.expr, aliases)
      if (col.alias) {
        aliases.set(col.alias, expr)
      }
      return { ...col, expr }
    }
    // Validate qualified references
    if (col.table && !(col.table in scopeTables)) {
      throw new TableNotFoundError({ table: col.table, tables: scopeTables })
    }
    return col
  })

  // Validate qualified references in other clauses
  validateTableRefs(select.where, scopeTables)
  validateTableRefs(select.having, scopeTables)
  for (const expr of select.groupBy) {
    validateTableRefs(expr, scopeTables)
  }
  for (const term of select.orderBy) {
    validateTableRefs(term.expr, scopeTables)
  }
  for (const join of select.joins) {
    validateTableRefs(join.on, scopeTables)
  }

  // Determine scan hints for direct table scans (WHERE and LIMIT/OFFSET are
  // included so they are only applied to fresh scans, not CTE/subquery plans)
  /** @type {ScanOptions} */
  const hints = {}
  const perTableColumns = extractColumns({ select, parentColumns })
  hints.columns = perTableColumns.get(sourceAlias)
  if (!select.joins.length) {
    hints.where = select.where
    if (!needsBuffering && !select.distinct) {
      hints.limit = select.limit
      hints.offset = select.offset
    }
  }

  // Start with the data source (FROM clause)
  /** @type {QueryPlan} */
  let plan = planFrom({ select, ctePlans, cteColumns, hints, tables })

  // Add JOINs
  if (select.joins.length) {
    plan = planJoin({ left: plan, joins: select.joins, leftTable: sourceAlias, ctePlans, cteColumns, perTableColumns, tables })
  }

  // Whether FROM resolved to our own direct table scan
  const isOwnScan = plan.type === 'Scan' && plan.hints === hints

  // Add WHERE filter when the scan didn't receive it
  if (select.where && !isOwnScan) {
    plan = { type: 'Filter', condition: select.where, child: plan }
  }

  if (useGrouping) {
    // Aggregation path: GROUP BY or scalar aggregate
    // HAVING is integrated into aggregate nodes for access to group context
    if (select.groupBy.length) {
      // Resolve SELECT aliases in GROUP BY expressions at plan time
      const groupBy = aliases.size > 0
        ? select.groupBy.map(expr => resolveAliases(expr, aliases))
        : select.groupBy
      plan = { type: 'HashAggregate', groupBy, columns, having: select.having, child: plan }
    } else if (!select.having && !select.where && plan.type === 'Scan' && isOwnScan && isAllCountStar(select.columns)) {
      plan = { type: 'Count', table: plan.table, columns: select.columns }
    } else {
      plan = { type: 'ScalarAggregate', columns, having: select.having, child: plan }
    }

    // ORDER BY (after aggregation)
    if (select.orderBy.length) {
      plan = { type: 'Sort', orderBy: select.orderBy, child: plan }
    }

    // DISTINCT
    if (select.distinct) {
      plan = { type: 'Distinct', child: plan }
    }

    // LIMIT/OFFSET
    if (select.limit !== undefined || select.offset) {
      plan = { type: 'Limit', limit: select.limit, offset: select.offset, child: plan }
    }
  } else {
    // Non-aggregation path

    // ORDER BY (before projection so it can access all columns)
    // Resolve SELECT aliases in ORDER BY expressions at plan time
    if (select.orderBy.length) {
      const orderBy = aliases.size > 0
        ? select.orderBy.map(term => ({ ...term, expr: resolveAliases(term.expr, aliases) }))
        : select.orderBy
      plan = { type: 'Sort', orderBy, child: plan }
    }

    // DISTINCT needs to come after projection but before LIMIT
    // However, for streaming distinct we need to project first
    // So the order is: Sort -> Project -> Distinct -> Limit

    // Fast path for SELECT * without joins
    const isPassthrough = select.columns.length === 1 && select.columns[0].type === 'star' && !select.joins.length
    if (!isPassthrough) {
      let projectColumns = columns
      // When parent only needs specific columns, drop unneeded projections
      if (parentColumns) {
        projectColumns = projectColumns.filter(col =>
          col.type === 'star' || parentColumns.some(id => id.name === (col.alias ?? derivedAlias(col.expr)))
        )
      }
      plan = { type: 'Project', columns: projectColumns, child: plan }
    }

    if (select.distinct) {
      plan = { type: 'Distinct', child: plan }
    }

    if (!(isOwnScan && !needsBuffering && !select.distinct) && (select.limit !== undefined || select.offset)) {
      plan = { type: 'Limit', limit: select.limit, offset: select.offset, child: plan }
    }
  }

  return plan
}

/**
 * @param {object} options
 * @param {SelectStatement} options.select
 * @param {Map<string, QueryPlan>} [options.ctePlans]
 * @param {Map<string, string[]>} [options.cteColumns]
 * @param {ScanOptions} options.hints
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @returns {QueryPlan}
 */
function planFrom({ select, ctePlans, cteColumns, hints, tables }) {
  if (select.from.type === 'table') {
    const ctePlan = ctePlans?.get(select.from.table.toLowerCase())
    if (ctePlan) {
      return ctePlan
    }
    validateScan({ ...select.from, hints, tables })
    return { type: 'Scan', table: select.from.table, hints }
  } else {
    const subPlan = planStatement({
      stmt: select.from.query,
      ctePlans,
      cteColumns,
      tables,
      parentColumns: hints.columns?.map(name => ({ type: 'identifier', name, positionStart: 0, positionEnd: 0 })),
    })
    // Validate that requested columns exist in subquery output
    const availableColumns = inferStatementColumns({ stmt: select.from.query, cteColumns, tables })
    if (hints.columns && availableColumns.length) {
      const missingColumn = hints.columns.find(col => !availableColumns.includes(col))
      if (missingColumn) {
        throw new ColumnNotFoundError({ missingColumn, availableColumns, ...select.from })
      }
    }
    return subPlan
  }
}

/**
 * @param {object} options
 * @param {QueryPlan} options.left - the left side of the join (FROM or previous joins)
 * @param {JoinClause[]} options.joins - array of join clauses
 * @param {string} options.leftTable - name/alias of the left table
 * @param {Map<string, QueryPlan>} [options.ctePlans]
 * @param {Map<string, string[]>} [options.cteColumns]
 * @param {Map<string, string[] | undefined>} options.perTableColumns
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @returns {QueryPlan}
 */
function planJoin({ left, joins, leftTable, ctePlans, cteColumns, perTableColumns, tables }) {
  let plan = left
  let currentLeftTable = leftTable

  for (const join of joins) {
    const rightTable = join.alias ?? join.table

    const ctePlan = ctePlans?.get(join.table.toLowerCase())
    /** @type {ScanOptions} */
    const rightHints = {}
    if (!ctePlan) {
      rightHints.columns = perTableColumns.get(rightTable)
      validateScan({ ...join, hints: rightHints, tables })
    } else {
      // For CTE joins, use CTE column metadata for hints
      rightHints.columns = perTableColumns.get(rightTable) ?? cteColumns?.get(join.table.toLowerCase())
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
 * @param {ExprNode | undefined} node
 * @param {Map<string, ExprNode>} aliases
 * @returns {ExprNode}
 */
function resolveAliases(node, aliases) {
  if (!node || !aliases.size) return node
  if (node.type === 'identifier') {
    return node.prefix ? node : aliases.get(node.name) ?? node
  }
  if (node.type === 'unary') {
    return { ...node, argument: resolveAliases(node.argument, aliases) }
  }
  if (node.type === 'binary') {
    const left = resolveAliases(node.left, aliases)
    const right = resolveAliases(node.right, aliases)
    return { ...node, left, right }
  }
  if (node.type === 'function') {
    const args = node.args.map(arg => resolveAliases(arg, aliases))
    return { ...node, args }
  }
  if (node.type === 'cast') {
    return { ...node, expr: resolveAliases(node.expr, aliases) }
  }
  if (node.type === 'in valuelist') {
    const expr = resolveAliases(node.expr, aliases)
    const values = node.values.map(v => resolveAliases(v, aliases))
    return { ...node, expr, values }
  }
  if (node.type === 'case') {
    const caseExpr = resolveAliases(node.caseExpr, aliases)
    const whenClauses = node.whenClauses.map(w => {
      const condition = resolveAliases(w.condition, aliases)
      const result = resolveAliases(w.result, aliases)
      return { ...w, condition, result }
    })
    const elseResult = resolveAliases(node.elseResult, aliases)
    return { ...node, caseExpr, whenClauses, elseResult }
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
  if (condition.type !== 'binary' || condition.op !== '=') return
  const { left, right } = condition
  if (left.type !== 'identifier' || right.type !== 'identifier') return

  // Check if keys are in swapped order (right table ref on left side)
  const leftRefsRight = left.prefix === rightTable
  const rightRefsLeft = right.prefix === leftTable

  if (leftRefsRight && rightRefsLeft) {
    return { leftKey: right, rightKey: left }
  }

  return { leftKey: left, rightKey: right }
}

/**
 * Checks if every SELECT column is a plain COUNT(*).
 *
 * @param {SelectColumn[]} columns
 * @returns {columns is DerivedColumn[]}
 */
function isAllCountStar(columns) {
  if (columns.length === 0) return false
  return columns.every(col =>
    col.type === 'derived' &&
    col.expr.type === 'function' &&
    col.expr.funcName.toUpperCase() === 'COUNT' &&
    col.expr.args.length === 1 &&
    col.expr.args[0].type === 'star' &&
    !col.expr.distinct &&
    !col.expr.filter
  )
}
