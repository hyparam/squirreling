import { derivedAlias } from '../expression/alias.js'
import { parseSql } from '../parse/parse.js'
import { findAggregate } from '../validation/aggregates.js'
import { ParseError } from '../validation/parseErrors.js'
import { ColumnNotFoundError, TableNotFoundError } from '../validation/tables.js'
import { validateNoIdentifiers, validateScan, validateTableRefs } from '../validation/tables.js'
import { extractColumns, fromAlias, inferSelectSourceColumns, inferStatementColumns, tableFunctionColumnNames } from './columns.js'

/**
 * @import { AsyncDataSource, ExprNode, DerivedColumn, IdentifierNode, JoinClause, PlanSqlOptions, ScanOptions, SelectColumn, SelectStatement, SetOperationStatement, Statement, WindowFunctionNode } from '../types.js'
 * @import { QueryPlan, WindowSpec } from './types.d.ts'
 */

/**
 * Builds a query plan from a statement AST.
 * Resolves CTEs at plan time so no planning occurs during execution.
 *
 * @param {PlanSqlOptions} options
 * @returns {QueryPlan} the root of the query plan tree
 */
export function planSql({ query, functions, tables, ctePlans, cteColumns }) {
  /** @type {Statement} */
  const stmt = typeof query === 'string' ? parseSql({ query, functions }) : query
  return planStatement({ stmt, tables, ctePlans, cteColumns })
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
 * @param {string[]} [options.outerScope] - aliases from an outer query (for correlated subqueries)
 * @returns {QueryPlan}
 */
export function planStatement({ stmt, ctePlans, cteColumns, tables, parentColumns, outerScope }) {
  if (stmt.type === 'with') {
    // Build CTE plans in order (each CTE can reference preceding CTEs)
    ctePlans ??= new Map()
    cteColumns ??= new Map()
    for (const cte of stmt.ctes) {
      const ctePlan = planStatement({ stmt: cte.query, ctePlans, cteColumns, tables })
      ctePlans.set(cte.name.toLowerCase(), ctePlan)
      cteColumns.set(cte.name.toLowerCase(), inferStatementColumns({ stmt: cte.query, cteColumns, tables }))
    }
    return planStatement({ stmt: stmt.query, ctePlans, cteColumns, tables, parentColumns, outerScope })
  }
  if (stmt.type === 'compound') {
    return planSetOperation({ compound: stmt, ctePlans, cteColumns, tables, parentColumns })
  }
  return planSelect({ select: stmt, ctePlans, cteColumns, tables, parentColumns, outerScope })
}

/**
 * Plans a SetOperationStatement (UNION/INTERSECT/EXCEPT).
 *
 * @param {object} options
 * @param {SetOperationStatement} options.compound
 * @param {Map<string, QueryPlan>} [options.ctePlans]
 * @param {Map<string, string[]>} [options.cteColumns]
 * @param {Record<string, AsyncDataSource>} [options.tables]
 * @param {IdentifierNode[]} [options.parentColumns] - columns needed by the parent query
 * @returns {QueryPlan}
 */
function planSetOperation({ compound, ctePlans, cteColumns, tables, parentColumns }) {
  const left = planStatement({ stmt: compound.left, ctePlans, cteColumns, tables, parentColumns })
  const right = planStatement({ stmt: compound.right, ctePlans, cteColumns, tables, parentColumns })
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
 * @param {string[]} [options.outerScope] - aliases from an outer query (for correlated subqueries)
 * @returns {QueryPlan}
 */
function planSelect({ select, ctePlans, cteColumns, tables, parentColumns, outerScope }) {
  // Reject window functions in clauses where they're not permitted.
  expectNoWindowFunction(select.where, 'WHERE')
  expectNoWindowFunction(select.having, 'HAVING')
  for (const expr of select.groupBy) expectNoWindowFunction(expr, 'GROUP BY')
  for (const term of select.orderBy) expectNoWindowFunction(term.expr, 'ORDER BY')
  for (const join of select.joins) expectNoWindowFunction(join.on, 'JOIN ON')

  // Collect window functions from SELECT columns and rewrite them to identifiers
  // pointing at the synthetic cells produced by the Window plan node.
  /** @type {WindowSpec[]} */
  const windows = []
  const windowColumns = select.columns.map(col => {
    if (col.type !== 'derived') return col
    const originalAlias = col.alias ?? derivedAlias(col.expr)
    const expr = collectWindows(col.expr, windows)
    if (expr === col.expr) return col
    return { ...col, expr, alias: originalAlias }
  })

  if (windows.length && select.columns.some(col => col.type === 'derived' && findAggregate(col.expr))) {
    throw new ParseError({
      message: 'Window functions are not supported in queries with aggregation',
      ...select,
    })
  }
  if (windows.length && select.groupBy.length) {
    throw new ParseError({
      message: 'Window functions are not supported in queries with aggregation',
      ...select,
    })
  }

  // Preserve the pre-substitution columns for column-extraction, so synthetic
  // `__window_N` identifiers are not requested from the data source.
  const originalSelect = select
  select = { ...select, columns: windowColumns }

  // Check for aggregation
  const hasAggregate = select.columns.some(col =>
    col.type === 'derived' && findAggregate(col.expr)
  )
  const useGrouping = hasAggregate || select.groupBy.length > 0
  // Windows with PARTITION BY or ORDER BY buffer; `OVER ()` streams.
  const bufferingWindows = windows.some(w => w.partitionBy.length > 0 || w.orderBy.length > 0)
  const needsBuffering = useGrouping || select.orderBy.length > 0 || bufferingWindows

  // Source alias for FROM clause
  const sourceAlias = fromAlias(select.from)

  // Resolve aliases (and validate qualified references)
  // Include outerScope aliases so correlated references pass validation
  const scopeTables = Object.fromEntries([sourceAlias, ...select.joins.map(j => j.alias ?? j.table), ...outerScope ?? []].map(a => [a, true]))
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
      const qualified = col.table + '.*'
      throw new TableNotFoundError({ table: col.table, qualified, tables: scopeTables, ...col })
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
  const perTableColumns = extractColumns({ select: originalSelect, parentColumns })
  hints.columns = perTableColumns.get(sourceAlias)
  // Empty columns array means no columns were referenced, but a FROM subquery
  // still needs its own columns (e.g. for DISTINCT). Treat empty as unrestricted.
  if (hints.columns?.length === 0 && select.from.type === 'subquery') {
    hints.columns = undefined
  }
  if (!select.joins.length) {
    hints.where = select.where
    if (!needsBuffering && !select.distinct) {
      hints.limit = select.limit
      hints.offset = select.offset
    }
  }

  // Start with the data source (FROM clause)
  /** @type {QueryPlan} */
  let plan = planFrom({ select, ctePlans, cteColumns, hints, tables, outerScope })

  // Add JOINs
  if (select.joins.length) {
    plan = planJoin({ left: plan, joins: select.joins, leftTable: sourceAlias, ctePlans, cteColumns, perTableColumns, tables, outerScope })
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

    // Window functions: insert before Sort so outer ORDER BY can reference
    // the window output aliases.
    if (windows.length) {
      plan = { type: 'Window', windows, child: plan }
    }

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
      // Normalize identifiers to match the child's cell-key layout, so the
      // Project executor can look up cells by exact name instead of relying
      // on the evaluator's suffix-search fallback.
      const sourceColumns = inferSelectSourceColumns({ select, cteColumns, tables })
      projectColumns = projectColumns.map(col =>
        col.type === 'derived'
          ? { ...col, expr: normalizeIdentifiers(col.expr, sourceColumns) }
          : col
      )
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
 * @param {string[]} [options.outerScope]
 * @returns {QueryPlan}
 */
function planFrom({ select, ctePlans, cteColumns, hints, tables, outerScope }) {
  if (select.from.type === 'table') {
    const ctePlan = ctePlans?.get(select.from.table.toLowerCase())
    if (ctePlan) {
      return ctePlan
    }
    validateScan({ ...select.from, hints, tables })
    return { type: 'Scan', table: select.from.table, hints }
  } else if (select.from.type === 'function') {
    for (const arg of select.from.args) {
      validateNoIdentifiers(arg, select.from.funcName)
    }
    return planTableFunction(select.from)
  } else {
    const subPlan = planStatement({
      stmt: select.from.query,
      ctePlans,
      cteColumns,
      tables,
      outerScope,
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
 * Builds a TableFunction plan node for a FromFunction AST.
 *
 * @param {import('../types.js').FromFunction} from
 * @returns {import('./types.d.ts').TableFunctionNode}
 */
function planTableFunction(from) {
  return {
    type: 'TableFunction',
    funcName: from.funcName,
    args: from.args,
    columnNames: tableFunctionColumnNames(from),
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
 * @param {string[]} [options.outerScope] - aliases from an outer query (for correlated subqueries)
 * @returns {QueryPlan}
 */
function planJoin({ left, joins, leftTable, ctePlans, cteColumns, perTableColumns, tables, outerScope }) {
  let plan = left
  let currentLeftTable = leftTable

  // Running scope for lateral UNNEST arg validation — excludes the current
  // join's own alias (self-reference) and later joins (forward reference).
  /** @type {Record<string, any>} */
  const lateralScope = { [leftTable]: true }
  for (const alias of outerScope ?? []) {
    lateralScope[alias] = true
  }

  for (const join of joins) {
    const rightTable = join.alias ?? join.table

    // LATERAL table function: right side references left-side columns.
    if (join.fromFunction) {
      const lateralOuterScope = Object.keys(lateralScope)
      for (const arg of join.fromFunction.args) {
        validateTableRefs(arg, lateralScope)
        validateLateralSubqueries({ expr: arg, ctePlans, cteColumns, tables, outerScope: lateralOuterScope })
      }
      plan = {
        type: 'NestedLoopJoin',
        joinType: join.joinType,
        leftAlias: currentLeftTable,
        rightAlias: rightTable,
        condition: join.on,
        left: plan,
        right: planTableFunction(join.fromFunction),
        lateral: true,
      }
      currentLeftTable = `${currentLeftTable}_${rightTable}`
      lateralScope[rightTable] = true
      continue
    }

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
    lateralScope[rightTable] = true
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
 * Rewrites identifiers so their `prefix`/`name` pair matches a cell key that
 * will actually exist in the child row. A join child yields cells keyed as
 * `alias.column`; a plain scan or CTE yields bare `column`. Applying this at
 * plan time lets the Project fast path use exact lookups and keeps the
 * evaluator from having to suffix-search row.columns at runtime.
 *
 * @param {ExprNode} node
 * @param {string[]} sourceColumns
 * @returns {ExprNode}
 */
function normalizeIdentifiers(node, sourceColumns) {
  if (!node) return node
  if (node.type === 'identifier') {
    const current = node.prefix ? `${node.prefix}.${node.name}` : node.name
    if (sourceColumns.includes(current)) return node
    if (node.prefix) {
      if (sourceColumns.includes(node.name)) return { ...node, prefix: undefined }
      return node
    }
    const suffix = '.' + node.name
    const matches = sourceColumns.filter(c => c.endsWith(suffix))
    if (matches.length === 1) {
      return { ...node, prefix: matches[0].slice(0, matches[0].length - suffix.length) }
    }
    return node
  }
  if (node.type === 'unary') {
    return { ...node, argument: normalizeIdentifiers(node.argument, sourceColumns) }
  }
  if (node.type === 'binary') {
    return { ...node, left: normalizeIdentifiers(node.left, sourceColumns), right: normalizeIdentifiers(node.right, sourceColumns) }
  }
  if (node.type === 'function') {
    return { ...node, args: node.args.map(arg => normalizeIdentifiers(arg, sourceColumns)) }
  }
  if (node.type === 'cast') {
    return { ...node, expr: normalizeIdentifiers(node.expr, sourceColumns) }
  }
  if (node.type === 'in valuelist') {
    return {
      ...node,
      expr: normalizeIdentifiers(node.expr, sourceColumns),
      values: node.values.map(v => normalizeIdentifiers(v, sourceColumns)),
    }
  }
  if (node.type === 'case') {
    return {
      ...node,
      caseExpr: normalizeIdentifiers(node.caseExpr, sourceColumns),
      whenClauses: node.whenClauses.map(w => ({
        ...w,
        condition: normalizeIdentifiers(w.condition, sourceColumns),
        result: normalizeIdentifiers(w.result, sourceColumns),
      })),
      elseResult: normalizeIdentifiers(node.elseResult, sourceColumns),
    }
  }
  // literal, interval, subquery, in, exists: leave unchanged (subquery bodies
  // have their own source layout; correlated references must stay as-is).
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
 * Validates subquery expressions inside a lateral UNNEST argument by planning
 * them against the lateral scope. Forward references (to joins that appear
 * after the UNNEST) are rejected at plan time rather than deferring to
 * execution.
 *
 * @param {object} options
 * @param {ExprNode} options.expr
 * @param {Map<string, QueryPlan>} [options.ctePlans]
 * @param {Map<string, string[]>} [options.cteColumns]
 * @param {Record<string, AsyncDataSource> | undefined} options.tables
 * @param {string[]} options.outerScope
 */
function validateLateralSubqueries({ expr, ctePlans, cteColumns, tables, outerScope }) {
  if (!expr) return
  if (expr.type === 'subquery' || expr.type === 'exists' || expr.type === 'not exists') {
    planStatement({ stmt: expr.subquery, ctePlans, cteColumns, tables, outerScope })
    return
  }
  if (expr.type === 'in') {
    validateLateralSubqueries({ expr: expr.expr, ctePlans, cteColumns, tables, outerScope })
    planStatement({ stmt: expr.subquery, ctePlans, cteColumns, tables, outerScope })
    return
  }
  if (expr.type === 'binary') {
    validateLateralSubqueries({ expr: expr.left, ctePlans, cteColumns, tables, outerScope })
    validateLateralSubqueries({ expr: expr.right, ctePlans, cteColumns, tables, outerScope })
  } else if (expr.type === 'unary') {
    validateLateralSubqueries({ expr: expr.argument, ctePlans, cteColumns, tables, outerScope })
  } else if (expr.type === 'function') {
    for (const arg of expr.args) {
      validateLateralSubqueries({ expr: arg, ctePlans, cteColumns, tables, outerScope })
    }
  } else if (expr.type === 'cast') {
    validateLateralSubqueries({ expr: expr.expr, ctePlans, cteColumns, tables, outerScope })
  } else if (expr.type === 'in valuelist') {
    validateLateralSubqueries({ expr: expr.expr, ctePlans, cteColumns, tables, outerScope })
    for (const val of expr.values) {
      validateLateralSubqueries({ expr: val, ctePlans, cteColumns, tables, outerScope })
    }
  } else if (expr.type === 'case') {
    validateLateralSubqueries({ expr: expr.caseExpr, ctePlans, cteColumns, tables, outerScope })
    for (const w of expr.whenClauses) {
      validateLateralSubqueries({ expr: w.condition, ctePlans, cteColumns, tables, outerScope })
      validateLateralSubqueries({ expr: w.result, ctePlans, cteColumns, tables, outerScope })
    }
    validateLateralSubqueries({ expr: expr.elseResult, ctePlans, cteColumns, tables, outerScope })
  }
}

/**
 * Walks an expression, replacing every window function subnode with an
 * identifier that points at a synthetic `__window_N` cell. The collected
 * WindowSpec entries drive the Window plan node. Returns the same node
 * reference when no window function is present, so untouched expressions
 * aren't shallow-cloned.
 *
 * @param {ExprNode} expr
 * @param {WindowSpec[]} windows
 * @returns {ExprNode}
 */
function collectWindows(expr, windows) {
  if (!expr || !findWindow(expr)) return expr
  if (expr.type === 'window') {
    const alias = `__window_${windows.length}`
    windows.push({
      alias,
      funcName: expr.funcName.toUpperCase(),
      args: expr.args,
      partitionBy: expr.partitionBy,
      orderBy: expr.orderBy,
    })
    return {
      type: 'identifier',
      name: alias,
      positionStart: expr.positionStart,
      positionEnd: expr.positionEnd,
    }
  }
  if (expr.type === 'unary') {
    return { ...expr, argument: collectWindows(expr.argument, windows) }
  }
  if (expr.type === 'binary') {
    return { ...expr, left: collectWindows(expr.left, windows), right: collectWindows(expr.right, windows) }
  }
  if (expr.type === 'function') {
    return { ...expr, args: expr.args.map(a => collectWindows(a, windows)) }
  }
  if (expr.type === 'cast') {
    return { ...expr, expr: collectWindows(expr.expr, windows) }
  }
  if (expr.type === 'in valuelist') {
    return {
      ...expr,
      expr: collectWindows(expr.expr, windows),
      values: expr.values.map(v => collectWindows(v, windows)),
    }
  }
  if (expr.type === 'case') {
    return {
      ...expr,
      caseExpr: expr.caseExpr && collectWindows(expr.caseExpr, windows),
      whenClauses: expr.whenClauses.map(w => ({
        ...w,
        condition: collectWindows(w.condition, windows),
        result: collectWindows(w.result, windows),
      })),
      elseResult: expr.elseResult && collectWindows(expr.elseResult, windows),
    }
  }
  return expr
}

/**
 * Throws if the expression tree contains a window function.
 *
 * @param {ExprNode | undefined} expr
 * @param {string} clause
 */
function expectNoWindowFunction(expr, clause) {
  const win = findWindow(expr)
  if (win) {
    throw new ParseError({
      message: `Window function ${win.funcName.toUpperCase()} is not allowed in ${clause} clause`,
      positionStart: win.positionStart,
      positionEnd: win.positionEnd,
    })
  }
}

/**
 * @param {ExprNode | undefined} expr
 * @returns {WindowFunctionNode | undefined}
 */
function findWindow(expr) {
  if (!expr) return undefined
  if (expr.type === 'window') return expr
  if (expr.type === 'binary') return findWindow(expr.left) || findWindow(expr.right)
  if (expr.type === 'unary') return findWindow(expr.argument)
  if (expr.type === 'function') {
    for (const arg of expr.args) {
      const found = findWindow(arg)
      if (found) return found
    }
    return undefined
  }
  if (expr.type === 'cast') return findWindow(expr.expr)
  if (expr.type === 'in valuelist') {
    const found = findWindow(expr.expr)
    if (found) return found
    for (const val of expr.values) {
      const f = findWindow(val)
      if (f) return f
    }
    return undefined
  }
  if (expr.type === 'case') {
    if (expr.caseExpr) {
      const f = findWindow(expr.caseExpr)
      if (f) return f
    }
    for (const w of expr.whenClauses) {
      const f = findWindow(w.condition) || findWindow(w.result)
      if (f) return f
    }
    if (expr.elseResult) return findWindow(expr.elseResult)
  }
  return undefined
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
