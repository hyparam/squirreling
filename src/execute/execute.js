import { memorySource } from '../backend/dataSource.js'
import { tableNotFoundError } from '../executionErrors.js'
import { derivedAlias } from '../expression/alias.js'
import { evaluateExpr } from '../expression/evaluate.js'
import { parseSql } from '../parse/parse.js'
import { planSql } from '../plan/plan.js'
import { executeHashAggregate, executeScalarAggregate } from './aggregates.js'
import { executeHashJoin, executeNestedLoopJoin, executePositionalJoin } from './join.js'
import { executeSort } from './sort.js'
import { stableRowKey } from './utils.js'

/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, ExecuteContext, ExecuteSqlOptions, ExprNode, SelectStatement } from '../types.js'
 * @import { CountNode, DistinctNode, FilterNode, LimitNode, ProjectNode, QueryPlan, ScanNode } from '../plan/types.js'
 */

/**
 * Executes a SQL SELECT query against tables
 *
 * @param {ExecuteSqlOptions} options
 * @yields {AsyncRow}
 */
export async function* executeSql({ tables, query, functions, signal }) {
  const select = typeof query === 'string' ? parseSql({ query, functions }) : query

  // Normalize tables: convert arrays to AsyncDataSource
  /** @type {Record<string, AsyncDataSource>} */
  const normalizedTables = {}
  for (const [name, source] of Object.entries(tables)) {
    if (Array.isArray(source)) {
      normalizedTables[name] = memorySource(source)
    } else {
      normalizedTables[name] = source
    }
  }

  yield* executeSelect({ select, context: { tables: normalizedTables, functions, signal } })
}

/**
 * Executes a SELECT query against the provided tables
 *
 * @param {Object} options
 * @param {SelectStatement} options.select
 * @param {ExecuteContext} options.context
 * @yields {AsyncRow}
 */
export async function* executeSelect({ select, context }) {
  const plan = planSql({ query: select, functions: context.functions })
  yield* executePlan({ plan, context })
}

/**
 * Executes a query plan and yields result rows
 *
 * @param {Object} options
 * @param {QueryPlan} options.plan - the query plan to execute
 * @param {ExecuteContext} options.context - execution context
 * @returns {AsyncGenerator<AsyncRow>}
 */
export async function* executePlan({ plan, context }) {
  if (plan.type === 'Scan') {
    yield* executeScan(plan, context)
  } else if (plan.type === 'Count') {
    yield* executeCount(plan, context)
  } else if (plan.type === 'Filter') {
    yield* executeFilter(plan, context)
  } else if (plan.type === 'Project') {
    yield* executeProject(plan, context)
  } else if (plan.type === 'HashJoin') {
    yield* executeHashJoin(plan, context)
  } else if (plan.type === 'NestedLoopJoin') {
    yield* executeNestedLoopJoin(plan, context)
  } else if (plan.type === 'PositionalJoin') {
    yield* executePositionalJoin(plan, context)
  } else if (plan.type === 'HashAggregate') {
    yield* executeHashAggregate(plan, context)
  } else if (plan.type === 'ScalarAggregate') {
    yield* executeScalarAggregate(plan, context)
  } else if (plan.type === 'Sort') {
    yield* executeSort(plan, context)
  } else if (plan.type === 'Distinct') {
    yield* executeDistinct(plan, context)
  } else if (plan.type === 'Limit') {
    yield* executeLimit(plan, context)
  }
}

/**
 * Executes a table scan
 *
 * @param {ScanNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
async function* executeScan(plan, context) {
  const { tables, signal } = context
  const dataSource = tables[plan.table]
  if (dataSource === undefined) {
    throw tableNotFoundError({ tableName: plan.table })
  }

  const scanResult = dataSource.scan({ ...plan.hints, signal })
  if (!scanResult.rows) {
    throw new Error(`Data source "${plan.table}" scan() must return a ScanResults object with { rows, appliedWhere, appliedLimitOffset }`)
  }
  const { rows, appliedWhere, appliedLimitOffset } = scanResult

  // Applied limit/offset without applied where is invalid
  const hasLimitOffset = plan.hints.limit !== undefined || plan.hints.offset // 0 offset is noop
  if (!appliedWhere && appliedLimitOffset && plan.hints.where && hasLimitOffset) {
    throw new Error(`Data source "${plan.table}" applied limit/offset without applying where`)
  }

  let result = rows

  // Apply WHERE if data source did not
  if (!appliedWhere && plan.hints.where) {
    result = filterRows(result, plan.hints.where, context)
  }

  // Apply LIMIT/OFFSET if data source did not
  if (!appliedLimitOffset && hasLimitOffset) {
    result = limitRows(result, plan.hints.limit, plan.hints.offset, signal)
  }

  yield* result
}

/**
 * Executes a Count node using numRows when available, falling back to scan
 *
 * @param {CountNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
async function* executeCount(plan, { tables, signal }) {
  const dataSource = tables[plan.table]
  if (dataSource === undefined) {
    throw tableNotFoundError({ tableName: plan.table })
  }

  // Use source numRows if available
  let count = dataSource.numRows
  if (dataSource.numRows === undefined) {
    // Fall back to counting rows via scan
    count = 0
    const { rows } = dataSource.scan({ signal })
    // eslint-disable-next-line no-unused-vars
    for await (const _ of rows) {
      if (signal?.aborted) return
      count++
    }
  }

  /** @type {string[]} */
  const columns = []
  /** @type {AsyncCells} */
  const cells = {}
  for (const col of plan.columns) {
    const alias = col.alias ?? derivedAlias(col.expr)
    columns.push(alias)
    cells[alias] = () => Promise.resolve(count)
  }
  yield { columns, cells }
}

/**
 * Filters rows by a condition
 *
 * @param {AsyncIterable<AsyncRow>} rows
 * @param {ExprNode} condition
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
async function* filterRows(rows, condition, context) {
  let rowIndex = 0
  for await (const row of rows) {
    if (context.signal?.aborted) return
    rowIndex++
    const pass = await evaluateExpr({ node: condition, row, rowIndex, context })
    if (pass) yield row
  }
}

/**
 * Skips the first `offset` rows, then yields at most `limit` rows
 *
 * @param {AsyncIterable<AsyncRow>} rows
 * @param {number} [limit]
 * @param {number} [offset]
 * @param {AbortSignal} [signal]
 * @yields {AsyncRow}
 */
async function* limitRows(rows, limit, offset, signal) {
  const skip = offset ?? 0
  const max = limit ?? Infinity
  if (max <= 0) return
  let skipped = 0
  let yielded = 0
  for await (const row of rows) {
    if (signal?.aborted) return
    if (skipped < skip) {
      skipped++
      continue
    }
    yield row
    yielded++
    if (yielded >= max) return
  }
}

/**
 * Executes a filter operation (WHERE clause)
 *
 * @param {FilterNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
async function* executeFilter(plan, context) {
  yield* filterRows(executePlan({ plan: plan.child, context }), plan.condition, context)
}

/**
 * Executes a projection operation (SELECT columns)
 *
 * @param {ProjectNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
async function* executeProject(plan, context) {
  let rowIndex = 0

  for await (const row of executePlan({ plan: plan.child, context })) {
    if (context.signal?.aborted) return
    rowIndex++
    const currentRowIndex = rowIndex

    /** @type {string[]} */
    const columns = []
    /** @type {AsyncCells} */
    const cells = {}

    for (const col of plan.columns) {
      if (col.kind === 'star') {
        for (const key of row.columns) {
          columns.push(key)
          cells[key] = row.cells[key]
        }
      } else if (col.kind === 'derived') {
        const alias = col.alias ?? derivedAlias(col.expr)
        columns.push(alias)
        cells[alias] = () => evaluateExpr({
          node: col.expr,
          row,
          rowIndex: currentRowIndex,
          context,
        })
      }
    }

    yield { columns, cells }
  }
}

/**
 * Executes a distinct operation
 *
 * @param {DistinctNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
async function* executeDistinct(plan, context) {
  const { signal } = context

  /** @type {Set<string>} */
  const seen = new Set()

  for await (const row of executePlan({ plan: plan.child, context })) {
    if (signal?.aborted) return

    const key = await stableRowKey(row.cells)
    if (!seen.has(key)) {
      seen.add(key)
      yield row
    }
  }
}

/**
 * Executes a limit operation (LIMIT/OFFSET)
 *
 * @param {LimitNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
async function* executeLimit(plan, context) {
  yield* limitRows(executePlan({ plan: plan.child, context }), plan.limit, plan.offset, context.signal)
}
