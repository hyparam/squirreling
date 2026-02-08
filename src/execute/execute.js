import { memorySource } from '../backend/dataSource.js'
import { tableNotFoundError } from '../executionErrors.js'
import { evaluateExpr } from '../expression/evaluate.js'
import { parseSql } from '../parse/parse.js'
import { missingClauseError } from '../parseErrors.js'
import { queryPlan } from '../plan/plan.js'
import { executeHashAggregate, executeScalarAggregate } from './aggregates.js'
import { executeHashJoin, executeNestedLoopJoin, executePositionalJoin } from './join.js'
import { executeSort } from './sort.js'
import { defaultDerivedAlias, stableRowKey } from './utils.js'

/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, ExecuteSqlOptions, SelectStatement, UserDefinedFunction } from '../types.js'
 * @import { DistinctNode, ExecuteContext, FilterNode, LimitNode, ProjectNode, QueryPlan, ScanNode } from '../plan/types.js'
 */

/**
 * Executes a SQL SELECT query against named data sources
 *
 * @param {ExecuteSqlOptions} options - the execution options
 * @yields {AsyncRow} async generator yielding result rows
 */
export async function* executeSql({ tables, query, functions, signal }) {
  const select = typeof query === 'string' ? parseSql({ query, functions }) : query

  // Check for unsupported operations
  if (!select.from) {
    throw missingClauseError({
      missing: 'FROM clause',
      context: 'SELECT statement',
    })
  }

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

  yield* executeSelect({ select, tables: normalizedTables, functions, signal })
}

/**
 * Executes a SELECT query against the provided tables
 *
 * @param {Object} options
 * @param {SelectStatement} options.select
 * @param {Record<string, AsyncDataSource>} options.tables
 * @param {Record<string, UserDefinedFunction>} [options.functions]
 * @param {AbortSignal} [options.signal]
 * @yields {AsyncRow}
 */
export async function* executeSelect({ select, tables, functions, signal }) {
  const plan = queryPlan(select)
  yield* executePlan(plan, { tables, functions, signal })
}

/**
 * Executes a query plan and yields result rows
 *
 * @param {QueryPlan} plan - the query plan to execute
 * @param {ExecuteContext} context - execution context
 * @returns {AsyncGenerator<AsyncRow>}
 */
export async function* executePlan(plan, context) {
  if (plan.type === 'Scan') {
    yield* executeScan(plan, context)
  } else if (plan.type === 'SubqueryScan') {
    yield* executePlan(plan.subquery, context)
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

  yield* dataSource.scan({ ...plan.hints, signal })
}

/**
 * Executes a filter operation (WHERE clause)
 *
 * @param {FilterNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
async function* executeFilter(plan, context) {
  const { tables, functions, signal } = context
  let rowIndex = 0

  for await (const row of executePlan(plan.child, context)) {
    if (signal?.aborted) return
    rowIndex++
    const pass = await evaluateExpr({
      node: plan.condition,
      row,
      tables,
      functions,
      rowIndex,
      signal,
    })
    if (pass) {
      yield row
    }
  }
}

/**
 * Executes a projection operation (SELECT columns)
 *
 * @param {ProjectNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
async function* executeProject(plan, context) {
  const { tables, functions, signal } = context
  let rowIndex = 0

  for await (const row of executePlan(plan.child, context)) {
    if (signal?.aborted) return
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
        const alias = col.alias ?? defaultDerivedAlias(col.expr)
        columns.push(alias)
        cells[alias] = () => evaluateExpr({
          node: col.expr,
          row,
          tables,
          functions,
          rowIndex: currentRowIndex,
          signal,
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

  for await (const row of executePlan(plan.child, context)) {
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
  const { signal } = context

  const offset = plan.offset ?? 0
  const limit = plan.limit ?? Infinity
  if (limit <= 0) return

  let rowsSkipped = 0
  let rowsYielded = 0

  for await (const row of executePlan(plan.child, context)) {
    if (signal?.aborted) return

    if (rowsSkipped < offset) {
      rowsSkipped++
      continue
    }

    yield row
    rowsYielded++

    if (rowsYielded >= limit) {
      break
    }
  }
}
