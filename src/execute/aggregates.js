import { evaluateExpr } from '../expression/evaluate.js'
import { defaultDerivedAlias, stringify } from './utils.js'
import { executePlan } from './execute.js'

/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, SelectColumn, UserDefinedFunction } from '../types.js'
 * @import { ExecuteContext, HashAggregateNode, ScalarAggregateNode } from '../plan/types.js'
 */

/**
 * Projects aggregate columns from a group of rows
 *
 * @param {SelectColumn[]} selectColumns
 * @param {AsyncRow[]} group
 * @param {Record<string, AsyncDataSource>} tables
 * @param {Record<string, UserDefinedFunction>} [functions]
 * @param {AbortSignal} [signal]
 * @returns {AsyncRow}
 */
function projectAggregateColumns(selectColumns, group, tables, functions, signal) {
  /** @type {string[]} */
  const columns = []
  /** @type {AsyncCells} */
  const cells = {}

  for (const col of selectColumns) {
    if (col.kind === 'star') {
      const firstRow = group[0]
      if (firstRow) {
        for (const key of firstRow.columns) {
          columns.push(key)
          cells[key] = firstRow.cells[key]
        }
      }
    } else if (col.kind === 'derived') {
      const alias = col.alias ?? defaultDerivedAlias(col.expr)
      columns.push(alias)
      cells[alias] = () => evaluateExpr({
        node: col.expr,
        row: group[0] ?? { columns: [], cells: {} },
        tables,
        functions,
        rows: group,
        signal,
      })
    }
  }

  return { columns, cells }
}

/**
 * Executes a hash aggregate operation (GROUP BY)
 *
 * @param {HashAggregateNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
export async function* executeHashAggregate(plan, context) {
  const { tables, functions, signal } = context

  // Collect all rows
  /** @type {AsyncRow[]} */
  const allRows = []
  for await (const row of executePlan(plan.child, context)) {
    if (signal?.aborted) return
    allRows.push(row)
  }

  // Group rows by GROUP BY keys
  /** @type {Map<string, AsyncRow[]>} */
  const groupMap = new Map()
  /** @type {AsyncRow[][]} */
  const groups = []

  for (const row of allRows) {
    /** @type {string[]} */
    const keyParts = []
    for (const expr of plan.groupBy) {
      const v = await evaluateExpr({ node: expr, row, tables, functions, signal })
      keyParts.push(stringify(v))
    }
    const key = keyParts.join('|')
    let group = groupMap.get(key)
    if (!group) {
      group = []
      groupMap.set(key, group)
      groups.push(group)
    }
    group.push(row)
  }

  // Yield one row per group
  for (const group of groups) {
    const asyncRow = projectAggregateColumns(plan.columns, group, tables, functions, signal)

    // Apply HAVING filter
    if (plan.having) {
      const context = { ...group[0], ...asyncRow }
      const passes = await evaluateExpr({
        node: plan.having,
        row: context,
        rows: group,
        tables,
        functions,
        signal,
      })
      if (!passes) continue
    }

    yield asyncRow
  }
}

/**
 * Executes a scalar aggregate operation (no GROUP BY, whole table aggregate)
 *
 * @param {ScalarAggregateNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
export async function* executeScalarAggregate(plan, context) {
  const { tables, functions, signal } = context

  // Collect all rows into single group
  /** @type {AsyncRow[]} */
  const group = []
  for await (const row of executePlan(plan.child, context)) {
    if (signal?.aborted) return
    group.push(row)
  }

  const asyncRow = projectAggregateColumns(plan.columns, group, tables, functions, signal)

  // Apply HAVING filter
  if (plan.having) {
    const context = { ...group[0], ...asyncRow }
    const passes = await evaluateExpr({
      node: plan.having,
      row: context,
      rows: group,
      tables,
      functions,
      signal,
    })
    if (!passes) return
  }

  yield asyncRow
}
