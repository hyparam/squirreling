import { evaluateExpr } from '../expression/evaluate.js'
import { defaultDerivedAlias, stringify } from './utils.js'
import { executePlan } from './execute.js'

/**
 * @import { AsyncCells, AsyncRow, ExecuteContext, SelectColumn } from '../types.js'
 * @import { HashAggregateNode, ScalarAggregateNode } from '../plan/types.js'
 */

/**
 * Projects aggregate columns from a group of rows
 *
 * @param {SelectColumn[]} selectColumns
 * @param {AsyncRow[]} group
 * @param {ExecuteContext} context
 * @returns {AsyncRow}
 */
function projectAggregateColumns(selectColumns, group, context) {
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
        rows: group,
        context,
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
  // Collect all rows
  /** @type {AsyncRow[]} */
  const allRows = []
  for await (const row of executePlan({ plan: plan.child, context })) {
    if (context.signal?.aborted) return
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
      const v = await evaluateExpr({ node: expr, row, context })
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
    const asyncRow = projectAggregateColumns(plan.columns, group, context)

    // Apply HAVING filter
    if (plan.having) {
      const havingRow = { ...group[0], ...asyncRow }
      const passes = await evaluateExpr({
        node: plan.having,
        row: havingRow,
        rows: group,
        context,
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
  // Collect all rows into single group
  /** @type {AsyncRow[]} */
  const group = []
  for await (const row of executePlan({ plan: plan.child, context })) {
    if (context.signal?.aborted) return
    group.push(row)
  }

  const asyncRow = projectAggregateColumns(plan.columns, group, context)

  // Apply HAVING filter
  if (plan.having) {
    const havingRow = { ...group[0], ...asyncRow }
    const passes = await evaluateExpr({
      node: plan.having,
      row: havingRow,
      rows: group,
      context,
    })
    if (!passes) return
  }

  yield asyncRow
}
