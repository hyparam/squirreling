import { derivedAlias } from '../expression/alias.js'
import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { stringify } from './utils.js'

/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, ExecuteContext, ExprNode, SelectColumn, SqlPrimitive } from '../types.js'
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
    if (col.type === 'star') {
      const firstRow = group[0]
      if (firstRow) {
        for (const key of firstRow.columns) {
          columns.push(key)
          cells[key] = firstRow.cells[key]
        }
      }
    } else {
      const alias = col.alias ?? derivedAlias(col.expr)
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
      /** @type {AsyncRow} */
      const havingRow = {
        columns: [...group[0].columns, ...asyncRow.columns],
        cells: { ...group[0].cells, ...asyncRow.cells },
      }
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
  // Fast path: use scanColumn when available
  const fast = tryColumnScanAggregate(plan, context)
  if (fast) {
    yield* fast
    return
  }

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
    /** @type {AsyncRow} */
    const havingRow = {
      columns: [...group[0].columns, ...asyncRow.columns],
      cells: { ...group[0].cells, ...asyncRow.cells },
    }
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

/**
 * @typedef {{
 *   funcName: string,
 *   column: string,
 *   alias: string,
 *   distinct?: boolean,
 * }} ColumnAggSpec
 */

/**
 * Checks if a scalar aggregate can use the scanColumn fast path.
 * Returns an async generator if so, undefined otherwise.
 *
 * @param {ScalarAggregateNode} plan
 * @param {ExecuteContext} context
 * @returns {AsyncGenerator<AsyncRow> | undefined}
 */
function tryColumnScanAggregate(plan, context) {
  // No HAVING support in fast path
  if (plan.having) return
  // Child must be a direct table scan
  if (plan.child.type !== 'Scan') return
  const scanNode = plan.child
  // No WHERE in scan (scanColumn doesn't support filtering)
  if (scanNode.hints.where) return

  const table = context.tables[scanNode.table]
  if (!table?.scanColumn) return

  // All columns must be simple aggregates on plain identifiers
  /** @type {ColumnAggSpec[]} */
  const specs = []
  for (const col of plan.columns) {
    if (col.type !== 'derived') return
    const spec = extractColumnAggSpec(col.expr, col.alias)
    if (!spec) return
    specs.push(spec)
  }

  return (async function* () {
    /** @type {string[]} */
    const columns = []
    /** @type {AsyncCells} */
    const cells = {}

    for (const spec of specs) {
      const value = await scanColumnAggregate(table, spec, context.signal)
      columns.push(spec.alias)
      cells[spec.alias] = () => Promise.resolve(value)
    }

    yield { columns, cells }
  })()
}

/**
 * Extracts aggregate spec from a simple aggregate expression node.
 * Returns undefined if the expression is not a supported simple aggregate.
 *
 * @param {ExprNode} node
 * @param {string} [alias]
 * @returns {ColumnAggSpec | undefined}
 */
function extractColumnAggSpec(node, alias) {
  if (node.type !== 'function') return
  const funcName = node.funcName.toUpperCase()
  const supported = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
  if (!supported.includes(funcName)) return
  // No FILTER clause
  if (node.filter) return
  const arg = node.args[0]
  // Argument must be a plain column identifier
  if (arg.type !== 'identifier') return
  return {
    funcName,
    column: derivedAlias(arg),
    alias: alias ?? derivedAlias(node),
    distinct: node.distinct,
  }
}

/**
 * Scans a single column and computes an aggregate value.
 *
 * @param {AsyncDataSource} table
 * @param {ColumnAggSpec} spec
 * @param {AbortSignal} [signal]
 * @returns {Promise<SqlPrimitive>}
 */
async function scanColumnAggregate(table, spec, signal) {
  const values = table.scanColumn({ column: spec.column, signal })

  if (spec.funcName === 'COUNT' && spec.distinct) {
    /** @type {Set<string>} */
    const seen = new Set()
    for await (const chunk of values) {
      if (signal?.aborted) return null
      for (let i = 0; i < chunk.length; i++) {
        if (chunk[i] != null) seen.add(stringify(chunk[i]))
      }
    }
    return seen.size
  }

  if (spec.funcName === 'COUNT') {
    let count = 0
    for await (const chunk of values) {
      if (signal?.aborted) return null
      for (let i = 0; i < chunk.length; i++) {
        if (chunk[i] != null) count++
      }
    }
    return count
  }

  // SUM, AVG, MIN, MAX
  let sum = 0
  let count = 0
  /** @type {SqlPrimitive} */
  let min = null
  /** @type {SqlPrimitive} */
  let max = null

  for await (const chunk of values) {
    if (signal?.aborted) return null
    for (let i = 0; i < chunk.length; i++) {
      const v = chunk[i]
      if (v == null) continue
      if (min === null || v < min) min = v
      if (max === null || v > max) max = v
      const num = Number(v)
      if (!Number.isFinite(num)) continue
      sum += num
      count++
    }
  }

  if (spec.funcName === 'SUM') return count === 0 ? null : sum
  if (spec.funcName === 'AVG') return count === 0 ? null : sum / count
  if (spec.funcName === 'MIN') return min
  if (spec.funcName === 'MAX') return max
  return null
}
