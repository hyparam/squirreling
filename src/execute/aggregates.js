import { derivedAlias } from '../expression/alias.js'
import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan } from './execute.js'
import { keyify } from './utils.js'

/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, DerivedColumn, ExecuteContext, SelectColumn, SqlPrimitive } from '../types.js'
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
  /** @type {Map<any, AsyncRow[]>} */
  const groups = new Map()

  for (const row of allRows) {
    const key = keyify(...await Promise.all(plan.groupBy.map(expr => evaluateExpr({ node: expr, row, context }))))
    let group = groups.get(key)
    if (!group) {
      group = []
      groups.set(key, group)
    }
    group.push(row)
  }

  // Yield one row per group
  for (const group of groups.values()) {
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
function tryColumnScanAggregate(plan, { tables, signal }) {
  // No HAVING support in fast path
  if (plan.having) return
  // Child must be a direct table scan
  if (plan.child.type !== 'Scan') return
  const scanNode = plan.child
  const { limit, offset, where } = scanNode.hints
  // scanColumn doesn't support filtering
  if (where) return

  const table = tables[scanNode.table]
  if (!table?.scanColumn) return

  // All columns must be simple aggregates on plain identifiers
  /** @type {ColumnAggSpec[]} */
  const specs = []
  for (const col of plan.columns) {
    if (col.type !== 'derived') return
    const spec = extractColumnAggSpec(col)
    if (!spec) return
    specs.push(spec)
  }

  return (async function* () {
    /** @type {string[]} */
    const columns = []
    /** @type {AsyncCells} */
    const cells = {}

    for (const spec of specs) {
      columns.push(spec.alias)
      cells[spec.alias] = () => scanColumnAggregate({ table, spec, limit, offset, signal })
    }

    yield { columns, cells }
  })()
}

/**
 * Extracts aggregate spec from a simple aggregate expression node.
 * Returns undefined if the expression is not a supported simple aggregate.
 *
 * @param {DerivedColumn} col
 * @returns {ColumnAggSpec | undefined}
 */
function extractColumnAggSpec({ expr, alias }) {
  if (expr.type !== 'function') return
  if (expr.filter) return // FILTER not supported in fast path
  const funcName = expr.funcName.toUpperCase()
  if (!['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(funcName)) return

  // Argument must be a plain column identifier
  const arg = expr.args[0]
  if (arg.type !== 'identifier') return
  return {
    funcName,
    column: derivedAlias(arg),
    alias: alias ?? derivedAlias(expr),
    distinct: expr.distinct,
  }
}

/**
 * Scans a single column and computes an aggregate value.
 *
 * @param {Object} options
 * @param {AsyncDataSource} options.table
 * @param {ColumnAggSpec} options.spec
 * @param {number} [options.limit]
 * @param {number} [options.offset]
 * @param {AbortSignal} [options.signal]
 * @returns {Promise<SqlPrimitive>}
 */
async function scanColumnAggregate({ table, spec, limit, offset, signal }) {
  const values = table.scanColumn({ column: spec.column, limit, offset, signal })

  if (spec.funcName === 'COUNT' && spec.distinct) {
    const seen = new Set()
    for await (const chunk of values) {
      if (signal?.aborted) return null
      for (let i = 0; i < chunk.length; i++) {
        const v = chunk[i]
        if (v == null) continue
        seen.add(keyify(v))
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
