import { derivedAlias } from '../expression/alias.js'
import { evaluateExpr } from '../expression/evaluate.js'
import { executePlan, selectColumnNames } from './execute.js'
import { sortEntriesByTerms } from './sort.js'
import { keyify } from './utils.js'

/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, ColumnBatch, DerivedColumn, ExecuteContext, QueryResults, SelectColumn, SqlPrimitive } from '../types.js'
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
        const prefix = col.table ? `${col.table}.` : undefined
        for (const key of firstRow.columns) {
          if (prefix && !key.startsWith(prefix)) continue
          const dotIndex = key.indexOf('.')
          const outputKey = prefix ? key.substring(prefix.length) : dotIndex >= 0 ? key.substring(dotIndex + 1) : key
          columns.push(outputKey)
          cells[outputKey] = firstRow.cells[key]
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
 * Builds the row visible to post-aggregation expressions such as HAVING and
 * grouped ORDER BY: source group columns plus aggregate output aliases.
 *
 * @param {AsyncRow[]} group
 * @param {AsyncRow} aggregateRow
 * @returns {AsyncRow}
 */
function aggregateContextRow(group, aggregateRow) {
  const baseRow = group[0] ?? { columns: [], cells: {} }
  return {
    columns: [...baseRow.columns, ...aggregateRow.columns],
    cells: { ...baseRow.cells, ...aggregateRow.cells },
  }
}

/**
 * Executes a hash aggregate operation (GROUP BY)
 *
 * @param {HashAggregateNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executeHashAggregate(plan, context) {
  const child = executePlan({ plan: plan.child, context })
  return {
    columns: selectColumnNames(plan.columns, child.columns),
    maxRows: child.maxRows,
    async *rows() {
      const op = context.budget?.operator('HashAggregate')
      // Collect all rows
      /** @type {AsyncRow[]} */
      const allRows = []
      for await (const row of child.rows()) {
        if (context.signal?.aborted) return
        op?.addRow()
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

      /** @type {{ row: AsyncRow, rows: AsyncRow[], outputRow: AsyncRow }[]} */
      const aggregateRows = []

      for (const group of groups.values()) {
        const asyncRow = projectAggregateColumns(plan.columns, group, context)
        const contextRow = aggregateContextRow(group, asyncRow)

        // Apply HAVING filter
        if (plan.having) {
          const passes = await evaluateExpr({
            node: plan.having,
            row: contextRow,
            rows: group,
            context,
          })
          if (!passes) continue
        }

        aggregateRows.push({ row: contextRow, rows: group, outputRow: asyncRow })
      }

      const outputRows = plan.orderBy?.length
        ? await sortEntriesByTerms({
          entries: aggregateRows,
          orderBy: plan.orderBy,
          context,
        })
        : aggregateRows

      for (const { outputRow } of outputRows) {
        yield outputRow
      }
    },
  }
}

/**
 * Executes a scalar aggregate operation (no GROUP BY, whole table aggregate)
 *
 * @param {ScalarAggregateNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executeScalarAggregate(plan, context) {
  // Fast path: use scanColumn when available
  const allowFast = context.budget ? context.budget.allowDerivedColumnScan : true
  const fast = allowFast ? tryColumnScanAggregate(plan, context) : undefined
  if (fast) {
    return {
      columns: selectColumnNames(plan.columns, []),
      numRows: 1,
      maxRows: 1,
      rows: fast,
    }
  }

  const child = executePlan({ plan: plan.child, context })

  // Batch-mode fast path: child emits column batches and every output column is
  // a simple aggregate (COUNT/SUM/AVG/MIN/MAX) on a plain identifier (or
  // COUNT(*)). Lets us consume columnar data directly without materializing
  // per-row AsyncRow objects.
  const batchFast = tryBatchAggregate(plan, child, context)
  if (batchFast) {
    return {
      columns: selectColumnNames(plan.columns, child.columns),
      numRows: 1,
      maxRows: 1,
      rows: batchFast,
    }
  }

  return {
    columns: selectColumnNames(plan.columns, child.columns),
    numRows: plan.having ? undefined : 1,
    maxRows: 1,
    async *rows() {
      const op = context.budget?.operator('ScalarAggregate')
      // Collect all rows into single group
      /** @type {AsyncRow[]} */
      const group = []
      for await (const row of child.rows()) {
        if (context.signal?.aborted) return
        op?.addRow()
        group.push(row)
      }

      const asyncRow = projectAggregateColumns(plan.columns, group, context)

      // Apply HAVING filter
      if (plan.having) {
        const baseRow = group[0] ?? { columns: [], cells: {} }
        /** @type {AsyncRow} */
        const havingRow = {
          columns: [...baseRow.columns, ...asyncRow.columns],
          cells: { ...baseRow.cells, ...asyncRow.cells },
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
    },
  }
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
 * @returns {(() => AsyncGenerator<AsyncRow>) | undefined}
 */
function tryColumnScanAggregate(plan, { tables, signal, budget }) {
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

  return async function* () {
    /** @type {string[]} */
    const columns = []
    /** @type {AsyncCells} */
    const cells = {}

    for (const spec of specs) {
      columns.push(spec.alias)
      cells[spec.alias] = () => scanColumnAggregate({ table, spec, limit, offset, signal, budget })
    }

    yield { columns, cells }
  }
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
 * @param {import('../types.js').BudgetTracker} [options.budget]
 * @returns {Promise<SqlPrimitive>}
 */
async function scanColumnAggregate({ table, spec, limit, offset, signal, budget }) {
  const values = table.scanColumn({ column: spec.column, limit, offset, signal })

  if (spec.funcName === 'COUNT' && spec.distinct) {
    const seen = new Set()
    for await (const chunk of values) {
      if (signal?.aborted) return
      budget?.checkTimeout()
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
      if (signal?.aborted) return
      budget?.checkTimeout()
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
    if (signal?.aborted) return
    budget?.checkTimeout()
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

/**
 * @typedef {{
 *   funcName: 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX',
 *   column: string | null,
 *   alias: string,
 *   resolvedColumn: string | null,
 * }} BatchAggSpec
 */

/**
 * Extracts a batch-aggregate spec from a SELECT column. Returns undefined for
 * shapes the batch fast path can't handle: non-functions, FILTER, DISTINCT,
 * unsupported function names, or arguments that aren't a plain identifier or
 * star. The fast path mirrors tryColumnScanAggregate but consumes columnar
 * batches instead of dispatching to scanColumn.
 *
 * @param {DerivedColumn} col
 * @returns {Omit<BatchAggSpec, 'resolvedColumn'> | undefined}
 */
function extractBatchAggSpec({ expr, alias }) {
  if (expr.type !== 'function') return
  if (expr.filter) return
  if (expr.distinct) return
  const funcName = expr.funcName.toUpperCase()
  if (funcName !== 'COUNT' && funcName !== 'SUM' && funcName !== 'AVG' && funcName !== 'MIN' && funcName !== 'MAX') return

  const arg = expr.args[0]
  if (arg.type === 'star') {
    if (funcName !== 'COUNT') return
    return { funcName, column: null, alias: alias ?? derivedAlias(expr) }
  }
  if (arg.type !== 'identifier') return
  return {
    funcName,
    column: derivedAlias(arg),
    alias: alias ?? derivedAlias(expr),
  }
}

/**
 * Resolves a spec's column name to a key in a batch's column map. Returns the
 * actual key or undefined if unresolvable. Mirrors the suffix-match fallback
 * used by projectBatchesSimple so prefixed child columns (e.g. 't.id') still
 * resolve when the spec carries the bare identifier name.
 *
 * @param {string} name
 * @param {string[]} childColumns
 * @returns {string | undefined}
 */
function resolveBatchColumn(name, childColumns) {
  if (childColumns.includes(name)) return name
  const suffix = '.' + name
  return childColumns.find(c => c.endsWith(suffix))
}

/**
 * Builds an async generator that consumes column batches from `child` and
 * emits a single aggregate row. Returns undefined when the fast path can't
 * apply — caller falls back to row-mode aggregation.
 *
 * @param {ScalarAggregateNode} plan
 * @param {QueryResults} child
 * @param {ExecuteContext} context
 * @returns {(() => AsyncGenerator<AsyncRow>) | undefined}
 */
function tryBatchAggregate(plan, child, context) {
  if (plan.having) return
  if (!child.batches) return

  /** @type {BatchAggSpec[]} */
  const specs = []
  for (const col of plan.columns) {
    if (col.type !== 'derived') return
    const spec = extractBatchAggSpec(col)
    if (!spec) return
    /** @type {string | null} */
    let resolvedColumn = null
    if (spec.column !== null) {
      const resolved = resolveBatchColumn(spec.column, child.columns)
      if (!resolved) return
      resolvedColumn = resolved
    }
    specs.push({ ...spec, resolvedColumn })
  }

  const childBatches = child.batches.bind(child)

  return async function* () {
    /** @type {Record<string, { count: number, sum: number, min: SqlPrimitive, max: SqlPrimitive }>} */
    const state = {}
    for (const spec of specs) {
      state[spec.alias] = { count: 0, sum: 0, min: null, max: null }
    }

    for await (const batch of childBatches()) {
      if (context.signal?.aborted) return
      for (const spec of specs) {
        accumulateBatch(state[spec.alias], spec, batch)
      }
    }

    /** @type {string[]} */
    const columns = []
    /** @type {AsyncCells} */
    const cells = {}
    for (const spec of specs) {
      columns.push(spec.alias)
      const value = finalizeAggregate(spec, state[spec.alias])
      cells[spec.alias] = () => Promise.resolve(value)
    }
    yield { columns, cells }
  }
}

/**
 * Accumulates one batch into the running state for a single aggregate spec.
 * Null-handling mirrors scanColumnAggregate: nulls are skipped for every
 * function; SUM/AVG additionally skip values that don't coerce to a finite
 * number; MIN/MAX work on any non-null value (including strings).
 *
 * @param {{ count: number, sum: number, min: SqlPrimitive, max: SqlPrimitive }} s
 * @param {BatchAggSpec} spec
 * @param {ColumnBatch} batch
 */
function accumulateBatch(s, spec, batch) {
  if (spec.funcName === 'COUNT' && spec.resolvedColumn === null) {
    s.count += batch.rowCount
    return
  }
  const { resolvedColumn } = spec
  if (resolvedColumn === null) return
  const col = batch.columns[resolvedColumn]
  if (!col) return
  const len = batch.rowCount
  if (spec.funcName === 'COUNT') {
    for (let i = 0; i < len; i++) {
      if (col[i] != null) s.count++
    }
    return
  }
  if (spec.funcName === 'SUM' || spec.funcName === 'AVG') {
    let { sum, count } = s
    for (let i = 0; i < len; i++) {
      const v = col[i]
      if (v == null) continue
      const num = Number(v)
      if (!Number.isFinite(num)) continue
      sum += num
      count++
    }
    s.sum = sum
    s.count = count
    return
  }
  if (spec.funcName === 'MIN') {
    let { min } = s
    for (let i = 0; i < len; i++) {
      const v = col[i]
      if (v == null) continue
      if (min === null || v < min) min = v
    }
    s.min = min
    return
  }
  if (spec.funcName === 'MAX') {
    let { max } = s
    for (let i = 0; i < len; i++) {
      const v = col[i]
      if (v == null) continue
      if (max === null || v > max) max = v
    }
    s.max = max
  }
}

/**
 * Computes the final aggregate value from running state.
 *
 * @param {BatchAggSpec} spec
 * @param {{ count: number, sum: number, min: SqlPrimitive, max: SqlPrimitive }} s
 * @returns {SqlPrimitive}
 */
function finalizeAggregate(spec, s) {
  if (spec.funcName === 'COUNT') return s.count
  if (spec.funcName === 'SUM') return s.count === 0 ? null : s.sum
  if (spec.funcName === 'AVG') return s.count === 0 ? null : s.sum / s.count
  if (spec.funcName === 'MIN') return s.min
  return s.max
}
