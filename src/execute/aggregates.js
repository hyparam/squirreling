import { derivedAlias } from '../expression/alias.js'
import { evaluateExpr } from '../expression/evaluate.js'
import { finalizeAccumulator, newAccumulator, updateAccumulator } from './accumulator.js'
import { executePlan, selectColumnNames } from './execute.js'
import { sortEntriesByTerms } from './sort.js'
import { planStreamingAggregates, streamingHashAggregateRows, streamingScalarAggregateRows } from './streamingAggregate.js'
import { keyify } from './utils.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, DerivedColumn, ExecuteContext, QueryResults, SelectColumn, SqlPrimitive } from '../types.js'
 * @import { HashAggregateNode, ScalarAggregateNode } from '../plan/types.js'
 */

// Yield to the event loop every this many iterations so that aborts can actually fire
const YIELD_INTERVAL = 4000

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
  const streaming = planStreamingAggregates(plan, child.columns)
  if (streaming) {
    return {
      columns: selectColumnNames(plan.columns, child.columns),
      maxRows: child.maxRows,
      rows: streamingHashAggregateRows({ plan, streaming, child, context }),
    }
  }
  return {
    columns: selectColumnNames(plan.columns, child.columns),
    maxRows: child.maxRows,
    async *rows() {
      // Collect all rows
      /** @type {AsyncRow[]} */
      const allRows = []
      let collectCount = 0
      for await (const row of child.rows()) {
        if (++collectCount % YIELD_INTERVAL === 0) {
          await yieldToEventLoop()
          context.signal?.throwIfAborted()
        }
        allRows.push(row)
      }
      context.signal?.throwIfAborted()

      // Group rows by GROUP BY keys.
      // Each chunk dispatches all per-row key evaluations in parallel so
      // async cells (e.g. lazy parquet decode) overlap; the await is at the
      // chunk boundary. Synchronous cells stay cheap because we skip the
      // inner Promise.all wrapper when there's a single GROUP BY expression.
      /** @type {Map<any, AsyncRow[]>} */
      const groups = new Map()
      const { groupBy } = plan
      const singleKey = groupBy.length === 1
      const singleExpr = singleKey ? groupBy[0] : null

      for (let chunkStart = 0; chunkStart < allRows.length; chunkStart += YIELD_INTERVAL) {
        if (chunkStart > 0) {
          await yieldToEventLoop()
          context.signal?.throwIfAborted()
        }
        const chunkEnd = Math.min(chunkStart + YIELD_INTERVAL, allRows.length)
        const chunkLen = chunkEnd - chunkStart
        /** @type {Promise<any>[]} */
        const pending = new Array(chunkLen)
        if (singleKey) {
          for (let j = 0; j < chunkLen; j++) {
            pending[j] = evaluateExpr({ node: singleExpr, row: allRows[chunkStart + j], context })
          }
        } else {
          for (let j = 0; j < chunkLen; j++) {
            const row = allRows[chunkStart + j]
            pending[j] = Promise.all(groupBy.map(expr => evaluateExpr({ node: expr, row, context })))
          }
        }
        const chunkKeys = await Promise.all(pending)
        for (let j = 0; j < chunkLen; j++) {
          const key = singleKey ? keyify(chunkKeys[j]) : keyify(...chunkKeys[j])
          const row = allRows[chunkStart + j]
          let group = groups.get(key)
          if (!group) {
            group = []
            groups.set(key, group)
          }
          group.push(row)
        }
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
  const fast = tryColumnScanAggregate(plan, context)
  if (fast) {
    return {
      columns: selectColumnNames(plan.columns, []),
      numRows: 1,
      maxRows: 1,
      rows: fast,
    }
  }

  const child = executePlan({ plan: plan.child, context })
  const streaming = planStreamingAggregates(plan, child.columns)
  if (streaming) {
    return {
      columns: selectColumnNames(plan.columns, child.columns),
      numRows: plan.having ? undefined : 1,
      maxRows: 1,
      rows: streamingScalarAggregateRows({ plan, streaming, child, context }),
    }
  }
  return {
    columns: selectColumnNames(plan.columns, child.columns),
    numRows: plan.having ? undefined : 1,
    maxRows: 1,
    async *rows() {
      // Collect all rows into single group
      /** @type {AsyncRow[]} */
      const group = []
      let collectCount = 0
      for await (const row of child.rows()) {
        if (++collectCount % YIELD_INTERVAL === 0) {
          await yieldToEventLoop()
          context.signal?.throwIfAborted()
        }
        group.push(row)
      }
      context.signal?.throwIfAborted()

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

  return async function* () {
    /** @type {string[]} */
    const columns = []
    /** @type {AsyncCells} */
    const cells = {}

    // Group specs by column so each column is scanned at most once no matter how
    // many aggregates read it (e.g. MIN(x), MAX(x), AVG(x) share one pass).
    /** @type {Map<string, ColumnAggSpec[]>} */
    const specsByColumn = new Map()
    for (const spec of specs) {
      const group = specsByColumn.get(spec.column)
      if (group) group.push(spec)
      else specsByColumn.set(spec.column, [spec])
    }

    // Each column's single pass is computed once and shared by all its cells;
    // a column is only scanned if one of its aggregates is actually read.
    /** @type {Map<string, Promise<Map<string, SqlPrimitive>>>} */
    const passes = new Map()
    /**
     * @param {string} column
     * @returns {Promise<Map<string, SqlPrimitive>>}
     */
    function scanOnce(column) {
      let pass = passes.get(column)
      if (!pass) {
        pass = scanColumnGroup({ table, specs: specsByColumn.get(column) ?? [], limit, offset, signal })
        passes.set(column, pass)
      }
      return pass
    }

    for (const spec of specs) {
      columns.push(spec.alias)
      cells[spec.alias] = async () => (await scanOnce(spec.column)).get(spec.alias) ?? null
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
 * Scans a column once and computes every aggregate over it in a single pass.
 * All specs share the one scanColumn walk, so MIN(x)/MAX(x)/AVG(x) decode x once.
 *
 * @param {Object} options
 * @param {AsyncDataSource} options.table
 * @param {ColumnAggSpec[]} options.specs - aggregates over the same column
 * @param {number} [options.limit]
 * @param {number} [options.offset]
 * @param {AbortSignal} [options.signal]
 * @returns {Promise<Map<string, SqlPrimitive>>} alias → aggregate value
 */
async function scanColumnGroup({ table, specs, limit, offset, signal }) {
  const { column } = specs[0]
  const values = table.scanColumn({ column, limit, offset, signal })

  const accs = specs.map(spec => ({ spec, acc: newAccumulator(spec.funcName, spec.distinct) }))

  for await (const chunk of values) {
    signal?.throwIfAborted()
    for (let i = 0; i < chunk.length; i++) {
      const v = chunk[i]
      if (v == null) continue
      for (const { spec, acc } of accs) {
        updateAccumulator(spec.funcName, acc, v)
      }
    }
  }
  signal?.throwIfAborted()

  /** @type {Map<string, SqlPrimitive>} */
  const result = new Map()
  for (const { spec, acc } of accs) {
    result.set(spec.alias, finalizeAccumulator(spec.funcName, acc))
  }
  return result
}
