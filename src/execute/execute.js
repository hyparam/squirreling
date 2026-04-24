import { memorySource } from '../backend/dataSource.js'
import { derivedAlias } from '../expression/alias.js'
import { evaluateExpr } from '../expression/evaluate.js'
import { parseSql } from '../parse/parse.js'
import { planSql, planStatement } from '../plan/plan.js'
import { fromAlias } from '../plan/columns.js'
import { validateScan, validateTable } from '../validation/tables.js'
import { executeHashAggregate, executeScalarAggregate } from './aggregates.js'
import { executeHashJoin, executeNestedLoopJoin, executePositionalJoin } from './join.js'
import { executeSort } from './sort.js'
import { addBounds, minBounds, stableRowKey } from './utils.js'

/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, DerivedColumn, ExecuteContext, ExecuteSqlOptions, ExprNode, IdentifierNode, QueryResults, SelectColumn, SqlPrimitive, Statement } from '../types.js'
 * @import { CountNode, DistinctNode, FilterNode, LimitNode, ProjectNode, QueryPlan, ScanNode, SetOperationNode, TableFunctionNode } from '../plan/types.js'
 */

/**
 * Executes a SQL SELECT query against tables
 *
 * @param {ExecuteSqlOptions} options
 * @returns {QueryResults}
 */
export function executeSql({ tables, query, functions, signal }) {
  const parsed = typeof query === 'string' ? parseSql({ query, functions }) : query

  // Normalize tables: convert arrays to AsyncDataSource
  /** @type {Record<string, AsyncDataSource>} */
  const normalizedTables = {}
  for (const [name, data] of Object.entries(tables)) {
    if (Array.isArray(data)) {
      normalizedTables[name] = memorySource({ data })
    } else {
      normalizedTables[name] = data
    }
  }

  const scope = statementScope(parsed)
  // CTEs are resolved at plan time for FROM/JOIN positions. Subqueries inside
  // expressions are re-planned during execution, so capture the CTE maps here
  // and thread them through the context so those re-plans can still resolve
  // CTE references.
  /** @type {Map<string, QueryPlan>} */
  const ctePlans = new Map()
  /** @type {Map<string, string[]>} */
  const cteColumns = new Map()
  const context = { tables: normalizedTables, functions, signal, scope, ctePlans, cteColumns }
  const plan = planSql({ query: parsed, functions, tables: normalizedTables, ctePlans, cteColumns })
  return executePlan({ plan, context })
}

/**
 * Executes a statement against the provided tables
 *
 * @param {Object} options
 * @param {Statement} options.query
 * @param {ExecuteContext} options.context
 * @param {string[]} [options.outerScope] - outer query aliases for correlated subqueries
 * @returns {QueryResults}
 */
export function executeStatement({ query, context, outerScope }) {
  const plan = planStatement({
    stmt: query,
    tables: context.tables,
    ctePlans: context.ctePlans,
    cteColumns: context.cteColumns,
    outerScope,
  })
  // Compute this query's scope (FROM alias + JOIN aliases) for nested correlated subqueries
  const scope = statementScope(query)
  return executePlan({ plan, context: scope ? { ...context, scope } : context })
}

/**
 * Extracts the table aliases from a statement's FROM and JOIN clauses.
 *
 * @param {Statement} stmt
 * @returns {string[] | undefined}
 */
function statementScope(stmt) {
  if (stmt.type === 'with') return statementScope(stmt.query)
  if (stmt.type === 'compound') return undefined
  return [fromAlias(stmt.from), ...stmt.joins.map(j => j.alias ?? j.table)]
}

/**
 * Executes a query plan and returns query results with row count estimates
 *
 * @param {Object} options
 * @param {QueryPlan} options.plan - the query plan to execute
 * @param {ExecuteContext} options.context - execution context
 * @returns {QueryResults}
 */
export function executePlan({ plan, context }) {
  if (plan.type === 'Scan') {
    return executeScan(plan, context)
  } else if (plan.type === 'Count') {
    return executeCount(plan, context)
  } else if (plan.type === 'Filter') {
    return executeFilter(plan, context)
  } else if (plan.type === 'Project') {
    return executeProject(plan, context)
  } else if (plan.type === 'HashJoin') {
    return executeHashJoin(plan, context)
  } else if (plan.type === 'NestedLoopJoin') {
    return executeNestedLoopJoin(plan, context)
  } else if (plan.type === 'PositionalJoin') {
    return executePositionalJoin(plan, context)
  } else if (plan.type === 'HashAggregate') {
    return executeHashAggregate(plan, context)
  } else if (plan.type === 'ScalarAggregate') {
    return executeScalarAggregate(plan, context)
  } else if (plan.type === 'Sort') {
    return executeSort(plan, context)
  } else if (plan.type === 'Distinct') {
    return executeDistinct(plan, context)
  } else if (plan.type === 'Limit') {
    return executeLimit(plan, context)
  } else if (plan.type === 'SetOperation') {
    return executeSetOperation(plan, context)
  } else if (plan.type === 'TableFunction') {
    return executeTableFunction(plan, context)
  }
  return { columns: [], async *rows() {} }
}

/**
 * Executes a table-valued function (e.g. UNNEST).
 * Evaluates the argument once against an empty row and yields one row per
 * element of the resulting array. Null or non-array input yields zero rows.
 *
 * @param {TableFunctionNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeTableFunction(plan, context) {
  if (plan.funcName !== 'UNNEST') {
    throw new Error(`Unsupported table function: ${plan.funcName}`)
  }
  const columns = [plan.columnName]
  return {
    columns,
    async *rows() {
      /** @type {AsyncRow} */
      const row = context.outerRow ?? { columns: [], cells: {} }
      const value = await evaluateExpr({ node: plan.args[0], row, rowIndex: 1, context })
      if (!Array.isArray(value)) return
      for (const element of value) {
        if (context.signal?.aborted) return
        yield {
          columns,
          cells: { [plan.columnName]: () => Promise.resolve(element) },
        }
      }
    },
  }
}

/**
 * Derives output column names from SELECT columns and available child columns.
 *
 * @param {SelectColumn[]} selectColumns
 * @param {string[]} childColumns
 * @returns {string[]}
 */
export function selectColumnNames(selectColumns, childColumns) {
  /** @type {string[]} */
  const result = []
  for (const col of selectColumns) {
    if (col.type === 'star') {
      const prefix = col.table ? `${col.table}.` : undefined
      for (const key of childColumns) {
        if (prefix && !key.startsWith(prefix)) continue
        const dotIndex = key.indexOf('.')
        const outputKey = dotIndex >= 0 ? key.substring(dotIndex + 1) : key
        result.push(outputKey)
      }
    } else {
      result.push(col.alias ?? derivedAlias(col.expr))
    }
  }
  return result
}

/**
 * @param {ScanNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeScan(plan, context) {
  const { tables, signal } = context
  const table = validateTable({ ...plan, tables })
  validateScan({ ...plan, tables })
  const hasLimitOffset = plan.hints.limit !== undefined || plan.hints.offset // 0 offset is noop

  // Fast path: single column scan without WHERE
  if (table.scanColumn && plan.hints.columns?.length === 1 && !plan.hints.where) {
    const column = plan.hints.columns[0]
    const chunks = table.scanColumn({
      column,
      limit: plan.hints.limit,
      offset: plan.hints.offset,
      signal,
    })
    const scanRows = computeScanRows(table.numRows, plan.hints.limit, plan.hints.offset)
    return {
      columns: [column],
      numRows: scanRows,
      maxRows: scanRows,
      async *rows() {
        const columns = [column]
        for await (const chunk of chunks) {
          if (signal?.aborted) return
          for (let i = 0; i < chunk.length; i++) {
            const value = chunk[i]
            yield {
              columns,
              cells: { [column]: () => Promise.resolve(value) },
            }
          }
        }
      },
    }
  }

  // do the scan
  const scanResult = table.scan({ ...plan.hints, signal })
  const { appliedWhere, appliedLimitOffset } = scanResult

  // Applied limit/offset without applied where is invalid
  if (!appliedWhere && appliedLimitOffset && plan.hints.where && hasLimitOffset) {
    throw new Error(`Data source "${plan.table}" applied limit/offset without applying where`)
  }

  const scanRows = computeScanRows(table.numRows, plan.hints.limit, plan.hints.offset)
  return {
    columns: plan.hints.columns ?? table.columns,
    numRows: !plan.hints.where ? scanRows : undefined,
    maxRows: scanRows,
    async *rows() {
      let result = scanResult.rows()

      // Apply WHERE if data source did not
      if (!appliedWhere && plan.hints.where) {
        result = filterRows(result, plan.hints.where, context, plan.hints.limit)
      }

      // Apply LIMIT/OFFSET if data source did not
      if (!appliedLimitOffset && hasLimitOffset) {
        result = limitRows(result, plan.hints.limit, plan.hints.offset, signal)
      }

      yield* result
    },
  }
}

/**
 * Executes a Count node using numRows when available, falling back to scan
 *
 * @param {CountNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeCount(plan, context) {
  const { tables, signal } = context
  const table = validateTable({ ...plan, tables })
  const columns = plan.columns.map(col => col.alias ?? derivedAlias(col.expr))

  return {
    columns,
    numRows: 1,
    maxRows: 1,
    async *rows() {
      // Use source numRows if available
      const countPromise = table.numRows !== undefined ? Promise.resolve(table.numRows) : (async () => {
        // Fall back to counting rows via scan
        let count = 0
        const { rows } = table.scan({ signal })
        // eslint-disable-next-line no-unused-vars
        for await (const _ of rows()) {
          if (signal?.aborted) return
          count++
        }
        return count
      })()

      /** @type {AsyncCells} */
      const cells = {}
      for (const alias of columns) {
        cells[alias] = () => countPromise
      }
      yield { columns, cells }
    },
  }
}

/**
 * Computes numRows for a scan when the table provides numRows and there is no WHERE.
 *
 * @param {number | undefined} tableNumRows
 * @param {number} [limit]
 * @param {number} [offset]
 * @returns {number | undefined}
 */
function computeScanRows(tableNumRows, limit, offset) {
  if (tableNumRows === undefined) return undefined
  const afterOffset = Math.max(0, tableNumRows - (offset ?? 0))
  return limit !== undefined ? Math.min(limit, afterOffset) : afterOffset
}

/**
 * Filters rows by a condition
 *
 * @param {AsyncIterable<AsyncRow>} rows
 * @param {ExprNode} condition
 * @param {ExecuteContext} context
 * @param {number} [limit] - downstream LIMIT hint for chunk sizing
 * @yields {AsyncRow}
 */
async function* filterRows(rows, condition, context, limit) {
  const MAX_CHUNK = 256
  let chunkSize = limit ?? Infinity
  let rowIndex = 0

  /** @type {{ row: AsyncRow, rowIndex: number }[]} */
  let buffer = []

  for await (const row of rows) {
    if (context.signal?.aborted) return
    rowIndex++
    buffer.push({ row, rowIndex })

    if (buffer.length >= chunkSize) {
      const results = await Promise.all(buffer.map(b =>
        evaluateExpr({ node: condition, row: b.row, rowIndex: b.rowIndex, context })
      ))
      for (let i = 0; i < buffer.length; i++) {
        if (results[i]) yield buffer[i].row
      }
      buffer = []
      chunkSize = Math.min(chunkSize * 2, MAX_CHUNK)
    }
  }

  // Flush remaining rows
  if (buffer.length > 0) {
    const results = await Promise.all(buffer.map(b =>
      evaluateExpr({ node: condition, row: b.row, rowIndex: b.rowIndex, context })
    ))
    for (let i = 0; i < buffer.length; i++) {
      if (results[i]) yield buffer[i].row
    }
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
async function* limitRows(rows, limit = Infinity, offset = 0, signal) {
  if (limit <= 0) return
  let skipped = 0
  let yielded = 0
  for await (const row of rows) {
    if (signal?.aborted) return
    if (skipped < offset) {
      skipped++
      continue
    }
    yield row
    yielded++
    if (yielded >= limit) return
  }
}

/**
 * Executes a filter operation (WHERE clause)
 *
 * @param {FilterNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeFilter(plan, context) {
  const child = executePlan({ plan: plan.child, context })
  return {
    columns: child.columns,
    maxRows: child.maxRows,
    rows: () => filterRows(child.rows(), plan.condition, context),
  }
}

/**
 * Executes a projection operation (SELECT columns)
 *
 * @param {ProjectNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeProject(plan, context) {
  const child = executePlan({ plan: plan.child, context })
  const columns = selectColumnNames(plan.columns, child.columns)

  const resolveable = plan.columns.every(col =>
    col.type === 'star' || col.type === 'derived' && col.expr.type === 'identifier'
  )

  return {
    columns,
    numRows: child.numRows,
    maxRows: child.maxRows,
    async *rows() {
      let rowIndex = 0

      for await (const row of child.rows()) {
        if (context.signal?.aborted) return
        rowIndex++
        const currentRowIndex = rowIndex

        /** @type {AsyncCells} */
        const cells = {}
        // Only safe to propagate resolved when every output column comes from
        // the star branch — derived expressions evaluate lazily and can't be
        // pre-materialized here, and a partial resolved would make
        // collect()/downstream identifier fast paths read undefined.
        const source = resolveable ? row.resolved : undefined
        /** @type {Record<string, SqlPrimitive> | undefined} */
        const resolved = source ? {} : undefined

        let colIdx = 0
        for (const col of plan.columns) {
          if (col.type === 'star') {
            const prefix = col.table ? `${col.table}.` : undefined
            for (const key of row.columns) {
              if (prefix && !key.startsWith(prefix)) continue
              const dotIndex = key.indexOf('.')
              const outputKey = dotIndex >= 0 ? key.substring(dotIndex + 1) : key
              cells[outputKey] = row.cells[key]
              if (resolved && source) resolved[outputKey] = source[key]
              colIdx++
            }
          } else if (col.expr.type === 'identifier') {
            // Common case: simple identifier. Avoid evaluateExpr overhead by
            // directly mapping to the child's cell, relying on the planner to
            // have normalized the identifier to match the child's column layout.
            // If the identifier didn't normalize to a child cell key (e.g. the
            // name refers to a table alias that produced no matching column),
            // fall through to the evaluator so suffix-search and the proper
            // ColumnNotFoundError apply instead of emitting an undefined cell.
            const id = col.expr
            const sourceName = id.prefix ? `${id.prefix}.${id.name}` : id.name
            const alias = columns[colIdx++]
            if (sourceName in row.cells) {
              cells[alias] = row.cells[sourceName]
              if (resolved && source) resolved[alias] = source[sourceName]
            } else {
              const { expr } = col
              cells[alias] = () => evaluateExpr({
                node: expr,
                row,
                rowIndex: currentRowIndex,
                context,
              })
            }
          } else {
            const alias = columns[colIdx++]
            cells[alias] = () => evaluateExpr({
              node: col.expr,
              row,
              rowIndex: currentRowIndex,
              context,
            })
          }
        }

        yield { columns, cells, resolved }
      }
    },
  }
}

/**
 * Executes a distinct operation
 *
 * @param {DistinctNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeDistinct(plan, context) {
  const child = executePlan({ plan: plan.child, context })
  return {
    columns: child.columns,
    maxRows: child.maxRows,
    async *rows() {
      const { signal } = context
      const MAX_CHUNK = 256

      const seen = new Set()

      /** @type {AsyncRow[]} */
      let buffer = []

      for await (const row of child.rows()) {
        if (signal?.aborted) return
        buffer.push(row)

        if (buffer.length >= MAX_CHUNK) {
          const keys = buffer.map(stableRowKey)
          for (let i = 0; i < buffer.length; i++) {
            const key = await keys[i]
            if (!seen.has(key)) {
              seen.add(key)
              yield buffer[i]
            }
          }
          buffer = []
        }
      }

      // Flush remaining
      if (buffer.length > 0) {
        const keys = buffer.map(stableRowKey)
        for (let i = 0; i < buffer.length; i++) {
          const key = await keys[i]
          if (!seen.has(key)) {
            seen.add(key)
            yield buffer[i]
          }
        }
      }
    },
  }
}

/**
 * Executes a limit operation (LIMIT/OFFSET)
 *
 * @param {LimitNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeLimit(plan, context) {
  const child = executePlan({ plan: plan.child, context })
  return {
    columns: child.columns,
    numRows: computeScanRows(child.numRows, plan.limit, plan.offset),
    maxRows: computeScanRows(child.maxRows, plan.limit, plan.offset),
    rows: () => limitRows(child.rows(), plan.limit, plan.offset, context.signal),
  }
}

/**
 * Executes a set operation (UNION, INTERSECT, EXCEPT)
 *
 * @param {SetOperationNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeSetOperation(plan, context) {
  const { signal } = context

  if (plan.operator === 'UNION') {
    if (plan.all) {
      const left = executePlan({ plan: plan.left, context })
      const right = executePlan({ plan: plan.right, context })
      return {
        columns: left.columns,
        numRows: addBounds(left.numRows, right.numRows),
        maxRows: addBounds(left.maxRows, right.maxRows),
        async *rows() {
          // UNION ALL: yield all rows from both sides
          yield* left.rows()
          yield* right.rows()
        },
      }
    } else {
      const left = executePlan({ plan: plan.left, context })
      const right = executePlan({ plan: plan.right, context })
      return {
        columns: left.columns,
        maxRows: addBounds(left.maxRows, right.maxRows),
        async *rows() {
          // UNION: yield deduplicated rows from both sides
          const seen = new Set()
          for await (const row of left.rows()) {
            if (signal?.aborted) return
            const key = await stableRowKey(row)
            if (!seen.has(key)) {
              seen.add(key)
              yield row
            }
          }
          for await (const row of right.rows()) {
            if (signal?.aborted) return
            const key = await stableRowKey(row)
            if (!seen.has(key)) {
              seen.add(key)
              yield row
            }
          }
        },
      }
    }
  } else if (plan.operator === 'INTERSECT') {
    const left = executePlan({ plan: plan.left, context })
    const right = executePlan({ plan: plan.right, context })
    return {
      columns: left.columns,
      maxRows: minBounds(left.maxRows, right.maxRows),
      async *rows() {
        // Materialize right side keys
        /** @type {Map<any, number>} */
        const rightKeys = new Map()
        for await (const row of right.rows()) {
          if (signal?.aborted) return
          const key = await stableRowKey(row)
          rightKeys.set(key, (rightKeys.get(key) ?? 0) + 1)
        }

        if (plan.all) {
          // INTERSECT ALL: yield each left row that matches, consuming right counts
          for await (const row of left.rows()) {
            if (signal?.aborted) return
            const key = await stableRowKey(row)
            const count = rightKeys.get(key)
            if (count) {
              rightKeys.set(key, count - 1)
              yield row
            }
          }
        } else {
          // INTERSECT: yield deduplicated rows present in both
          const seen = new Set()
          for await (const row of left.rows()) {
            if (signal?.aborted) return
            const key = await stableRowKey(row)
            if (rightKeys.has(key) && !seen.has(key)) {
              seen.add(key)
              yield row
            }
          }
        }
      },
    }
  } else {
    // EXCEPT
    const left = executePlan({ plan: plan.left, context })
    const right = executePlan({ plan: plan.right, context })
    return {
      columns: left.columns,
      maxRows: left.maxRows,
      async *rows() {
        // Materialize right side keys
        /** @type {Map<any, number>} */
        const rightKeys = new Map()
        for await (const row of right.rows()) {
          if (signal?.aborted) return
          const key = await stableRowKey(row)
          rightKeys.set(key, (rightKeys.get(key) ?? 0) + 1)
        }

        if (plan.all) {
          // EXCEPT ALL: yield left rows, consuming right counts
          for await (const row of left.rows()) {
            if (signal?.aborted) return
            const key = await stableRowKey(row)
            const count = rightKeys.get(key)
            if (count) {
              rightKeys.set(key, count - 1)
            } else {
              yield row
            }
          }
        } else {
          // EXCEPT: yield deduplicated left rows not in right
          const seen = new Set()
          for await (const row of left.rows()) {
            if (signal?.aborted) return
            const key = await stableRowKey(row)
            if (!rightKeys.has(key) && !seen.has(key)) {
              seen.add(key)
              yield row
            }
          }
        }
      },
    }
  }
}
