import { memorySource } from '../backend/dataSource.js'
import { derivedAlias } from '../expression/alias.js'
import { evaluateExpr } from '../expression/evaluate.js'
import { parseSql } from '../parse/parse.js'
import { planSql } from '../plan/plan.js'
import { validateScan, validateTable } from '../validation/tables.js'
import { executeHashAggregate, executeScalarAggregate } from './aggregates.js'
import { executeHashJoin, executeNestedLoopJoin, executePositionalJoin } from './join.js'
import { executeSort } from './sort.js'
import { stableRowKey } from './utils.js'

/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, ExecuteContext, ExecuteSqlOptions, ExprNode, QueryResults, Statement } from '../types.js'
 * @import { CountNode, DistinctNode, FilterNode, LimitNode, ProjectNode, QueryPlan, ScanNode, SetOperationNode } from '../plan/types.js'
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

  const context = { tables: normalizedTables, functions, signal }
  const plan = planSql({ query: parsed, functions, tables: normalizedTables })
  return executePlan({ plan, context })
}

/**
 * Executes a statement against the provided tables
 *
 * @param {Object} options
 * @param {Statement} options.query
 * @param {ExecuteContext} options.context
 * @returns {QueryResults}
 */
export function executeStatement({ query, context }) {
  const plan = planSql({ query, functions: context.functions, tables: context.tables })
  return executePlan({ plan, context })
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
  }
  return { async *rows () {} }
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
    return {
      async *rows () {
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

  return {
    async *rows () {
      let result = scanResult.rows

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

  return {
    async *rows () {
      // Use source numRows if available
      let count = table.numRows
      if (count === undefined) {
        // Fall back to counting rows via scan
        count = 0
        const { rows } = table.scan({ signal })
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
    },
  }
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
 * @returns {QueryResults}
 */
function executeFilter(plan, context) {
  const child = executePlan({ plan: plan.child, context })
  return {
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
  return {
    async *rows () {
      let rowIndex = 0

      for await (const row of child.rows()) {
        if (context.signal?.aborted) return
        rowIndex++
        const currentRowIndex = rowIndex

        /** @type {string[]} */
        const columns = []
        /** @type {AsyncCells} */
        const cells = {}

        for (const col of plan.columns) {
          if (col.type === 'star') {
            const prefix = col.table ? `${col.table}.` : undefined
            for (const key of row.columns) {
              if (prefix && !key.startsWith(prefix)) continue
              // Strip table prefix for output column names
              const dotIndex = key.indexOf('.')
              const outputKey = prefix ? key.substring(prefix.length) : dotIndex >= 0 ? key.substring(dotIndex + 1) : key
              columns.push(outputKey)
              cells[outputKey] = row.cells[key]
            }
          } else {
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
    async *rows () {
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
        async *rows () {
          // UNION ALL: yield all rows from both sides
          yield* left.rows()
          yield* right.rows()
        },
      }
    } else {
      const left = executePlan({ plan: plan.left, context })
      const right = executePlan({ plan: plan.right, context })
      return {
        async *rows () {
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
      async *rows () {
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
      async *rows () {
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
