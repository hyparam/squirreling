import { missingClauseError } from '../parseErrors.js'
import { tableNotFoundError, unsupportedOperationError } from '../executionErrors.js'
import { generatorSource, memorySource } from '../backend/dataSource.js'
import { parseSql } from '../parse/parse.js'
import { containsAggregate, extractColumns } from './columns.js'
import { evaluateExpr } from './expression.js'
import { evaluateHavingExpr } from './having.js'
import { executeJoins } from './join.js'
import { compareForTerm, defaultDerivedAlias, stringify } from './utils.js'

/**
 * @import { AsyncCells, AsyncDataSource, AsyncRow, ExecuteSqlOptions, OrderByItem, QueryHints, SelectStatement, SqlPrimitive, UserDefinedFunction } from '../types.js'
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
  /** @type {AsyncDataSource} */
  let dataSource
  /** @type {string} */
  let leftTable

  if (select.from.kind === 'table') {
    // Use alias for column prefixing, but look up the actual table name
    leftTable = select.from.alias ?? select.from.table
    dataSource = tables[select.from.table]
    if (dataSource === undefined) {
      throw tableNotFoundError({ tableName: select.from.table })
    }
  } else {
    // Nested subquery - recursively resolve
    leftTable = select.from.alias
    dataSource = generatorSource(executeSelect({ select: select.from.query, tables, functions, signal }))
  }

  // Execute JOINs if present
  if (select.joins.length) {
    dataSource = await executeJoins({ leftSource: dataSource, joins: select.joins, leftTable, tables, functions })
  }

  yield* evaluateSelectAst({ select, dataSource, tables, functions, signal })
}

/**
 * Creates a stable string key for a row to enable deduplication
 *
 * @param {AsyncCells} cells
 * @returns {Promise<string>} a stable string representation of the row
 */
async function stableRowKey(cells) {
  const keys = Object.keys(cells).sort()
  /** @type {string[]} */
  const parts = []
  for (const k of keys) {
    const v = await cells[k]()
    parts.push(k + ':' + stringify(v))
  }
  return parts.join('|')
}

/**
 * Applies DISTINCT filtering to remove duplicate rows
 *
 * @param {AsyncRow[]} rows - the input rows
 * @param {boolean} distinct - whether to apply deduplication
 * @returns {Promise<AsyncRow[]>} the deduplicated rows
 */
async function applyDistinct(rows, distinct) {
  if (!distinct) return rows
  /** @type {Set<string>} */
  const seen = new Set()
  /** @type {AsyncRow[]} */
  const result = []
  for (const row of rows) {
    const key = await stableRowKey(row.cells)
    if (seen.has(key)) continue
    seen.add(key)
    result.push(row)
  }
  return result
}

/**
 * Applies ORDER BY sorting to rows using multi-pass lazy evaluation.
 * Secondary ORDER BY columns are only evaluated for rows that tie on
 * previous columns, reducing expensive cell evaluations.
 *
 * @param {Object} options
 * @param {AsyncRow[]} options.rows - the input rows
 * @param {OrderByItem[]} options.orderBy - the sort specifications
 * @param {Record<string, AsyncDataSource>} options.tables
 * @param {Record<string, UserDefinedFunction>} [options.functions]
 * @returns {Promise<AsyncRow[]>} the sorted rows
 */
async function sortRows({ rows, orderBy, tables, functions }) {
  if (!orderBy.length) return rows

  // Cache for evaluated values: evaluatedValues[rowIdx][colIdx]
  /** @type {(SqlPrimitive | undefined)[][]} */
  const evaluatedValues = rows.map(() => Array(orderBy.length))

  // Start with all indices in one group
  /** @type {number[][]} */
  let groups = [rows.map((_, i) => i)]

  // Process each ORDER BY column incrementally
  for (let orderByIdx = 0; orderByIdx < orderBy.length; orderByIdx++) {
    const term = orderBy[orderByIdx]
    /** @type {number[][]} */
    const nextGroups = []

    for (const group of groups) {
      // Single-element groups don't need sorting or evaluation
      if (group.length <= 1) {
        nextGroups.push(group)
        continue
      }

      // Evaluate this column for all rows in the group
      for (const idx of group) {
        if (evaluatedValues[idx][orderByIdx] === undefined) {
          evaluatedValues[idx][orderByIdx] = await evaluateExpr({
            node: term.expr,
            row: rows[idx],
            tables,
            functions,
          })
        }
      }

      // Sort the group by this column
      group.sort((aIdx, bIdx) => {
        const av = evaluatedValues[aIdx][orderByIdx]
        const bv = evaluatedValues[bIdx][orderByIdx]
        return compareForTerm(av, bv, term)
      })

      // Split into sub-groups based on ties (for next column)
      if (orderByIdx < orderBy.length - 1) {
        /** @type {number[]} */
        let currentSubGroup = [group[0]]
        for (let i = 1; i < group.length; i++) {
          const prevIdx = group[i - 1]
          const currIdx = group[i]
          const prevVal = evaluatedValues[prevIdx][orderByIdx]
          const currVal = evaluatedValues[currIdx][orderByIdx]

          if (compareForTerm(prevVal, currVal, term) === 0) {
            // Same value, extend current sub-group
            currentSubGroup.push(currIdx)
          } else {
            // Different value, start new sub-group
            nextGroups.push(currentSubGroup)
            currentSubGroup = [currIdx]
          }
        }
        nextGroups.push(currentSubGroup)
      } else {
        // Last column, no need to split
        nextGroups.push(group)
      }
    }

    groups = nextGroups
  }

  // Flatten groups to get final sorted indices
  return groups.flat().map(i => rows[i])
}

/**
 * Evaluates a select with a resolved FROM data source
 *
 * @param {Object} options
 * @param {SelectStatement} options.select
 * @param {AsyncDataSource} options.dataSource
 * @param {Record<string, AsyncDataSource>} options.tables
 * @param {Record<string, UserDefinedFunction>} [options.functions]
 * @param {AbortSignal} [options.signal]
 * @yields {AsyncRow}
 */
async function* evaluateSelectAst({ select, dataSource, tables, functions, signal }) {
  // SQL priority: from, where, group by, having, select, order by, offset, limit

  const hasAggregate = select.columns.some(col => col.kind === 'derived' && containsAggregate(col.expr))
  const useGrouping = hasAggregate || select.groupBy.length > 0
  const needsBuffering = useGrouping || select.orderBy.length > 0

  if (needsBuffering) {
    // BUFFERING PATH: Collect all rows, process, then yield
    yield* evaluateBuffered({ select, dataSource, tables, functions, hasAggregate, useGrouping, signal })
  } else {
    // STREAMING PATH: Yield rows one by one
    yield* evaluateStreaming({ select, dataSource, tables, functions, signal })
  }
}

/**
 * Streaming evaluation for simple queries (no ORDER BY or GROUP BY)
 * Supports DISTINCT by tracking seen row keys without buffering full rows
 *
 * @param {Object} options
 * @param {SelectStatement} options.select
 * @param {AsyncDataSource} options.dataSource
 * @param {Record<string, AsyncDataSource>} options.tables
 * @param {Record<string, UserDefinedFunction>} [options.functions]
 * @param {AbortSignal} [options.signal]
 * @yields {AsyncRow}
 */
async function* evaluateStreaming({ select, dataSource, tables, functions, signal }) {
  let rowsYielded = 0
  let rowsSkipped = 0
  let rowIndex = 0
  const offset = select.offset ?? 0
  const limit = select.limit ?? Infinity
  if (limit <= 0) return

  // For DISTINCT, track seen row keys
  /** @type {Set<string> | undefined} */
  const seen = select.distinct ? new Set() : undefined

  // hints for data source optimization
  /** @type {QueryHints} */
  const hints = {
    columns: extractColumns(select),
    where: select.where,
    limit: select.limit,
    offset: select.offset,
  }

  for await (const row of dataSource.scan({ hints, signal })) {
    rowIndex++
    // WHERE filter
    if (select.where) {
      const pass = await evaluateExpr({ node: select.where, row, tables, functions, rowIndex })
      if (!pass) continue
    }

    // For non-DISTINCT queries, we can skip rows before projection (optimization)
    if (!seen && rowsSkipped < offset) {
      rowsSkipped++
      continue
    }

    // SELECT projection
    /** @type {string[]} */
    const columns = []
    /** @type {AsyncCells} */
    const cells = {}
    const currentRowIndex = rowIndex
    for (const col of select.columns) {
      if (col.kind === 'star') {
        for (const key of row.columns) {
          columns.push(key)
          cells[key] = row.cells[key]
        }
      } else if (col.kind === 'derived') {
        const alias = col.alias ?? defaultDerivedAlias(col.expr)
        columns.push(alias)
        cells[alias] = () => evaluateExpr({ node: col.expr, row, tables, functions, rowIndex: currentRowIndex })
      }
    }

    // DISTINCT: skip duplicate rows
    if (seen) {
      const key = await stableRowKey(cells)
      if (seen.has(key)) continue
      seen.add(key)
      // OFFSET applies to distinct rows
      if (rowsSkipped < offset) {
        rowsSkipped++
        continue
      }
    }

    yield { columns, cells }
    rowsYielded++
    if (rowsYielded >= limit) {
      break
    }
  }
}

/**
 * Buffered evaluation for complex queries (with ORDER BY or GROUP BY)
 *
 * @param {Object} options
 * @param {SelectStatement} options.select
 * @param {AsyncDataSource} options.dataSource
 * @param {Record<string, AsyncDataSource>} options.tables
 * @param {Record<string, UserDefinedFunction>} [options.functions]
 * @param {boolean} options.hasAggregate
 * @param {boolean} options.useGrouping
 * @param {AbortSignal} [options.signal]
 * @yields {AsyncRow}
 */
async function* evaluateBuffered({ select, dataSource, tables, functions, hasAggregate, useGrouping, signal }) {
  // Build hints for data source optimization
  // Note: limit/offset not passed here since buffering needs all rows for sorting/grouping
  /** @type {QueryHints} */
  const hints = {
    where: select.where,
    columns: extractColumns(select),
  }

  // Step 1: Collect all rows from data source
  /** @type {AsyncRow[]} */
  const working = []
  for await (const row of dataSource.scan({ hints, signal })) {
    working.push(row)
  }

  // Step 2: WHERE clause filtering
  /** @type {AsyncRow[]} */
  const filtered = []

  for (let i = 0; i < working.length; i++) {
    const row = working[i]
    const rowIndex = i + 1 // 1-based
    if (select.where) {
      const passes = await evaluateExpr({ node: select.where, row, tables, functions, rowIndex })

      if (!passes) {
        continue
      }
    }
    filtered.push(row)
  }

  // Step 3: Projection (grouping vs non-grouping)
  /** @type {AsyncRow[]} */
  let projected = []

  if (useGrouping) {
    // Grouping due to GROUP BY or aggregate functions
    /** @type {AsyncRow[][]} */
    const groups = []

    if (select.groupBy.length) {
      /** @type {Map<string, AsyncRow[]>} */
      const map = new Map()
      for (const row of filtered) {
        /** @type {string[]} */
        const keyParts = []
        for (const expr of select.groupBy) {
          const v = await evaluateExpr({ node: expr, row, tables, functions })
          keyParts.push(stringify(v))
        }
        const key = keyParts.join('|')
        let group = map.get(key)
        if (!group) {
          group = []
          map.set(key, group)
          groups.push(group)
        }
        group.push(row)
      }
    } else {
      groups.push(filtered)
    }

    const hasStar = select.columns.some(col => col.kind === 'star')
    if (hasStar && hasAggregate) {
      throw unsupportedOperationError({
        operation: 'SELECT * with aggregate functions is not supported',
        hint: 'Replace * with specific column names when using aggregate functions.',
      })
    }

    for (const group of groups) {
      const columns = []
      /** @type {AsyncCells} */
      const cells = {}
      for (const col of select.columns) {
        if (col.kind === 'star') {
          const firstRow = group[0]
          if (firstRow) {
            for (const key of firstRow.columns) {
              columns.push(key)
              cells[key] = firstRow.cells[key]
            }
          }
          continue
        }

        if (col.kind === 'derived') {
          const alias = col.alias ?? defaultDerivedAlias(col.expr)
          columns.push(alias)
          // Pass group to evaluateExpr so it can handle aggregate functions within expressions
          // For empty groups, still provide an empty row context for aggregates to return appropriate values
          cells[alias] = () => evaluateExpr({ node: col.expr, row: group[0] ?? { columns: [], cells: {} }, tables, functions, rows: group })
          continue
        }
      }
      const asyncRow = { columns, cells }

      // Apply HAVING filter before adding to projected results
      if (select.having) {
        if (!await evaluateHavingExpr({ expr: select.having, row: asyncRow, group, tables, functions })) {
          continue
        }
      }

      projected.push(asyncRow)
    }
  } else {
    // No grouping, simple projection
    // Sort before projection so ORDER BY can access columns not in SELECT
    const sorted = await sortRows({ rows: filtered, orderBy: select.orderBy, tables, functions })

    // OPTIMIZATION: For non-DISTINCT queries, apply OFFSET/LIMIT before projection
    // to avoid reading expensive cells for rows that won't be in the final result
    let rowsToProject = sorted
    if (!select.distinct) {
      const start = select.offset ?? 0
      const end = select.limit ? start + select.limit : sorted.length
      rowsToProject = sorted.slice(start, end)
    }

    for (const row of rowsToProject) {
      const columns = []
      /** @type {AsyncCells} */
      const cells = {}
      for (const col of select.columns) {
        if (col.kind === 'star') {
          for (const key of row.columns) {
            columns.push(key)
            cells[key] = row.cells[key]
          }
        } else if (col.kind === 'derived') {
          const alias = col.alias ?? defaultDerivedAlias(col.expr)
          columns.push(alias)
          cells[alias] = () => evaluateExpr({ node: col.expr, row, tables, functions })
        }
      }
      projected.push({ columns, cells })
    }
  }

  // Step 4: DISTINCT
  projected = await applyDistinct(projected, select.distinct)

  // Step 5: ORDER BY (final sort for grouped queries)
  if (useGrouping) {
    projected = await sortRows({ rows: projected, orderBy: select.orderBy, tables, functions })
  }

  // Step 6: OFFSET and LIMIT
  // For non-DISTINCT, non-grouping queries, OFFSET/LIMIT was already applied before projection
  if (select.distinct || useGrouping) {
    const start = select.offset ?? 0
    const end = select.limit ? start + select.limit : projected.length

    // Step 7: Yield results
    for (let i = start; i < end && i < projected.length; i++) {
      yield projected[i]
    }
  } else {
    // Already limited, yield all projected rows
    for (const row of projected) {
      yield row
    }
  }
}
