import { missingClauseError, tableNotFoundError, unsupportedOperationError } from '../errors.js'
import { generatorSource, memorySource } from '../backend/dataSource.js'
import { parseSql } from '../parse/parse.js'
import { defaultAggregateAlias, evaluateAggregate } from './aggregates.js'
import { extractColumns } from './columns.js'
import { evaluateExpr } from './expression.js'
import { evaluateHavingExpr } from './having.js'
import { executeJoins } from './join.js'
import { compareForTerm, defaultDerivedAlias, stringify } from './utils.js'

/**
 * @import { AsyncDataSource, AsyncRow, ExecuteSqlOptions, OrderByItem, QueryHints, SelectStatement, SqlPrimitive } from '../types.js'
 */

/**
 * Executes a SQL SELECT query against named data sources
 *
 * @param {ExecuteSqlOptions} options - the execution options
 * @yields {AsyncRow} async generator yielding result rows
 */
export async function* executeSql({ tables, query }) {
  const select = parseSql(query)

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

  yield* executeSelect(select, normalizedTables)
}

/**
 * Executes a SELECT query against the provided tables
 *
 * @param {SelectStatement} select
 * @param {Record<string, AsyncDataSource>} tables
 * @yields {AsyncRow}
 */
export async function* executeSelect(select, tables) {
  /** @type {AsyncDataSource} */
  let dataSource
  /** @type {string} */
  let fromTableName

  if (select.from.kind === 'table') {
    // Use alias for column prefixing, but look up the actual table name
    fromTableName = select.from.alias ?? select.from.table
    dataSource = tables[select.from.table]
    if (dataSource === undefined) {
      throw tableNotFoundError(select.from.table)
    }
  } else {
    // Nested subquery - recursively resolve
    fromTableName = select.from.alias
    dataSource = generatorSource(executeSelect(select.from.query, tables))
  }

  // Execute JOINs if present
  if (select.joins.length) {
    dataSource = await executeJoins(dataSource, select.joins, fromTableName, tables)
  }

  yield* evaluateSelectAst(select, dataSource, tables)
}

/**
 * Creates a stable string key for a row to enable deduplication
 *
 * @param {AsyncRow} row
 * @returns {Promise<string>} a stable string representation of the row
 */
async function stableRowKey(row) {
  const keys = Object.keys(row).sort()
  /** @type {string[]} */
  const parts = []
  for (const k of keys) {
    const v = await row[k]()
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
    const key = await stableRowKey(row)
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
 * @param {AsyncRow[]} rows - the input rows
 * @param {OrderByItem[]} orderBy - the sort specifications
 * @param {Record<string, AsyncDataSource>} tables
 * @returns {Promise<AsyncRow[]>} the sorted rows
 */
async function sortRows(rows, orderBy, tables) {
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
 * @param {SelectStatement} select
 * @param {AsyncDataSource} dataSource
 * @param {Record<string, AsyncDataSource>} tables
 * @yields {AsyncRow}
 */
async function* evaluateSelectAst(select, dataSource, tables) {
  // SQL priority: from, where, group by, having, select, order by, offset, limit

  const hasAggregate = select.columns.some(col => col.kind === 'aggregate')
  const useGrouping = hasAggregate || select.groupBy.length > 0
  const needsBuffering = useGrouping || select.orderBy.length > 0

  if (needsBuffering) {
    // BUFFERING PATH: Collect all rows, process, then yield
    yield* evaluateBuffered(select, dataSource, tables, hasAggregate, useGrouping)
  } else {
    // STREAMING PATH: Yield rows one by one
    yield* evaluateStreaming(select, dataSource, tables)
  }
}

/**
 * Streaming evaluation for simple queries (no ORDER BY or GROUP BY)
 * Supports DISTINCT by tracking seen row keys without buffering full rows
 *
 * @param {SelectStatement} select
 * @param {AsyncDataSource} dataSource
 * @param {Record<string, AsyncDataSource>} tables
 * @yields {AsyncRow}
 */
async function* evaluateStreaming(select, dataSource, tables) {
  let rowsYielded = 0
  let rowsSkipped = 0
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

  for await (const row of dataSource.getRows(hints)) {
    // WHERE filter
    if (select.where) {
      const pass = await evaluateExpr({ node: select.where, row, tables })
      if (!pass) continue
    }

    // For non-DISTINCT queries, we can skip rows before projection (optimization)
    if (!seen && rowsSkipped < offset) {
      rowsSkipped++
      continue
    }

    // SELECT projection
    /** @type {AsyncRow} */
    const outRow = {}
    for (const col of select.columns) {
      if (col.kind === 'star') {
        for (const [key, cell] of Object.entries(row)) {
          outRow[key] = cell
        }
      } else if (col.kind === 'derived') {
        const alias = col.alias ?? defaultDerivedAlias(col.expr)
        outRow[alias] = () => evaluateExpr({ node: col.expr, row, tables })
      } else if (col.kind === 'aggregate') {
        throw new Error(
          'Aggregate functions require GROUP BY or will act on the whole dataset; add GROUP BY or remove aggregates'
        )
      }
    }

    // DISTINCT: skip duplicate rows
    if (seen) {
      const key = await stableRowKey(outRow)
      if (seen.has(key)) continue
      seen.add(key)
      // OFFSET applies to distinct rows
      if (rowsSkipped < offset) {
        rowsSkipped++
        continue
      }
    }

    yield outRow
    rowsYielded++
    if (rowsYielded >= limit) {
      break
    }
  }
}

/**
 * Buffered evaluation for complex queries (with ORDER BY or GROUP BY)
 *
 * @param {SelectStatement} select
 * @param {AsyncDataSource} dataSource
 * @param {Record<string, AsyncDataSource>} tables
 * @param {boolean} hasAggregate
 * @param {boolean} useGrouping
 * @yields {AsyncRow}
 */
async function* evaluateBuffered(select, dataSource, tables, hasAggregate, useGrouping) {
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
  for await (const row of dataSource.getRows(hints)) {
    working.push(row)
  }

  // Step 2: WHERE clause filtering
  /** @type {AsyncRow[]} */
  const filtered = []

  for (const row of working) {
    if (select.where) {
      const passes = await evaluateExpr({ node: select.where, row, tables })

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
          const v = await evaluateExpr({ node: expr, row, tables })
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
      throw unsupportedOperationError(
        'SELECT * with aggregate functions is not supported',
        'Replace * with specific column names when using aggregate functions.'
      )
    }

    for (const group of groups) {
      /** @type {AsyncRow} */
      const resultRow = {}
      for (const col of select.columns) {
        if (col.kind === 'star') {
          const firstRow = group[0]
          if (firstRow) {
            for (const [key, cell] of Object.entries(firstRow)) {
              resultRow[key] = cell
            }
          }
          continue
        }

        if (col.kind === 'derived') {
          const alias = col.alias ?? defaultDerivedAlias(col.expr)
          if (group.length > 0) {
            resultRow[alias] = () => evaluateExpr({ node: col.expr, row: group[0], tables })
          } else {
            delete resultRow[alias]
          }
          continue
        }

        if (col.kind === 'aggregate') {
          const alias = col.alias ?? defaultAggregateAlias(col)
          resultRow[alias] = () => evaluateAggregate({ col, rows: group, tables })
          continue
        }
      }

      // Apply HAVING filter before adding to projected results
      if (select.having) {
        if (!await evaluateHavingExpr(select.having, resultRow, group, tables)) {
          continue
        }
      }

      projected.push(resultRow)
    }
  } else {
    // No grouping, simple projection
    // Sort before projection so ORDER BY can access columns not in SELECT
    const sorted = await sortRows(filtered, select.orderBy, tables)

    // OPTIMIZATION: For non-DISTINCT queries, apply OFFSET/LIMIT before projection
    // to avoid reading expensive cells for rows that won't be in the final result
    let rowsToProject = sorted
    if (!select.distinct) {
      const start = select.offset ?? 0
      const end = select.limit ? start + select.limit : sorted.length
      rowsToProject = sorted.slice(start, end)
    }

    for (const row of rowsToProject) {
      /** @type {AsyncRow} */
      const outRow = {}
      for (const col of select.columns) {
        if (col.kind === 'star') {
          for (const [key, cell] of Object.entries(row)) {
            outRow[key] = cell
          }
        } else if (col.kind === 'derived') {
          const alias = col.alias ?? defaultDerivedAlias(col.expr)
          outRow[alias] = () => evaluateExpr({ node: col.expr, row, tables })
        }
      }
      projected.push(outRow)
    }
  }

  // Step 4: DISTINCT
  projected = await applyDistinct(projected, select.distinct)

  // Step 5: ORDER BY (final sort for grouped queries)
  if (useGrouping) {
    projected = await sortRows(projected, select.orderBy, tables)
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
