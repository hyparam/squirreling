import { evaluateExpr } from './expression.js'
import { parseSql } from '../parse/parse.js'
import { asyncRow, generatorSource, memorySource } from '../backend/dataSource.js'
import { defaultAggregateAlias, evaluateAggregate } from './aggregates.js'
import { evaluateHavingExpr } from './having.js'

/**
 * @import { AsyncDataSource, ExecuteSqlOptions, ExprNode, OrderByItem, AsyncRow, SelectStatement, SqlPrimitive } from '../types.js'
 */

/**
 * Executes a SQL SELECT query against named data sources
 *
 * @param {ExecuteSqlOptions} options - the execution options
 * @returns {AsyncGenerator<Record<string, any>>} async generator yielding result rows
 */
export async function* executeSql({ tables, query }) {
  const select = parseSql(query)

  // Check for unsupported operations
  if (select.joins.length) {
    throw new Error('JOIN is not supported')
  }
  if (!select.from) {
    throw new Error('FROM clause is required')
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
 * @returns {AsyncGenerator<Record<string, any>>} async generator yielding result rows
 */
export async function* executeSelect(select, tables) {
  /** @type {AsyncDataSource} */
  let dataSource

  if (typeof select.from === 'string') {
    const table = tables[select.from]
    if (table === undefined) {
      throw new Error(`Table "${select.from}" not found`)
    }

    dataSource = table
  } else {
    // Nested subquery - recursively resolve
    dataSource = generatorSource(executeSelect(select.from.query, tables))
  }

  yield* evaluateSelectAst(select, dataSource, tables)
}

/**
 * Generates a default alias for a derived column expression
 *
 * @param {ExprNode} expr - the expression node
 * @returns {string} the generated alias
 */
function defaultDerivedAlias(expr) {
  if (expr.type === 'identifier') {
    return expr.name
  }
  if (expr.type === 'function') {
    const base = expr.name.toLowerCase()
    // Try to extract column names from identifier arguments
    const columnNames = expr.args
      .filter(arg => arg.type === 'identifier')
      .map(arg => arg.name)
    if (columnNames.length > 0) {
      return base + '_' + columnNames.join('_')
    }
    return base
  }
  if (expr.type === 'cast') return 'cast_expr'
  if (expr.type === 'unary' && expr.argument.type === 'identifier') {
    return expr.op === '-' ? 'neg_' + expr.argument.name : 'expr'
  }
  return 'expr'
}

/**
 * Creates a stable string key for a row to enable deduplication
 *
 * @param {Record<string, any>} row
 * @returns {string} a stable string representation of the row
 */
function stableRowKey(row) {
  const keys = Object.keys(row).sort()
  /** @type {string[]} */
  const parts = []
  for (const k of keys) {
    const v = row[k]
    parts.push(k + ':' + JSON.stringify(v))
  }
  return parts.join('|')
}

/**
 * Compares two SQL values for sorting
 *
 * @param {SqlPrimitive} a
 * @param {SqlPrimitive} b
 * @returns {number} negative if a < b, positive if a > b, 0 if equal
 */
function compareValues(a, b) {
  if (a === b) return 0
  if (a == null) return -1
  if (b == null) return 1

  if (typeof a === 'number' && typeof b === 'number') {
    if (a < b) return -1
    if (a > b) return 1
    return 0
  }

  const as = String(a)
  const bs = String(b)
  if (as < bs) return -1
  if (as > bs) return 1
  return 0
}

/**
 * Applies DISTINCT filtering to remove duplicate rows
 *
 * @param {Record<string, any>[]} rows - The input rows
 * @param {boolean} distinct - Whether to apply deduplication
 * @returns {Record<string, any>[]} The deduplicated rows
 */
function applyDistinct(rows, distinct) {
  if (!distinct) return rows
  /** @type {Set<string>} */
  const seen = new Set()
  /** @type {Record<string, any>[]} */
  const result = []
  for (const row of rows) {
    const key = stableRowKey(row)
    if (seen.has(key)) continue
    seen.add(key)
    result.push(row)
  }
  return result
}

/**
 * Applies ORDER BY sorting to RowSource array (before projection)
 *
 * @param {AsyncRow[]} rows - the input row sources
 * @param {OrderByItem[]} orderBy - the sort specifications
 * @param {Record<string, AsyncDataSource>} tables
 * @returns {Promise<AsyncRow[]>} the sorted row sources
 */
async function sortRowSources(rows, orderBy, tables) {
  if (!orderBy.length) return rows

  // Pre-evaluate ORDER BY expressions for all rows
  /** @type {SqlPrimitive[][]} */
  const evaluatedValues = []
  for (const row of rows) {
    /** @type {SqlPrimitive[]} */
    const rowValues = []
    for (const term of orderBy) {
      const value = await evaluateExpr({ node: term.expr, row, tables })
      rowValues.push(value)
    }
    evaluatedValues.push(rowValues)
  }

  // Create index array and sort it
  const indices = rows.map((_, i) => i)
  indices.sort((aIdx, bIdx) => {
    for (let termIdx = 0; termIdx < orderBy.length; termIdx++) {
      const term = orderBy[termIdx]
      const dir = term.direction
      const av = evaluatedValues[aIdx][termIdx]
      const bv = evaluatedValues[bIdx][termIdx]

      // Handle NULLS FIRST / NULLS LAST
      const aIsNull = av == null
      const bIsNull = bv == null

      if (aIsNull || bIsNull) {
        if (aIsNull && bIsNull) continue

        const nullsFirst = term.nulls === 'LAST' ? false : true

        if (aIsNull) {
          return nullsFirst ? -1 : 1
        } else {
          return nullsFirst ? 1 : -1
        }
      }

      const cmp = compareValues(av, bv)
      if (cmp !== 0) {
        return dir === 'DESC' ? -cmp : cmp
      }
    }
    return 0
  })

  // Return sorted rows
  return indices.map(i => rows[i])
}

/**
 * Applies ORDER BY sorting to rows
 *
 * @param {Record<string, any>[]} rows - the input rows
 * @param {OrderByItem[]} orderBy - the sort specifications
 * @param {Record<string, AsyncDataSource>} tables
 * @returns {Promise<Record<string, any>[]>} the sorted rows
 */
async function applyOrderBy(rows, orderBy, tables) {
  if (!orderBy.length) return rows

  // Pre-evaluate ORDER BY expressions for all rows
  /** @type {SqlPrimitive[][]} */
  const evaluatedValues = []
  for (const row of rows) {
    /** @type {SqlPrimitive[]} */
    const rowValues = []
    for (const term of orderBy) {
      const value = await evaluateExpr({ node: term.expr, row: asyncRow(row), tables })
      rowValues.push(value)
    }
    evaluatedValues.push(rowValues)
  }

  // Create index array and sort it
  const indices = rows.map((_, i) => i)
  indices.sort((aIdx, bIdx) => {
    for (let termIdx = 0; termIdx < orderBy.length; termIdx++) {
      const term = orderBy[termIdx]
      const dir = term.direction
      const av = evaluatedValues[aIdx][termIdx]
      const bv = evaluatedValues[bIdx][termIdx]

      // Handle NULLS FIRST / NULLS LAST
      const aIsNull = av == null
      const bIsNull = bv == null

      if (aIsNull || bIsNull) {
        if (aIsNull && bIsNull) continue

        const nullsFirst = term.nulls === 'LAST' ? false : true

        if (aIsNull) {
          return nullsFirst ? -1 : 1
        } else {
          return nullsFirst ? 1 : -1
        }
      }

      const cmp = compareValues(av, bv)
      if (cmp !== 0) {
        return dir === 'DESC' ? -cmp : cmp
      }
    }
    return 0
  })

  // Return sorted rows
  return indices.map(i => rows[i])
}

/**
 * Evaluates a select with a resolved FROM data source
 *
 * @param {SelectStatement} select - the parsed SQL AST
 * @param {AsyncDataSource} dataSource - the async data source
 * @param {Record<string, AsyncDataSource>} tables
 * @returns {AsyncGenerator<Record<string, any>>} async generator yielding result rows
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
 * @returns {AsyncGenerator<Record<string, any>>}
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

  for await (const row of dataSource.getRows()) {
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
    /** @type {Record<string, any>} */
    const outRow = {}
    for (const col of select.columns) {
      if (col.kind === 'star') {
        const keys = row.getKeys()
        for (const key of keys) {
          outRow[key] = row.getCell(key)
        }
      } else if (col.kind === 'derived') {
        const alias = col.alias ?? defaultDerivedAlias(col.expr)
        outRow[alias] = await evaluateExpr({ node: col.expr, row, tables })
      } else if (col.kind === 'aggregate') {
        throw new Error(
          'Aggregate functions require GROUP BY or will act on the whole dataset; add GROUP BY or remove aggregates'
        )
      }
    }

    // DISTINCT: skip duplicate rows
    if (seen) {
      const key = stableRowKey(outRow)
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
 * @returns {AsyncGenerator<Record<string, any>>}
 */
async function* evaluateBuffered(select, dataSource, tables, hasAggregate, useGrouping) {
  // Step 1: Collect all rows from data source
  /** @type {AsyncRow[]} */
  const working = []
  for await (const row of dataSource.getRows()) {
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
  /** @type {Record<string, any>[]} */
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
          keyParts.push(JSON.stringify(v))
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
      throw new Error('SELECT * with aggregate functions is not supported in this implementation')
    }

    for (const group of groups) {
      /** @type {Record<string, any>} */
      const resultRow = {}
      for (const col of select.columns) {
        if (col.kind === 'star') {
          const firstRow = group[0]
          if (firstRow) {
            const keys = firstRow.getKeys()
            for (const key of keys) {
              resultRow[key] = firstRow.getCell(key)
            }
          }
          continue
        }

        if (col.kind === 'derived') {
          const alias = col.alias ?? defaultDerivedAlias(col.expr)
          if (group.length > 0) {
            const value = await evaluateExpr({ node: col.expr, row: group[0], tables })
            resultRow[alias] = value
          } else {
            resultRow[alias] = undefined
          }
          continue
        }

        if (col.kind === 'aggregate') {
          const alias = col.alias ?? defaultAggregateAlias(col)
          const value = await evaluateAggregate(col, group)
          resultRow[alias] = value
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
    const sorted = await sortRowSources(filtered, select.orderBy, tables)

    // OPTIMIZATION: For non-DISTINCT queries, apply OFFSET/LIMIT before projection
    // to avoid reading expensive cells for rows that won't be in the final result
    let rowsToProject = sorted
    if (!select.distinct) {
      const start = select.offset ?? 0
      const end = select.limit ? start + select.limit : sorted.length
      rowsToProject = sorted.slice(start, end)
    }

    for (const row of rowsToProject) {
      /** @type {Record<string, any>} */
      const outRow = {}
      for (const col of select.columns) {
        if (col.kind === 'star') {
          const keys = row.getKeys()
          for (const key of keys) {
            outRow[key] = row.getCell(key)
          }
        } else if (col.kind === 'derived') {
          const alias = col.alias ?? defaultDerivedAlias(col.expr)
          const value = await evaluateExpr({ node: col.expr, row, tables })
          outRow[alias] = value
        }
      }
      projected.push(outRow)
    }
  }

  // Step 4: DISTINCT
  projected = applyDistinct(projected, select.distinct)

  // Step 5: ORDER BY (final sort for grouped queries)
  projected = await applyOrderBy(projected, select.orderBy, tables)

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
