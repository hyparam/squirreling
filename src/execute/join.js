import { missingClauseError } from '../parseErrors.js'
import { tableNotFoundError } from '../executionErrors.js'
import { evaluateExpr } from './expression.js'
import { stringify } from './utils.js'

/**
 * @import { AsyncRow, AsyncDataSource, JoinClause, ExprNode, AsyncCells, UserDefinedFunction } from '../types.js'
 */

/**
 * Executes JOIN operations against a base data source
 *
 * @param {Object} options
 * @param {AsyncDataSource} options.leftSource - the left side of the join (FROM table)
 * @param {JoinClause[]} options.joins - array of join clauses to execute
 * @param {string} options.leftTable - name of the left table (for column prefixing)
 * @param {Record<string, AsyncDataSource>} options.tables - all available tables
 * @param {Record<string, UserDefinedFunction>} [options.functions]
 * @returns {Promise<AsyncDataSource>} data source yielding joined rows
 */
export async function executeJoins({ leftSource, joins, leftTable, tables, functions }) {
  let currentLeftTable = leftTable

  // Single join optimization: stream left rows without buffering
  if (joins.length === 1) {
    const join = joins[0]
    const rightSource = tables[join.table]
    if (rightSource === undefined) {
      throw tableNotFoundError({ tableName: join.table })
    }

    // Buffer right rows for hash index (required for hash join)
    /** @type {AsyncRow[]} */
    const rightRows = []
    for await (const row of rightSource.scan({})) {
      rightRows.push(row)
    }

    // Use alias for column prefixing if present
    const rightTable = join.alias ?? join.table

    // Return streaming data source - left rows stream through without buffering
    return {
      async *scan(options) {
        const { signal } = options
        if (join.joinType === 'POSITIONAL') {
          yield* positionalJoin({
            leftRows: leftSource.scan(options),
            rightRows,
            leftTable: currentLeftTable,
            rightTable,
            signal,
          })
        } else {
          yield* hashJoin({
            leftRows: leftSource.scan(options), // Stream directly, not buffered
            rightRows,
            join,
            leftTable: currentLeftTable,
            rightTable,
            tables,
            functions,
            signal,
          })
        }
      },
    }
  }

  // Multiple joins: buffer intermediate results, stream final join
  /** @type {AsyncRow[]} */
  let leftRows = []
  for await (const row of leftSource.scan({})) {
    leftRows.push(row)
  }

  // Process all but the last join, buffering intermediate results
  for (let i = 0; i < joins.length - 1; i++) {
    const join = joins[i]
    const rightSource = tables[join.table]
    if (rightSource === undefined) {
      throw tableNotFoundError({ tableName: join.table })
    }

    /** @type {AsyncRow[]} */
    const rightRows = []
    for await (const row of rightSource.scan({})) {
      rightRows.push(row)
    }

    // Use alias for column prefixing if present
    const rightTable = join.alias ?? join.table

    // Collect intermediate results into array for next join
    /** @type {AsyncRow[]} */
    const newLeftRows = []
    const joined = join.joinType === 'POSITIONAL'
      ? positionalJoin({
        leftRows,
        rightRows,
        leftTable: currentLeftTable,
        rightTable,
      })
      : hashJoin({
        leftRows,
        rightRows,
        join,
        leftTable: currentLeftTable,
        rightTable,
        tables,
        functions,
      })
    for await (const row of joined) {
      newLeftRows.push(row)
    }
    leftRows = newLeftRows

    // After join, the "left" table for the next join includes all joined tables
    currentLeftTable = `${currentLeftTable}_${rightTable}`
  }

  // Final join: stream the results
  const join = joins[joins.length - 1]
  const rightSource = tables[join.table]
  if (rightSource === undefined) {
    throw tableNotFoundError({ tableName: join.table })
  }

  /** @type {AsyncRow[]} */
  const rightRows = []
  for await (const row of rightSource.scan({})) {
    rightRows.push(row)
  }

  // Use alias for column prefixing if present
  const rightTable = join.alias ?? join.table

  return {
    async *scan(options) {
      const { signal } = options
      if (join.joinType === 'POSITIONAL') {
        yield* positionalJoin({
          leftRows,
          rightRows,
          leftTable: currentLeftTable,
          rightTable,
          signal,
        })
      } else {
        yield* hashJoin({
          leftRows,
          rightRows,
          join,
          leftTable: currentLeftTable,
          rightTable,
          tables,
          functions,
          signal,
        })
      }
    },
  }
}

/**
 * Checks if an expression references a specific table.
 * Returns true if the expression is an identifier prefixed with the table name.
 *
 * @param {ExprNode} expr
 * @param {string} tableName
 * @returns {boolean}
 */
function exprReferencesTable(expr, tableName) {
  return expr.type === 'identifier' && expr.name.startsWith(`${tableName}.`)
}

/**
 * Extracts the join key expressions from an ON condition.
 * Handles both `left.col = right.col` and `right.col = left.col` orderings.
 *
 * @param {ExprNode} onCondition
 * @param {string} leftTable
 * @param {string} rightTable
 * @returns {{ leftKey: ExprNode, rightKey: ExprNode } | undefined}
 */
function extractJoinKeys(onCondition, leftTable, rightTable) {
  if (onCondition.type === 'binary' && onCondition.op === '=') {
    const { left, right } = onCondition

    // Check if keys are swapped (right table referenced in left position)
    const leftRefsRight = exprReferencesTable(left, rightTable)
    const rightRefsLeft = exprReferencesTable(right, leftTable)

    if (leftRefsRight && rightRefsLeft) {
      // Keys are swapped, return them in correct order
      return { leftKey: right, rightKey: left }
    }

    // Default: assume left operand is for left table
    return { leftKey: left, rightKey: right }
  }
}

/**
 * Creates a NULL-filled row with the given column names
 *
 * @param {string[]} columnNames
 * @returns {AsyncRow}
 */
function createNullRow(columnNames) {
  /** @type {AsyncCells} */
  const cells = {}
  for (const col of columnNames) {
    cells[col] = () => Promise.resolve(null)
  }
  return { columns: columnNames, cells }
}

/**
 * Merges two rows into one, prefixing columns with table names
 *
 * @param {AsyncRow} leftRow
 * @param {AsyncRow} rightRow
 * @param {string} leftTable
 * @param {string} rightTable
 * @returns {AsyncRow}
 */
function mergeRows(leftRow, rightRow, leftTable, rightTable) {
  const columns = []
  /** @type {AsyncCells} */
  const cells = {}

  // Add left table columns with prefix
  for (const [key, cell] of Object.entries(leftRow.cells)) {
    // Skip already-prefixed keys (from previous joins)
    if (!key.includes('.')) {
      const alias = `${leftTable}.${key}`
      cells[alias] = cell
    }
    // Also keep unqualified name for convenience
    columns.push(key)
    cells[key] = cell
  }

  // Add right table columns with prefix
  for (const [key, cell] of Object.entries(rightRow.cells)) {
    if (!key.includes('.')) {
      cells[`${rightTable}.${key}`] = cell
    } else {
      cells[key] = cell
    }
    // Unqualified name (overwrites if same name exists in left table)
    columns.push(key)
    cells[key] = cell
  }

  return { columns, cells }
}

/**
 * Performs a positional join between left and right row sets.
 * Matches rows by their index position (row 0 with row 0, row 1 with row 1, etc.).
 * When tables have different lengths, the shorter table is padded with NULLs.
 *
 * @param {Object} params
 * @param {AsyncIterable<AsyncRow>|AsyncRow[]} params.leftRows - rows from left table
 * @param {AsyncRow[]} params.rightRows - rows from right table (must be buffered)
 * @param {string} params.leftTable - name of left table (for column prefixing)
 * @param {string} params.rightTable - name of right table (for column prefixing, may be alias)
 * @param {AbortSignal} [params.signal] - abort signal for cancellation
 * @yields {AsyncRow} joined rows
 */
async function* positionalJoin({ leftRows, rightRows, leftTable, rightTable, signal }) {
  // Buffer left rows if streaming
  /** @type {AsyncRow[]} */
  const leftArr = []
  for await (const row of leftRows) {
    if (signal?.aborted) return
    leftArr.push(row)
  }

  const maxLen = Math.max(leftArr.length, rightRows.length)

  // Get column info for NULL row creation
  const leftCols = leftArr[0]?.columns ?? []
  const rightCols = rightRows[0]?.columns ?? []
  const leftPrefixedCols = leftCols.flatMap(col =>
    col.includes('.') ? [col] : [`${leftTable}.${col}`, col]
  )
  const rightPrefixedCols = rightCols.flatMap(col =>
    col.includes('.') ? [col] : [`${rightTable}.${col}`, col]
  )

  for (let i = 0; i < maxLen; i++) {
    if (signal?.aborted) return
    const leftRow = leftArr[i] ?? createNullRow(leftPrefixedCols)
    const rightRow = rightRows[i] ?? createNullRow(rightPrefixedCols)
    yield mergeRows(leftRow, rightRow, leftTable, rightTable)
  }
}

/**
 * Performs a hash join between left and right row sets (streaming).
 * Yields rows as they are found instead of buffering all results.
 *
 * @param {Object} params
 * @param {AsyncIterable<AsyncRow>|AsyncRow[]} params.leftRows - rows from left table (can stream)
 * @param {AsyncRow[]} params.rightRows - rows from right table (must be buffered for hash index)
 * @param {JoinClause} params.join - join specification
 * @param {string} params.leftTable - name of left table (for column prefixing)
 * @param {string} params.rightTable - name of right table (for column prefixing, may be alias)
 * @param {Record<string, AsyncDataSource>} params.tables - all tables for expression evaluation
 * @param {Record<string, UserDefinedFunction>} [params.functions]
 * @param {AbortSignal} [params.signal] - abort signal for cancellation
 * @yields {AsyncRow} joined rows
 */
async function* hashJoin({ leftRows, rightRows, join, leftTable, rightTable, tables, functions, signal }) {
  const { joinType, on: onCondition } = join

  if (!onCondition) {
    throw missingClauseError({
      missing: 'ON condition',
      context: 'JOIN',
    })
  }

  const keys = extractJoinKeys(onCondition, leftTable, rightTable)

  // Get column names for NULL row generation (right side is always buffered)
  const rightCols = rightRows.length ? rightRows[0].columns : []
  const rightPrefixedCols = rightCols.flatMap(col =>
    col.includes('.') ? [col] : [`${rightTable}.${col}`, col]
  )

  // Track left column info - captured from first row during iteration
  /** @type {string[]|null} */
  let leftPrefixedCols = null

  if (keys) {
    // Hash join: build hash map on right table
    /** @type {Map<string, AsyncRow[]>} */
    const hashMap = new Map()

    // BUILD PHASE: Index right rows by join key
    // Skip null keys - SQL semantics: NULL != NULL
    for (const rightRow of rightRows) {
      const keyValue = await evaluateExpr({ node: keys.rightKey, row: rightRow, tables, functions })
      if (keyValue == null) continue // NULL keys never match
      const keyStr = stringify(keyValue)
      let bucket = hashMap.get(keyStr)
      if (!bucket) {
        bucket = []
        hashMap.set(keyStr, bucket)
      }
      bucket.push(rightRow)
    }

    // Track which right rows matched (only needed for RIGHT/FULL joins)
    /** @type {Set<AsyncRow>|null} */
    const matchedRightRows = joinType === 'RIGHT' || joinType === 'FULL' ? new Set() : null

    // PROBE PHASE: Stream through left rows, yield matches immediately
    for await (const leftRow of leftRows) {
      if (signal?.aborted) break
      // Capture left column info from first row (for NULL row generation)
      if (!leftPrefixedCols) {
        leftPrefixedCols = leftRow.columns.flatMap(col =>
          col.includes('.') ? [col] : [`${leftTable}.${col}`, col]
        )
      }

      const keyValue = await evaluateExpr({ node: keys.leftKey, row: leftRow, tables, functions })
      const keyStr = stringify(keyValue)

      const matchingRightRows = hashMap.get(keyStr)

      if (matchingRightRows && matchingRightRows.length > 0) {
        for (const rightRow of matchingRightRows) {
          if (matchedRightRows) matchedRightRows.add(rightRow)
          yield mergeRows(leftRow, rightRow, leftTable, rightTable)
        }
      } else if (joinType === 'LEFT' || joinType === 'FULL') {
        const nullRight = createNullRow(rightPrefixedCols)
        yield mergeRows(leftRow, nullRight, leftTable, rightTable)
      }
      // INNER join with no match: don't yield anything
    }

    // UNMATCHED PHASE: Handle unmatched right rows for RIGHT/FULL joins
    if (matchedRightRows) {
      for (const rightRow of rightRows) {
        if (!matchedRightRows.has(rightRow)) {
          // Use empty array if left table was empty (no rows to derive columns from)
          const nullLeft = createNullRow(leftPrefixedCols || [])
          yield mergeRows(nullLeft, rightRow, leftTable, rightTable)
        }
      }
    }
  } else {
    // Fallback to nested loop for complex ON conditions
    // Left rows stream through, right rows are iterated for each left row
    /** @type {Set<AsyncRow>|null} */
    const matchedRightRows = joinType === 'RIGHT' || joinType === 'FULL' ? new Set() : null

    for await (const leftRow of leftRows) {
      if (signal?.aborted) break
      // Capture left column info from first row (for NULL row generation)
      if (!leftPrefixedCols) {
        leftPrefixedCols = leftRow.columns.flatMap(col =>
          col.includes('.') ? [col] : [`${leftTable}.${col}`, col]
        )
      }

      let hasMatch = false

      for (const rightRow of rightRows) {
        const tempMerged = mergeRows(leftRow, rightRow, leftTable, rightTable)
        const matches = await evaluateExpr({ node: onCondition, row: tempMerged, tables, functions })

        if (matches) {
          hasMatch = true
          if (matchedRightRows) matchedRightRows.add(rightRow)
          yield tempMerged
        }
      }

      if (!hasMatch && (joinType === 'LEFT' || joinType === 'FULL')) {
        const nullRight = createNullRow(rightPrefixedCols)
        yield mergeRows(leftRow, nullRight, leftTable, rightTable)
      }
    }

    // Handle unmatched right rows for RIGHT/FULL joins
    if (matchedRightRows) {
      for (const rightRow of rightRows) {
        if (!matchedRightRows.has(rightRow)) {
          // Use empty array if left table was empty (no rows to derive columns from)
          const nullLeft = createNullRow(leftPrefixedCols || [])
          yield mergeRows(nullLeft, rightRow, leftTable, rightTable)
        }
      }
    }
  }
}
