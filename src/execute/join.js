import { evaluateExpr } from './expression.js'

/**
 * @import { AsyncRow, AsyncDataSource, JoinClause, ExprNode } from '../types.js'
 */

/**
 * Executes JOIN operations against a base data source
 *
 * @param {AsyncDataSource} leftSource - the left side of the join (FROM table)
 * @param {JoinClause[]} joins - array of join clauses to execute
 * @param {string} leftTableName - name of the left table (for column prefixing)
 * @param {Record<string, AsyncDataSource>} tables - all available tables
 * @returns {Promise<AsyncDataSource>} data source yielding joined rows
 */
export async function executeJoins(leftSource, joins, leftTableName, tables) {
  let currentLeftTable = leftTableName

  // Single join optimization: stream left rows without buffering
  if (joins.length === 1) {
    const join = joins[0]
    const rightSource = tables[join.table]
    if (rightSource === undefined) {
      throw new Error(`Table "${join.table}" not found`)
    }

    // Buffer right rows for hash index (required for hash join)
    /** @type {AsyncRow[]} */
    const rightRows = []
    for await (const row of rightSource.getRows()) {
      rightRows.push(row)
    }

    // Use alias for column prefixing if present
    const rightTableName = join.alias ?? join.table

    // Return streaming data source - left rows stream through without buffering
    return {
      async *getRows() {
        yield* hashJoin({
          leftRows: leftSource.getRows(), // Stream directly, not buffered
          rightRows,
          join,
          leftTable: currentLeftTable,
          rightTable: rightTableName,
          tables,
        })
      },
    }
  }

  // Multiple joins: buffer intermediate results, stream final join
  /** @type {AsyncRow[]} */
  let leftRows = []
  for await (const row of leftSource.getRows()) {
    leftRows.push(row)
  }

  // Process all but the last join, buffering intermediate results
  for (let i = 0; i < joins.length - 1; i++) {
    const join = joins[i]
    const rightSource = tables[join.table]
    if (rightSource === undefined) {
      throw new Error(`Table "${join.table}" not found`)
    }

    /** @type {AsyncRow[]} */
    const rightRows = []
    for await (const row of rightSource.getRows()) {
      rightRows.push(row)
    }

    // Use alias for column prefixing if present
    const rightTableName = join.alias ?? join.table

    // Collect intermediate results into array for next join
    /** @type {AsyncRow[]} */
    const newLeftRows = []
    const joined = hashJoin({
      leftRows,
      rightRows,
      join,
      leftTable: currentLeftTable,
      rightTable: rightTableName,
      tables,
    })
    for await (const row of joined) {
      newLeftRows.push(row)
    }
    leftRows = newLeftRows

    // After join, the "left" table for the next join includes all joined tables
    currentLeftTable = `${currentLeftTable}_${rightTableName}`
  }

  // Final join: stream the results
  const lastJoin = joins[joins.length - 1]
  const rightSource = tables[lastJoin.table]
  if (rightSource === undefined) {
    throw new Error(`Table "${lastJoin.table}" not found`)
  }

  /** @type {AsyncRow[]} */
  const rightRows = []
  for await (const row of rightSource.getRows()) {
    rightRows.push(row)
  }

  // Use alias for column prefixing if present
  const lastRightTableName = lastJoin.alias ?? lastJoin.table

  return {
    async *getRows() {
      yield* hashJoin({
        leftRows,
        rightRows,
        join: lastJoin,
        leftTable: currentLeftTable,
        rightTable: lastRightTableName,
        tables,
      })
    },
  }
}

/**
 * Extracts the join key expressions from an ON condition.
 * Assumes the ON condition is an equality comparison (a = b).
 *
 * @param {ExprNode} onCondition
 * @returns {{ leftKey: ExprNode, rightKey: ExprNode } | null}
 */
function extractJoinKeys(onCondition) {
  if (onCondition.type === 'binary' && onCondition.op === '=') {
    return {
      leftKey: onCondition.left,
      rightKey: onCondition.right,
    }
  }
  return null
}

/**
 * Creates a NULL-filled row with the given column names
 *
 * @param {string[]} columnNames
 * @returns {AsyncRow}
 */
function createNullRow(columnNames) {
  /** @type {AsyncRow} */
  const row = {}
  for (const col of columnNames) {
    row[col] = () => Promise.resolve(null)
  }
  return row
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
  /** @type {AsyncRow} */
  const merged = {}

  // Add left table columns with prefix
  for (const [key, cell] of Object.entries(leftRow)) {
    // Skip already-prefixed keys (from previous joins)
    if (!key.includes('.')) {
      merged[`${leftTable}.${key}`] = cell
    } else {
      merged[key] = cell
    }
    // Also keep unqualified name for convenience (may be overwritten if ambiguous)
    merged[key] = cell
  }

  // Add right table columns with prefix
  for (const [key, cell] of Object.entries(rightRow)) {
    if (!key.includes('.')) {
      merged[`${rightTable}.${key}`] = cell
    } else {
      merged[key] = cell
    }
    // Unqualified name (overwrites if same name exists in left table)
    merged[key] = cell
  }

  return merged
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
 * @yields {AsyncRow} joined rows
 */
async function* hashJoin({ leftRows, rightRows, join, leftTable, rightTable, tables }) {
  const { joinType, on: onCondition } = join

  if (!onCondition) {
    throw new Error('JOIN requires ON condition')
  }

  const keys = extractJoinKeys(onCondition)

  // Get column names for NULL row generation (right side is always buffered)
  const rightCols = rightRows.length ? Object.keys(rightRows[0]) : []
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
      const keyValue = await evaluateExpr({ node: keys.rightKey, row: rightRow, tables })
      if (keyValue == null) continue // NULL keys never match
      const keyStr = JSON.stringify(keyValue)

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
      // Capture left column info from first row (for NULL row generation)
      if (!leftPrefixedCols) {
        const leftCols = Object.keys(leftRow)
        leftPrefixedCols = leftCols.flatMap(col =>
          col.includes('.') ? [col] : [`${leftTable}.${col}`, col]
        )
      }

      const keyValue = await evaluateExpr({ node: keys.leftKey, row: leftRow, tables })
      const keyStr = JSON.stringify(keyValue)

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
      // Capture left column info from first row (for NULL row generation)
      if (!leftPrefixedCols) {
        const leftCols = Object.keys(leftRow)
        leftPrefixedCols = leftCols.flatMap(col =>
          col.includes('.') ? [col] : [`${leftTable}.${col}`, col]
        )
      }

      let hasMatch = false

      for (const rightRow of rightRows) {
        const tempMerged = mergeRows(leftRow, rightRow, leftTable, rightTable)
        const matches = await evaluateExpr({ node: onCondition, row: tempMerged, tables })

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
