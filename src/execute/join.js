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
  // Collect left rows (JOINs require buffering)
  /** @type {AsyncRow[]} */
  let leftRows = []
  for await (const row of leftSource.getRows()) {
    leftRows.push(row)
  }

  let currentLeftTable = leftTableName

  // Process each JOIN sequentially
  for (const join of joins) {
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

    leftRows = await hashJoin({
      leftRows,
      rightRows,
      join,
      leftTable: currentLeftTable,
      rightTable: rightTableName,
      tables,
    })

    // After join, the "left" table for the next join includes all joined tables
    currentLeftTable = `${currentLeftTable}_${rightTableName}`
  }

  // Convert back to AsyncDataSource
  return {
    async *getRows() {
      for (const row of leftRows) {
        yield row
      }
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
 * Performs a hash join between left and right row sets
 *
 * @param {Object} params
 * @param {AsyncRow[]} params.leftRows - rows from left table
 * @param {AsyncRow[]} params.rightRows - rows from right table
 * @param {JoinClause} params.join - join specification
 * @param {string} params.leftTable - name of left table (for column prefixing)
 * @param {string} params.rightTable - name of right table (for column prefixing, may be alias)
 * @param {Record<string, AsyncDataSource>} params.tables - all tables for expression evaluation
 * @returns {Promise<AsyncRow[]>} joined rows
 */
async function hashJoin({ leftRows, rightRows, join, leftTable, rightTable, tables }) {
  const { joinType, on: onCondition } = join

  if (!onCondition) {
    throw new Error('JOIN requires ON condition')
  }

  // Extract join keys from ON condition
  const keys = extractJoinKeys(onCondition)

  /** @type {AsyncRow[]} */
  const result = []

  // Get column names for NULL row generation
  const leftCols = leftRows.length ? Object.keys(leftRows[0]) : []
  const rightCols = rightRows.length ? Object.keys(rightRows[0]) : []

  // Generate prefixed column names for NULL rows
  const leftPrefixedCols = leftCols.flatMap(col =>
    col.includes('.') ? [col] : [`${leftTable}.${col}`, col]
  )
  const rightPrefixedCols = rightCols.flatMap(col =>
    col.includes('.') ? [col] : [`${rightTable}.${col}`, col]
  )

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

    // Track which right rows matched (for FULL join)
    /** @type {Set<AsyncRow>} */
    const matchedRightRows = new Set()

    // PROBE PHASE: Look up each left row
    for (const leftRow of leftRows) {
      const keyValue = await evaluateExpr({ node: keys.leftKey, row: leftRow, tables })
      const keyStr = JSON.stringify(keyValue)

      const matchingRightRows = hashMap.get(keyStr)

      if (matchingRightRows && matchingRightRows.length > 0) {
        // Found matches - emit merged row for each
        for (const rightRow of matchingRightRows) {
          matchedRightRows.add(rightRow)
          result.push(mergeRows(leftRow, rightRow, leftTable, rightTable))
        }
      } else if (joinType === 'LEFT' || joinType === 'FULL') {
        // No match but LEFT/FULL join - emit left row with NULL right columns
        const nullRight = createNullRow(rightPrefixedCols)
        result.push(mergeRows(leftRow, nullRight, leftTable, rightTable))
      }
      // INNER join with no match: don't emit anything
    }

    // UNMATCHED PHASE: Handle unmatched right rows for RIGHT/FULL joins
    if (joinType === 'RIGHT' || joinType === 'FULL') {
      for (const rightRow of rightRows) {
        if (!matchedRightRows.has(rightRow)) {
          const nullLeft = createNullRow(leftPrefixedCols)
          result.push(mergeRows(nullLeft, rightRow, leftTable, rightTable))
        }
      }
    }
  } else {
    // Fallback to nested loop for complex ON conditions
    // Track which right rows matched (for FULL join)
    /** @type {Set<AsyncRow>} */
    const matchedRightRows = new Set()

    for (const leftRow of leftRows) {
      let hasMatch = false

      for (const rightRow of rightRows) {
        // Create temporary merged row to evaluate ON condition
        const tempMerged = mergeRows(leftRow, rightRow, leftTable, rightTable)
        const matches = await evaluateExpr({ node: onCondition, row: tempMerged, tables })

        if (matches) {
          hasMatch = true
          matchedRightRows.add(rightRow)
          result.push(tempMerged)
        }
      }

      if (!hasMatch && (joinType === 'LEFT' || joinType === 'FULL')) {
        const nullRight = createNullRow(rightPrefixedCols)
        result.push(mergeRows(leftRow, nullRight, leftTable, rightTable))
      }
    }

    // Handle unmatched right rows for RIGHT/FULL joins
    if (joinType === 'RIGHT' || joinType === 'FULL') {
      for (const rightRow of rightRows) {
        if (!matchedRightRows.has(rightRow)) {
          const nullLeft = createNullRow(leftPrefixedCols)
          result.push(mergeRows(nullLeft, rightRow, leftTable, rightTable))
        }
      }
    }
  }

  return result
}
