import { evaluateExpr } from '../expression/evaluate.js'
import { missingClauseError } from '../parseErrors.js'
import { stringify } from './utils.js'
import { executePlan } from './execute.js'

/**
 * @import { AsyncCells, AsyncRow } from '../types.js'
 * @import { ExecuteContext, HashJoinNode, NestedLoopJoinNode, PositionalJoinNode, QueryPlan } from '../plan/types.js'
 */

/**
 * Executes a nested loop join operation
 *
 * @param {NestedLoopJoinNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
export async function* executeNestedLoopJoin(plan, context) {
  const { tables, functions, signal } = context
  const leftTable = getPlanAlias(plan.left)
  const rightTable = getPlanAlias(plan.right)

  if (!plan.condition) {
    throw missingClauseError({
      missing: 'ON condition',
      context: 'JOIN',
    })
  }

  // Buffer right rows
  /** @type {AsyncRow[]} */
  const rightRows = []
  for await (const row of executePlan(plan.right, context)) {
    if (signal?.aborted) return
    rightRows.push(row)
  }

  const rightCols = rightRows.length ? rightRows[0].columns : []
  const rightPrefixedCols = prefixColumns(rightCols, rightTable)

  /** @type {string[] | null} */
  let leftPrefixedCols = null
  /** @type {Set<AsyncRow> | null} */
  const matchedRightRows = plan.joinType === 'RIGHT' || plan.joinType === 'FULL' ? new Set() : null

  for await (const leftRow of executePlan(plan.left, context)) {
    if (signal?.aborted) break

    if (!leftPrefixedCols) {
      leftPrefixedCols = prefixColumns(leftRow.columns, leftTable)
    }

    let hasMatch = false

    for (const rightRow of rightRows) {
      const tempMerged = mergeRows(leftRow, rightRow, leftTable, rightTable)
      const matches = await evaluateExpr({
        node: plan.condition,
        row: tempMerged,
        tables,
        functions,
        signal,
      })

      if (matches) {
        hasMatch = true
        if (matchedRightRows) matchedRightRows.add(rightRow)
        yield tempMerged
      }
    }

    if (!hasMatch && (plan.joinType === 'LEFT' || plan.joinType === 'FULL')) {
      const nullRight = createNullRow(rightPrefixedCols)
      yield mergeRows(leftRow, nullRight, leftTable, rightTable)
    }
  }

  // Unmatched right rows for RIGHT/FULL joins
  if (matchedRightRows) {
    for (const rightRow of rightRows) {
      if (!matchedRightRows.has(rightRow)) {
        const nullLeft = createNullRow(leftPrefixedCols || [])
        yield mergeRows(nullLeft, rightRow, leftTable, rightTable)
      }
    }
  }
}

/**
 * Executes a positional join operation
 *
 * @param {PositionalJoinNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
export async function* executePositionalJoin(plan, context) {
  const { signal } = context
  const leftTable = getPlanAlias(plan.left)
  const rightTable = getPlanAlias(plan.right)

  // Buffer both sides (required for positional join)
  /** @type {AsyncRow[]} */
  const leftRows = []
  for await (const row of executePlan(plan.left, context)) {
    if (signal?.aborted) return
    leftRows.push(row)
  }

  /** @type {AsyncRow[]} */
  const rightRows = []
  for await (const row of executePlan(plan.right, context)) {
    if (signal?.aborted) return
    rightRows.push(row)
  }

  const maxLen = Math.max(leftRows.length, rightRows.length)
  const leftCols = leftRows[0]?.columns ?? []
  const rightCols = rightRows[0]?.columns ?? []
  const leftPrefixedCols = prefixColumns(leftCols, leftTable)
  const rightPrefixedCols = prefixColumns(rightCols, rightTable)

  for (let i = 0; i < maxLen; i++) {
    if (signal?.aborted) return
    const leftRow = leftRows[i] ?? createNullRow(leftPrefixedCols)
    const rightRow = rightRows[i] ?? createNullRow(rightPrefixedCols)
    yield mergeRows(leftRow, rightRow, leftTable, rightTable)
  }
}

/**
 * Executes a hash join operation
 *
 * @param {HashJoinNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
export async function* executeHashJoin(plan, context) {
  const { tables, functions, signal } = context
  const leftTable = getPlanAlias(plan.left)
  const rightTable = getPlanAlias(plan.right)

  // Buffer right rows and build hash map
  /** @type {AsyncRow[]} */
  const rightRows = []
  for await (const row of executePlan(plan.right, context)) {
    if (signal?.aborted) return
    rightRows.push(row)
  }

  /** @type {Map<string, AsyncRow[]>} */
  const hashMap = new Map()
  for (const rightRow of rightRows) {
    const keyValue = await evaluateExpr({
      node: plan.rightKey,
      row: rightRow,
      tables,
      functions,
      signal,
    })
    if (keyValue == null) continue
    const keyStr = stringify(keyValue)
    let bucket = hashMap.get(keyStr)
    if (!bucket) {
      bucket = []
      hashMap.set(keyStr, bucket)
    }
    bucket.push(rightRow)
  }

  // Get column info for NULL row generation
  const rightCols = rightRows.length ? rightRows[0].columns : []
  const rightPrefixedCols = prefixColumns(rightCols, rightTable)

  /** @type {string[] | null} */
  let leftPrefixedCols = null
  /** @type {Set<AsyncRow> | null} */
  const matchedRightRows = plan.joinType === 'RIGHT' || plan.joinType === 'FULL' ? new Set() : null

  // Probe phase: stream left rows
  for await (const leftRow of executePlan(plan.left, context)) {
    if (signal?.aborted) break

    if (!leftPrefixedCols) {
      leftPrefixedCols = prefixColumns(leftRow.columns, leftTable)
    }

    const keyValue = await evaluateExpr({
      node: plan.leftKey,
      row: leftRow,
      tables,
      functions,
      signal,
    })
    const keyStr = stringify(keyValue)
    const matchingRightRows = hashMap.get(keyStr)

    if (matchingRightRows?.length) {
      for (const rightRow of matchingRightRows) {
        if (matchedRightRows) matchedRightRows.add(rightRow)
        yield mergeRows(leftRow, rightRow, leftTable, rightTable)
      }
    } else if (plan.joinType === 'LEFT' || plan.joinType === 'FULL') {
      const nullRight = createNullRow(rightPrefixedCols)
      yield mergeRows(leftRow, nullRight, leftTable, rightTable)
    }
  }

  // Unmatched right rows for RIGHT/FULL joins
  if (matchedRightRows) {
    for (const rightRow of rightRows) {
      if (!matchedRightRows.has(rightRow)) {
        const nullLeft = createNullRow(leftPrefixedCols || [])
        yield mergeRows(nullLeft, rightRow, leftTable, rightTable)
      }
    }
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
 * Gets the table alias for a plan node.
 * For join nodes, returns the combined name of all joined tables.
 *
 * @param {QueryPlan} plan
 * @returns {string}
 */
function getPlanAlias(plan) {
  if (plan.type === 'Scan') {
    return plan.alias ?? plan.table
  }
  if (plan.type === 'SubqueryScan') {
    return plan.alias
  }
  if (plan.type === 'HashJoin' || plan.type === 'NestedLoopJoin' || plan.type === 'PositionalJoin') {
    const leftAlias = getPlanAlias(plan.left)
    const rightAlias = getPlanAlias(plan.right)
    return `${leftAlias}_${rightAlias}`
  }
  // All other node types have a child
  return getPlanAlias(plan.child)
}

/**
 * Prefixes column names with table alias, keeping already-prefixed columns as-is
 *
 * @param {string[]} cols
 * @param {string} table
 * @returns {string[]}
 */
function prefixColumns(cols, table) {
  return cols.flatMap(col => col.includes('.') ? [col] : [`${table}.${col}`, col])
}
