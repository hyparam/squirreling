import { evaluateExpr } from '../expression/evaluate.js'
import { keyify } from './utils.js'
import { executePlan } from './execute.js'

/**
 * @import { AsyncCells, AsyncRow, ExecuteContext } from '../types.js'
 * @import { HashJoinNode, NestedLoopJoinNode, PositionalJoinNode } from '../plan/types.js'
 */

/**
 * Executes a nested loop join operation
 *
 * @param {NestedLoopJoinNode} plan
 * @param {ExecuteContext} context
 * @yields {AsyncRow}
 */
export async function* executeNestedLoopJoin(plan, context) {
  const leftTable = plan.leftAlias
  const rightTable = plan.rightAlias

  // Buffer right rows
  /** @type {AsyncRow[]} */
  const rightRows = []
  for await (const row of executePlan({ plan: plan.right, context })) {
    if (context.signal?.aborted) return
    rightRows.push(row)
  }

  const rightPrefixedCols = rightRows.length ? prefixColumns(rightRows[0].columns, rightTable) : []

  /** @type {string[] | undefined} */
  let leftPrefixedCols = undefined
  /** @type {Set<AsyncRow> | undefined} */
  const matchedRightRows = plan.joinType === 'RIGHT' || plan.joinType === 'FULL' ? new Set() : undefined

  for await (const leftRow of executePlan({ plan: plan.left, context })) {
    if (context.signal?.aborted) break

    if (!leftPrefixedCols) {
      leftPrefixedCols = prefixColumns(leftRow.columns, leftTable)
    }

    let hasMatch = false

    for (const rightRow of rightRows) {
      const tempMerged = mergeRows(leftRow, rightRow, leftTable, rightTable)
      const matches = await evaluateExpr({
        node: plan.condition,
        row: tempMerged,
        context,
      })

      if (matches) {
        hasMatch = true
        matchedRightRows?.add(rightRow)
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
        const nullLeft = createNullRow(leftPrefixedCols ?? [])
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
  const leftTable = plan.leftAlias
  const rightTable = plan.rightAlias

  // Buffer both sides (required for positional join)
  /** @type {AsyncRow[]} */
  const leftRows = []
  for await (const row of executePlan({ plan: plan.left, context })) {
    if (signal?.aborted) return
    leftRows.push(row)
  }

  /** @type {AsyncRow[]} */
  const rightRows = []
  for await (const row of executePlan({ plan: plan.right, context })) {
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
  const leftTable = plan.leftAlias
  const rightTable = plan.rightAlias

  // Buffer right rows and build hash map
  /** @type {AsyncRow[]} */
  const rightRows = []
  for await (const row of executePlan({ plan: plan.right, context })) {
    if (context.signal?.aborted) return
    rightRows.push(row)
  }

  /** @type {Map<any, AsyncRow[]>} */
  const hashMap = new Map()
  for (const rightRow of rightRows) {
    const keyValue = await evaluateExpr({
      node: plan.rightKey,
      row: rightRow,
      context,
    })
    if (keyValue == null) continue
    const key = keyify(keyValue)
    let bucket = hashMap.get(key)
    if (!bucket) {
      bucket = []
      hashMap.set(key, bucket)
    }
    bucket.push(rightRow)
  }

  // Get column info for NULL row generation
  const rightCols = rightRows.length ? rightRows[0].columns : []
  const rightPrefixedCols = prefixColumns(rightCols, rightTable)

  /** @type {string[] | undefined} */
  let leftPrefixedCols
  /** @type {Set<AsyncRow> | undefined} */
  const matchedRightRows = plan.joinType === 'RIGHT' || plan.joinType === 'FULL' ? new Set() : undefined

  // Probe phase: stream left rows
  for await (const leftRow of executePlan({ plan: plan.left, context })) {
    if (context.signal?.aborted) break

    if (!leftPrefixedCols) {
      leftPrefixedCols = prefixColumns(leftRow.columns, leftTable)
    }

    const keyValue = await evaluateExpr({
      node: plan.leftKey,
      row: leftRow,
      context,
    })
    const key = keyify(keyValue)
    const matchingRightRows = hashMap.get(key)

    if (matchingRightRows?.length) {
      for (const rightRow of matchingRightRows) {
        matchedRightRows?.add(rightRow)
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
        const nullLeft = createNullRow(leftPrefixedCols ?? [])
        yield mergeRows(nullLeft, rightRow, leftTable, rightTable)
      }
    }
  }
}

/**
 * Creates a NULL-filled row with the given column names
 *
 * @param {string[]} columns
 * @returns {AsyncRow}
 */
function createNullRow(columns) {
  /** @type {AsyncCells} */
  const cells = {}
  for (const col of columns) {
    cells[col] = () => Promise.resolve(null)
  }
  return { columns, cells }
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
      columns.push(alias)
      cells[alias] = cell
    }
    // Also keep unqualified name for convenience
    columns.push(key)
    cells[key] = cell
  }

  // Add right table columns with prefix
  for (const [key, cell] of Object.entries(rightRow.cells)) {
    if (!key.includes('.')) {
      const alias = `${rightTable}.${key}`
      columns.push(alias)
      cells[alias] = cell
    }
    // Unqualified name (overwrites if same name exists in left table)
    columns.push(key)
    cells[key] = cell
  }

  return { columns, cells }
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
