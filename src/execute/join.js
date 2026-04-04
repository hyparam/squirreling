import { evaluateExpr } from '../expression/evaluate.js'
import { keyify } from './utils.js'
import { executePlan } from './execute.js'

/**
 * @import { AsyncCells, AsyncRow, ExecuteContext, QueryResults } from '../types.js'
 * @import { HashJoinNode, NestedLoopJoinNode, PositionalJoinNode } from '../plan/types.js'
 */

/**
 * Executes a nested loop join operation
 *
 * @param {NestedLoopJoinNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executeNestedLoopJoin(plan, context) {
  const left = executePlan({ plan: plan.left, context })
  const right = executePlan({ plan: plan.right, context })
  return {
    async *rows () {
      const leftTable = plan.leftAlias
      const rightTable = plan.rightAlias

      // Buffer right rows
      /** @type {AsyncRow[]} */
      const rightRows = []
      for await (const row of right.rows()) {
        if (context.signal?.aborted) return
        rightRows.push(row)
      }

      const rightCols = rightRows.length ? rightRows[0].columns : []

      /** @type {string[] | undefined} */
      let leftCols = undefined
      /** @type {Set<AsyncRow> | undefined} */
      const matchedRightRows = plan.joinType === 'RIGHT' || plan.joinType === 'FULL' ? new Set() : undefined

      for await (const leftRow of left.rows()) {
        if (context.signal?.aborted) break

        if (!leftCols) {
          leftCols = leftRow.columns
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
          const nullRight = createNullRow(rightCols)
          yield mergeRows(leftRow, nullRight, leftTable, rightTable)
        }
      }

      // Unmatched right rows for RIGHT/FULL joins
      if (matchedRightRows) {
        for (const rightRow of rightRows) {
          if (!matchedRightRows.has(rightRow)) {
            const nullLeft = createNullRow(leftCols ?? [])
            yield mergeRows(nullLeft, rightRow, leftTable, rightTable)
          }
        }
      }
    },
  }
}

/**
 * Executes a positional join operation
 *
 * @param {PositionalJoinNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executePositionalJoin(plan, context) {
  const left = executePlan({ plan: plan.left, context })
  const right = executePlan({ plan: plan.right, context })
  return {
    async *rows () {
      const { signal } = context
      const leftTable = plan.leftAlias
      const rightTable = plan.rightAlias

      // Buffer both sides (required for positional join)
      /** @type {AsyncRow[]} */
      const leftRows = []
      for await (const row of left.rows()) {
        if (signal?.aborted) return
        leftRows.push(row)
      }

      /** @type {AsyncRow[]} */
      const rightRows = []
      for await (const row of right.rows()) {
        if (signal?.aborted) return
        rightRows.push(row)
      }

      const maxLen = Math.max(leftRows.length, rightRows.length)
      const leftCols = leftRows[0]?.columns ?? []
      const rightCols = rightRows[0]?.columns ?? []

      for (let i = 0; i < maxLen; i++) {
        if (signal?.aborted) return
        const leftRow = leftRows[i] ?? createNullRow(leftCols)
        const rightRow = rightRows[i] ?? createNullRow(rightCols)
        yield mergeRows(leftRow, rightRow, leftTable, rightTable)
      }
    },
  }
}

/**
 * Executes a hash join operation
 *
 * @param {HashJoinNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
export function executeHashJoin(plan, context) {
  const left = executePlan({ plan: plan.left, context })
  const right = executePlan({ plan: plan.right, context })
  return {
    async *rows () {
      const leftTable = plan.leftAlias
      const rightTable = plan.rightAlias

      // Buffer right rows and build hash map
      /** @type {AsyncRow[]} */
      const rightRows = []
      for await (const row of right.rows()) {
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

      /** @type {string[] | undefined} */
      let leftCols
      /** @type {Set<AsyncRow> | undefined} */
      const matchedRightRows = plan.joinType === 'RIGHT' || plan.joinType === 'FULL' ? new Set() : undefined

      // Probe phase: stream left rows
      for await (const leftRow of left.rows()) {
        if (context.signal?.aborted) break

        if (!leftCols) {
          leftCols = leftRow.columns
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
          const nullRight = createNullRow(rightCols)
          yield mergeRows(leftRow, nullRight, leftTable, rightTable)
        }
      }

      // Unmatched right rows for RIGHT/FULL joins
      if (matchedRightRows) {
        for (const rightRow of rightRows) {
          if (!matchedRightRows.has(rightRow)) {
            const nullLeft = createNullRow(leftCols ?? [])
            yield mergeRows(nullLeft, rightRow, leftTable, rightTable)
          }
        }
      }
    },
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
    const alias = key.includes('.') ? key : `${leftTable}.${key}`
    columns.push(alias)
    cells[alias] = cell
  }

  // Add right table columns with prefix
  for (const [key, cell] of Object.entries(rightRow.cells)) {
    const alias = key.includes('.') ? key : `${rightTable}.${key}`
    columns.push(alias)
    cells[alias] = cell
  }

  return { columns, cells }
}
