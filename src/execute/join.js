import { evaluateExpr } from '../expression/evaluate.js'
import { materializeRow } from './cells.js'
import { keyify, maxBounds } from './utils.js'
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
  if (plan.lateral) {
    return executeLateralJoin(plan, context)
  }
  const left = executePlan({ plan: plan.left, context })
  const right = executePlan({ plan: plan.right, context })
  return {
    columns: mergeColumnNames(left.columns, right.columns, plan.leftAlias, plan.rightAlias),
    async *rows() {
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
 * Executes a LATERAL nested loop join — the right side is re-executed per
 * left row with the left row available as `context.outerRow`.
 *
 * @param {NestedLoopJoinNode} plan
 * @param {ExecuteContext} context
 * @returns {QueryResults}
 */
function executeLateralJoin(plan, context) {
  const left = executePlan({ plan: plan.left, context })
  // Right columns are known statically for table functions (the common case).
  const rightCols = plan.right.type === 'TableFunction' ? plan.right.columnNames : []
  return {
    columns: mergeColumnNames(left.columns, rightCols, plan.leftAlias, plan.rightAlias),
    async *rows() {
      const leftTable = plan.leftAlias
      const rightTable = plan.rightAlias

      for await (const leftRow of left.rows()) {
        if (context.signal?.aborted) return

        // When nested inside a correlated subquery, preserve the enclosing
        // outer row so UNNEST args can reference its columns (e.g. o.arr).
        const nestedOuter = context.outerRow
          ? mergeOuterRows(context.outerRow, leftRow, leftTable)
          : leftRow
        const subContext = { ...context, outerRow: nestedOuter }
        const right = executePlan({ plan: plan.right, context: subContext })

        let hasMatch = false
        for await (const rightRow of right.rows()) {
          if (context.signal?.aborted) return
          const merged = mergeRows(leftRow, rightRow, leftTable, rightTable)
          const matches = plan.condition === undefined
            ? true
            : await evaluateExpr({ node: plan.condition, row: merged, context })
          if (matches) {
            hasMatch = true
            yield merged
          }
        }

        if (!hasMatch && plan.joinType === 'LEFT') {
          const nullRight = createNullRow(rightCols)
          yield mergeRows(leftRow, nullRight, leftTable, rightTable)
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
  const numRows = left.numRows !== undefined && right.numRows !== undefined
    ? Math.max(left.numRows, right.numRows) : undefined
  return {
    columns: mergeColumnNames(left.columns, right.columns, plan.leftAlias, plan.rightAlias),
    numRows,
    maxRows: maxBounds(left.maxRows, right.maxRows),
    async *rows() {
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
    columns: mergeColumnNames(left.columns, right.columns, plan.leftAlias, plan.rightAlias),
    async *rows() {
      const leftTable = plan.leftAlias
      const rightTable = plan.rightAlias
      const { leftKeys, rightKeys, residual } = plan
      const keepUnmatchedRight = plan.joinType === 'RIGHT' || plan.joinType === 'FULL'

      // Build phase. Stream the right side directly into the hash map and, in
      // the same pass, materialize each row — replacing any thunk cells with
      // bare values — and drop the original AsyncRow. This severs any
      // upstream retention (parquet pages, arrow buffers, struct accessors)
      // the source's cell closures may have pinned per row, which is what
      // otherwise OOMs wide-schema build sides. Rows whose cells are already
      // bare values pass through unchanged.
      /** @type {AsyncRow[]} */
      const rightRows = []
      /** @type {Map<string | number | bigint | boolean, AsyncRow[]>} */
      const hashMap = new Map()
      // Used to null-pad unmatched left rows for LEFT/FULL. Default to the
      // right child's advertised columns so the column list is correct even
      // when the build side is empty or every row has a NULL key (LEFT joins
      // skip materializing those, so we can't read them off a slim row).
      let rightCols = right.columns
      for await (const row of right.rows()) {
        if (context.signal?.aborted) return
        const keyValues = await Promise.all(
          rightKeys.map(node => evaluateExpr({ node, row, context }))
        )
        // SQL semantics: NULL never equals anything, so a row with any NULL
        // join key is excluded from the hash table. INNER/LEFT joins don't
        // need to retain those rows at all.
        const nullKey = keyValues.some(v => v == null)
        if (nullKey && !keepUnmatchedRight) continue

        const slim = await materializeRow(row)
        rightCols = slim.columns
        if (keepUnmatchedRight) rightRows.push(slim)
        if (nullKey) continue

        const key = keyify(...keyValues)
        let bucket = hashMap.get(key)
        if (!bucket) {
          bucket = []
          hashMap.set(key, bucket)
        }
        bucket.push(slim)
      }

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

        const keyValues = await Promise.all(
          leftKeys.map(node => evaluateExpr({ node, row: leftRow, context }))
        )
        let matched = false
        if (!keyValues.some(v => v == null)) {
          const key = keyify(...keyValues)
          const candidates = hashMap.get(key)
          if (candidates?.length) {
            for (const rightRow of candidates) {
              const merged = mergeRows(leftRow, rightRow, leftTable, rightTable)
              if (residual) {
                const ok = await evaluateExpr({ node: residual, row: merged, context })
                if (!ok) continue
              }
              matched = true
              matchedRightRows?.add(rightRow)
              yield merged
            }
          }
        }

        if (!matched && (plan.joinType === 'LEFT' || plan.joinType === 'FULL')) {
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
 * Merges an enclosing correlated outer row with a lateral join's left row.
 * Outer cells are kept as-is; left cells are added under a qualified alias
 * so qualified refs on either side resolve unambiguously.
 *
 * @param {AsyncRow} outerRow
 * @param {AsyncRow} leftRow
 * @param {string} leftTable
 * @returns {AsyncRow}
 */
function mergeOuterRows(outerRow, leftRow, leftTable) {
  const columns = [...outerRow.columns]
  /** @type {AsyncCells} */
  const cells = { ...outerRow.cells }
  for (const [key, cell] of Object.entries(leftRow.cells)) {
    const alias = key.includes('.') ? key : `${leftTable}.${key}`
    if (!(alias in cells)) columns.push(alias)
    cells[alias] = cell
  }
  return { columns, cells }
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
    cells[col] = null
  }
  return { columns, cells }
}

/**
 * Merges column name arrays with table prefixes, matching mergeRows logic.
 *
 * @param {string[]} leftColumns
 * @param {string[]} rightColumns
 * @param {string} leftTable
 * @param {string} rightTable
 * @returns {string[]}
 */
function mergeColumnNames(leftColumns, rightColumns, leftTable, rightTable) {
  return [
    ...leftColumns.map(c => c.includes('.') ? c : `${leftTable}.${c}`),
    ...rightColumns.map(c => c.includes('.') ? c : `${rightTable}.${c}`),
  ]
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
