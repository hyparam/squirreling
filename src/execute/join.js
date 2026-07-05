import { evaluateExpr } from '../expression/evaluate.js'
import { keyify, maxBounds } from './utils.js'
import { executePlan } from './execute.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { AsyncCells, AsyncRow, ExecuteContext, QueryResults } from '../types.js'
 * @import { HashJoinNode, NestedLoopJoinNode, PositionalJoinNode } from '../plan/types.js'
 */

// Yield to the event loop every 4000 iterations so that aborts can actually fire
const YIELD_INTERVAL = 4000

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
  // Buffer the smaller side when both sizes are known, streaming the larger
  // side through the outer loop. Swapping reorders output rows, which SQL
  // leaves unspecified.
  const leftSize = left.numRows ?? left.maxRows
  const rightSize = right.numRows ?? right.maxRows
  const swap = leftSize !== undefined && rightSize !== undefined && leftSize < rightSize
  return {
    columns: mergeColumnNames(left.columns, right.columns, plan.leftAlias, plan.rightAlias),
    async *rows() {
      const leftTable = plan.leftAlias
      const rightTable = plan.rightAlias
      const inner = swap ? left : right
      const outer = swap ? right : left
      // Which sides must also emit their unmatched rows
      const innerOuter = plan.joinType === 'FULL' || plan.joinType === (swap ? 'LEFT' : 'RIGHT')
      const outerOuter = plan.joinType === 'FULL' || plan.joinType === (swap ? 'RIGHT' : 'LEFT')

      /**
       * @param {AsyncRow} outerRow
       * @param {AsyncRow} innerRow
       * @returns {AsyncRow}
       */
      function merge(outerRow, innerRow) {
        return swap
          ? mergeRows(innerRow, outerRow, leftTable, rightTable)
          : mergeRows(outerRow, innerRow, leftTable, rightTable)
      }

      // Buffer the inner side
      /** @type {AsyncRow[]} */
      const innerRows = []
      for await (const row of inner.rows()) {
        context.signal?.throwIfAborted()
        innerRows.push(row)
      }

      const innerCols = innerRows.length ? innerRows[0].columns : []

      /** @type {string[] | undefined} */
      let outerCols = undefined
      /** @type {Set<AsyncRow> | undefined} */
      const matchedInnerRows = innerOuter ? new Set() : undefined

      let innerCount = 0
      for await (const outerRow of outer.rows()) {
        context.signal?.throwIfAborted()

        if (!outerCols) {
          outerCols = outerRow.columns
        }

        let hasMatch = false

        for (const innerRow of innerRows) {
          if (++innerCount % YIELD_INTERVAL === 0) {
            await yieldToEventLoop()
            context.signal?.throwIfAborted()
          }
          const tempMerged = merge(outerRow, innerRow)
          const matches = await evaluateExpr({
            node: plan.condition,
            row: tempMerged,
            context,
          })

          if (matches) {
            hasMatch = true
            matchedInnerRows?.add(innerRow)
            yield tempMerged
          }
        }

        if (!hasMatch && outerOuter) {
          yield merge(outerRow, createNullRow(innerCols))
        }
      }

      context.signal?.throwIfAborted()

      // Unmatched inner rows for outer joins on the buffered side
      if (matchedInnerRows) {
        for (const innerRow of innerRows) {
          if (!matchedInnerRows.has(innerRow)) {
            yield merge(createNullRow(outerCols ?? []), innerRow)
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
        context.signal?.throwIfAborted()

        // When nested inside a correlated subquery, preserve the enclosing
        // outer row so UNNEST args can reference its columns (e.g. o.arr).
        const nestedOuter = context.outerRow
          ? mergeOuterRows(context.outerRow, leftRow, leftTable)
          : leftRow
        const subContext = { ...context, outerRow: nestedOuter }
        const right = executePlan({ plan: plan.right, context: subContext })

        let hasMatch = false
        for await (const rightRow of right.rows()) {
          context.signal?.throwIfAborted()
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

      // Zip both sides in lockstep without buffering either; the shorter
      // side is padded with NULL rows until the longer side is exhausted
      const leftRows = left.rows()
      const rightRows = right.rows()
      /** @type {string[]} */
      let leftCols = []
      /** @type {string[]} */
      let rightCols = []
      let tick = 0
      while (true) {
        const [leftResult, rightResult] = await Promise.all([leftRows.next(), rightRows.next()])
        if (leftResult.done && rightResult.done) return
        signal?.throwIfAborted()
        if (++tick % YIELD_INTERVAL === 0) {
          await yieldToEventLoop()
          signal?.throwIfAborted()
        }
        if (!leftResult.done && !leftCols.length) leftCols = leftResult.value.columns
        if (!rightResult.done && !rightCols.length) rightCols = rightResult.value.columns
        const leftRow = leftResult.done ? createNullRow(leftCols) : leftResult.value
        const rightRow = rightResult.done ? createNullRow(rightCols) : rightResult.value
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
  // Build the hash table on the smaller side when both sizes are known,
  // so joining a small table against a large one buffers the small one.
  // Swapping reorders output rows, which SQL leaves unspecified.
  const leftSize = left.numRows ?? left.maxRows
  const rightSize = right.numRows ?? right.maxRows
  const swap = leftSize !== undefined && rightSize !== undefined && leftSize < rightSize
  return {
    columns: mergeColumnNames(left.columns, right.columns, plan.leftAlias, plan.rightAlias),
    async *rows() {
      const leftTable = plan.leftAlias
      const rightTable = plan.rightAlias
      const { residual } = plan
      const build = swap ? left : right
      const probe = swap ? right : left
      const buildKeys = swap ? plan.leftKeys : plan.rightKeys
      const probeKeys = swap ? plan.rightKeys : plan.leftKeys
      // Which sides must also emit their unmatched rows
      const buildOuter = plan.joinType === 'FULL' || plan.joinType === (swap ? 'LEFT' : 'RIGHT')
      const probeOuter = plan.joinType === 'FULL' || plan.joinType === (swap ? 'RIGHT' : 'LEFT')

      /**
       * @param {AsyncRow} probeRow
       * @param {AsyncRow} buildRow
       * @returns {AsyncRow}
       */
      function merge(probeRow, buildRow) {
        return swap
          ? mergeRows(buildRow, probeRow, leftTable, rightTable)
          : mergeRows(probeRow, buildRow, leftTable, rightTable)
      }

      // Build phase: stream one side into the hash map. The full row list is
      // only retained when unmatched build rows must be emitted afterwards;
      // otherwise rows with NULL join keys are released immediately.
      /** @type {Map<string | number | bigint | boolean, AsyncRow[]>} */
      const hashMap = new Map()
      /** @type {AsyncRow[] | undefined} */
      const buildRows = buildOuter ? [] : undefined
      /** @type {string[]} */
      let buildCols = []
      let innerCount = 0
      for await (const buildRow of build.rows()) {
        context.signal?.throwIfAborted()
        if (++innerCount % YIELD_INTERVAL === 0) {
          await yieldToEventLoop()
          context.signal?.throwIfAborted()
        }
        if (!buildCols.length) {
          buildCols = buildRow.columns
        }
        buildRows?.push(buildRow)
        const keyValues = await Promise.all(
          buildKeys.map(node => evaluateExpr({ node, row: buildRow, context }))
        )
        // SQL semantics: NULL never equals anything, so a row with any NULL
        // join key is excluded from the hash table.
        if (keyValues.some(v => v == null)) continue
        const key = keyify(...keyValues)
        let bucket = hashMap.get(key)
        if (!bucket) {
          bucket = []
          hashMap.set(key, bucket)
        }
        bucket.push(buildRow)
      }

      /** @type {string[] | undefined} */
      let probeCols
      /** @type {Set<AsyncRow> | undefined} */
      const matchedBuildRows = buildOuter ? new Set() : undefined

      // Probe phase: stream the other side
      for await (const probeRow of probe.rows()) {
        context.signal?.throwIfAborted()

        if (!probeCols) {
          probeCols = probeRow.columns
        }

        const keyValues = await Promise.all(
          probeKeys.map(node => evaluateExpr({ node, row: probeRow, context }))
        )
        let matched = false
        if (!keyValues.some(v => v == null)) {
          const key = keyify(...keyValues)
          const candidates = hashMap.get(key)
          if (candidates?.length) {
            for (const buildRow of candidates) {
              if (++innerCount % YIELD_INTERVAL === 0) {
                await yieldToEventLoop()
                context.signal?.throwIfAborted()
              }
              const merged = merge(probeRow, buildRow)
              if (residual) {
                const ok = await evaluateExpr({ node: residual, row: merged, context })
                if (!ok) continue
              }
              matched = true
              matchedBuildRows?.add(buildRow)
              yield merged
            }
          }
        }

        if (!matched && probeOuter) {
          yield merge(probeRow, createNullRow(buildCols))
        }
      }

      context.signal?.throwIfAborted()

      // Unmatched build rows for outer joins on the build side
      if (buildRows && matchedBuildRows) {
        for (const buildRow of buildRows) {
          if (!matchedBuildRows.has(buildRow)) {
            yield merge(createNullRow(probeCols ?? []), buildRow)
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
    cells[col] = () => Promise.resolve(null)
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
