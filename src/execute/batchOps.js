import { evaluateExpr } from '../expression/evaluate.js'

/**
 * @import { AsyncCells, AsyncRow, ColumnBatch, ExecuteContext, ExprNode, SelectColumn, SqlPrimitive } from '../types.js'
 */

/**
 * Builds a reusable AsyncRow view backed by mutable cell holders. Cell
 * accessors return Promise.resolve of the current holder value; updating
 * holders between iterations advances the row to the next batch position
 * without allocating new closures. Safe for sequential evaluation since
 * evaluateExpr awaits each subexpression in order before returning.
 *
 * @param {string[]} columns
 * @returns {{ row: AsyncRow, advance(batch: ColumnBatch, i: number): void }}
 */
export function makeBatchRowView(columns) {
  /** @type {Record<string, SqlPrimitive>} */
  const resolved = {}
  /** @type {AsyncCells} */
  const cells = {}
  for (const col of columns) {
    resolved[col] = null
    cells[col] = () => Promise.resolve(resolved[col])
  }
  /** @type {AsyncRow} */
  const row = { columns, cells, resolved }
  return {
    row,
    advance(batch, i) {
      for (const col of columns) {
        resolved[col] = batch.columns[col][i]
      }
    },
  }
}

/**
 * Skips the first `offset` rows then yields at most `limit` rows from a stream
 * of column batches. Slices typed-array columns at batch boundaries.
 *
 * @param {AsyncIterable<ColumnBatch>} batches
 * @param {number} [limit] - undefined means no upper bound
 * @param {number} [offset] - default 0
 * @param {AbortSignal} [signal]
 * @yields {ColumnBatch}
 */
export async function* limitBatches(batches, limit = Infinity, offset = 0, signal) {
  if (limit <= 0) return
  let skipped = 0
  let yielded = 0
  for await (const batch of batches) {
    if (signal?.aborted) return
    let start = 0
    let take = batch.rowCount
    if (skipped < offset) {
      const remaining = offset - skipped
      if (batch.rowCount <= remaining) {
        skipped += batch.rowCount
        continue
      }
      start = remaining
      take = batch.rowCount - remaining
      skipped += remaining
    }
    if (yielded + take > limit) {
      take = limit - yielded
    }
    if (take === batch.rowCount && start === 0) {
      yield batch
    } else {
      yield sliceBatch(batch, start, take)
    }
    yielded += take
    if (yielded >= limit) return
  }
}

/**
 * Filters a stream of column batches by a predicate. Yields batches with
 * passing rows preserved (and typed-array column types preserved when
 * possible). All-pass batches are forwarded unchanged; no-pass batches are
 * dropped entirely.
 *
 * @param {AsyncIterable<ColumnBatch>} batches
 * @param {ExprNode} condition
 * @param {ExecuteContext} context
 * @param {string[]} columns - column names available in each batch
 * @yields {ColumnBatch}
 */
export async function* filterBatches(batches, condition, context, columns) {
  const view = makeBatchRowView(columns)
  let processed = 0
  for await (const batch of batches) {
    if (context.signal?.aborted) return

    const mask = new Uint8Array(batch.rowCount)
    let count = 0
    const baseRowIndex = batch.rowStart ?? processed
    for (let i = 0; i < batch.rowCount; i++) {
      if (context.signal?.aborted) return
      view.advance(batch, i)
      const result = await evaluateExpr({
        node: condition,
        row: view.row,
        rowIndex: baseRowIndex + i + 1,
        context,
      })
      if (result) {
        mask[i] = 1
        count++
      }
    }
    processed += batch.rowCount

    if (count === 0) continue
    if (count === batch.rowCount) {
      yield batch
      continue
    }

    /** @type {Record<string, ArrayLike<SqlPrimitive>>} */
    const out = {}
    for (const col of Object.keys(batch.columns)) {
      out[col] = compactColumn(batch.columns[col], mask, count)
    }
    /** @type {ColumnBatch} */
    const filtered = { rowCount: count, columns: out }
    if (batch.rowStart !== undefined) filtered.rowStart = batch.rowStart
    if (batch.rowIds !== undefined) filtered.rowIds = compactRowIds(batch.rowIds, mask, count)
    yield filtered
  }
}

/**
 * Projects a stream of column batches via a "simple" SELECT list (only stars
 * and identifier expressions). Stars expand to source columns; identifiers
 * select a column by name (with suffix matching as a fallback). Column arrays
 * are aliased — no per-cell copy — so typed-array columns flow through
 * unchanged.
 *
 * @param {AsyncIterable<ColumnBatch>} batches
 * @param {SelectColumn[]} planColumns
 * @param {string[]} outColumnNames - aliases produced by selectColumnNames
 * @param {string[]} childColumns - columns produced by the child operator
 * @yields {ColumnBatch}
 */
export async function* projectBatchesSimple(batches, planColumns, outColumnNames, childColumns) {
  for await (const batch of batches) {
    /** @type {Record<string, ArrayLike<SqlPrimitive>>} */
    const out = {}
    let colIdx = 0
    for (const col of planColumns) {
      if (col.type === 'star') {
        const prefix = col.table ? `${col.table}.` : undefined
        for (const key of childColumns) {
          if (prefix && !key.startsWith(prefix)) continue
          out[outColumnNames[colIdx++]] = batch.columns[key]
        }
      } else if (col.expr.type === 'identifier') {
        // Simple identifier projection (planColumns satisfies the resolveable
        // gate enforced by executeProject).
        const id = col.expr
        const sourceName = id.prefix ? `${id.prefix}.${id.name}` : id.name
        const alias = outColumnNames[colIdx++]
        if (sourceName in batch.columns) {
          out[alias] = batch.columns[sourceName]
        } else {
          // Suffix-match like the row-mode evaluator does.
          const suffix = '.' + id.name
          const match = childColumns.find(c => c.endsWith(suffix))
          if (match && match in batch.columns) {
            out[alias] = batch.columns[match]
          } else {
            // Column not found — emit an array of nulls. The row-mode path
            // would throw ColumnNotFoundError, but we should never reach this
            // branch because the planner resolves identifiers ahead of time.
            out[alias] = new Array(batch.rowCount).fill(null)
          }
        }
      } else {
        // Unreachable — executeProject only routes here when every non-star
        // column is an identifier. Throw to surface plan/executor drift.
        throw new Error(`projectBatchesSimple: unexpected expression type "${col.expr.type}"`)
      }
    }
    /** @type {ColumnBatch} */
    const projected = { rowCount: batch.rowCount, columns: out }
    if (batch.rowStart !== undefined) projected.rowStart = batch.rowStart
    if (batch.rowIds !== undefined) projected.rowIds = batch.rowIds
    yield projected
  }
}

/**
 * Picks the indices in `mask` from `src` into a new array. Preserves the
 * underlying typed-array constructor so e.g. Uint32Array stays Uint32Array.
 *
 * @param {ArrayLike<SqlPrimitive>} src
 * @param {Uint8Array} mask
 * @param {number} count
 * @returns {ArrayLike<SqlPrimitive>}
 */
function compactColumn(src, mask, count) {
  /** @type {any} */
  const anySrc = src
  const Ctor = anySrc.constructor
  /** @type {any} */
  let dst
  if (typeof Ctor === 'function' && Ctor !== Array && Ctor !== Object) {
    try {
      dst = new Ctor(count)
    } catch {
      dst = new Array(count)
    }
  } else {
    dst = new Array(count)
  }
  let j = 0
  for (let i = 0; i < mask.length; i++) {
    if (mask[i]) dst[j++] = src[i]
  }
  return dst
}

/**
 * Compacts a rowIds typed array using the same mask as compactColumn.
 *
 * @param {Uint32Array | BigUint64Array} rowIds
 * @param {Uint8Array} mask
 * @param {number} count
 * @returns {Uint32Array | BigUint64Array}
 */
function compactRowIds(rowIds, mask, count) {
  /** @type {any} */
  const Ctor = rowIds.constructor
  /** @type {any} */
  const dst = new Ctor(count)
  let j = 0
  for (let i = 0; i < mask.length; i++) {
    if (mask[i]) dst[j++] = rowIds[i]
  }
  return dst
}

/**
 * Slices a batch by [start, start + take) over its columns. Each column array
 * is sliced via its own slice() method, preserving typed-array constructors.
 *
 * @param {ColumnBatch} batch
 * @param {number} start
 * @param {number} take
 * @returns {ColumnBatch}
 */
function sliceBatch(batch, start, take) {
  /** @type {Record<string, ArrayLike<SqlPrimitive>>} */
  const out = {}
  for (const col of Object.keys(batch.columns)) {
    out[col] = sliceColumn(batch.columns[col], start, start + take)
  }
  /** @type {ColumnBatch} */
  const result = { rowCount: take, columns: out }
  if (batch.rowStart !== undefined) result.rowStart = batch.rowStart + start
  if (batch.rowIds !== undefined) {
    /** @type {any} */
    const { rowIds } = batch
    result.rowIds = rowIds.slice(start, start + take)
  }
  return result
}

/**
 * @param {ArrayLike<SqlPrimitive>} src
 * @param {number} start
 * @param {number} end
 * @returns {ArrayLike<SqlPrimitive>}
 */
function sliceColumn(src, start, end) {
  /** @type {any} */
  const anySrc = src
  if (typeof anySrc.slice === 'function') return anySrc.slice(start, end)
  // Fallback for ArrayLike values without slice
  const out = new Array(end - start)
  for (let i = start; i < end; i++) out[i - start] = src[i]
  return out
}
