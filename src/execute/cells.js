import { evaluateExpr } from '../expression/evaluate.js'

/**
 * @import { AsyncCells, AsyncRow, ExecuteContext, ExprNode, SqlPrimitive } from '../types.js'
 */

/**
 * Cell-construction helpers.
 *
 * AsyncCell is `SqlPrimitive | (() => Promise<SqlPrimitive>)`. Whenever a
 * cell's value is already in hand, store the bare primitive — no closure,
 * no promise. Reserve thunk cells for genuinely lazy/async work (parquet
 * page decode, derived expression).
 *
 * The hazard the closure form carries: a thunk pins its V8 scope's *entire*
 * context, not just the variables it names. A `() => ...` literal created
 * inside an executor loop body therefore pins the surrounding row buffer,
 * sort entries, etc. — which is how a single output row can keep an entire
 * input set alive. So:
 *
 *   1. Prefer raw values over thunks whenever the value is already known.
 *   2. When a thunk is genuinely needed, build it via a helper here so its
 *      context is bounded to the helper's parameters by construction.
 *
 * See CLAUDE.md "Closure hygiene for AsyncRow cells" for the full rule.
 */

/**
 * Cell that lazily evaluates an expression against a single row. The
 * expression isn't yet evaluated, so we have to defer — but the closure's
 * context is just (expr, row, rowIndex, context), never the caller's loop
 * locals.
 *
 * @param {ExprNode} expr
 * @param {AsyncRow} row
 * @param {number | undefined} rowIndex
 * @param {ExecuteContext} context
 * @returns {() => Promise<SqlPrimitive>}
 */
export function expressionCell(expr, row, rowIndex, context) {
  return () => evaluateExpr({ node: expr, row, rowIndex, context })
}

/**
 * Eagerly evaluates every cell on `row` and returns a row whose cells are
 * the resolved primitive values. Severs upstream retention (parquet pages,
 * decoded buffers, source-row closures) so callers that buffer the result —
 * hash-join build, aggregate, sort, buffered window — don't multiply that
 * retention by the buffer size.
 *
 * If every cell is already a bare value, returns the row as-is.
 *
 * @param {AsyncRow} row
 * @returns {Promise<AsyncRow>}
 */
export async function materializeRow(row) {
  const { columns, cells } = row
  let hasThunk = false
  for (const col of columns) {
    if (typeof cells[col] === 'function') { hasThunk = true; break }
  }
  if (!hasThunk) return row
  const values = await Promise.all(columns.map(c => {
    const cell = cells[c]
    return typeof cell === 'function' ? cell() : cell
  }))
  return slimRow(columns, values)
}

/**
 * Builds an AsyncRow from parallel `columns` / `values` arrays. Cells are
 * the raw primitives — no closures, so no V8 context to pin upstream state.
 *
 * @param {string[]} columns
 * @param {SqlPrimitive[]} values
 * @returns {AsyncRow}
 */
export function slimRow(columns, values) {
  /** @type {AsyncCells} */
  const cells = {}
  for (let i = 0; i < columns.length; i++) {
    cells[columns[i]] = values[i]
  }
  return { columns, cells }
}
