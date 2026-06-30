/**
 * @import { AsyncRow, ExecutionBudget, SqlPrimitive } from '../types.js'
 */

// Coarse per-value byte estimates used by the buffered-byte ceiling. The byte
// budget is a cheap safety bound, not exact accounting: lazy cells are never
// forced in order to measure them, so the estimate is intentionally approximate.
const STRING_BYTES_PER_CHAR = 2 // one UTF-16 code unit
const NUMBER_BYTES = 8
const SMALL_VALUE_BYTES = 4
const OBJECT_BYTES = 32
const KEY_OVERHEAD_BYTES = 16 // per buffered cell when no materialized value is available
const ROW_OVERHEAD_BYTES = 24

/**
 * Cheap byte estimate for a single buffered primitive value.
 *
 * @param {SqlPrimitive} value
 * @returns {number}
 */
export function estimateValueBytes(value) {
  if (value == null) return 0
  if (typeof value === 'string') return value.length * STRING_BYTES_PER_CHAR
  if (typeof value === 'number' || typeof value === 'bigint') return NUMBER_BYTES
  if (typeof value === 'boolean') return SMALL_VALUE_BYTES
  if (value instanceof Date) return NUMBER_BYTES
  return OBJECT_BYTES
}

/**
 * Cheap byte estimate for a buffered row. Uses pre-materialized values when the
 * source exposes them (`row.resolved`); otherwise approximates from the column
 * count so the estimate stays O(columns) and never forces a lazy cell.
 *
 * @param {AsyncRow} row
 * @returns {number}
 */
export function estimateRowBytes(row) {
  const { resolved } = row
  if (resolved) {
    let bytes = ROW_OVERHEAD_BYTES
    for (const key of Object.keys(resolved)) {
      bytes += key.length * STRING_BYTES_PER_CHAR + estimateValueBytes(resolved[key])
    }
    return bytes
  }
  return ROW_OVERHEAD_BYTES + (row.columns?.length ?? 0) * KEY_OVERHEAD_BYTES
}

/**
 * Thrown when an operator's in-memory accumulation would exceed the configured
 * execution budget. V1 of bounded query execution refuses over the ceiling
 * rather than spilling to disk or truncating (hypaware LLP 0056).
 *
 * What "refused" means for an already-running query depends on the operator
 * class:
 *
 * - Buffering operators (ORDER BY, GROUP BY, the scalar-aggregate slow path)
 *   fully buffer their input before they can emit a row, so the refusal is
 *   all-or-nothing: the error is thrown before the first row is yielded and no
 *   partial result escapes.
 * - Streaming operators (DISTINCT, COUNT(DISTINCT)) emit as they go and bound
 *   only their dedup-set memory, so rows 1..N may already have been yielded
 *   before a later key trips the ceiling. A consumer MUST treat a thrown
 *   QueryBudgetExceededError as invalidating the whole result — discarding any
 *   rows it already received — never as a spill or a truncation point.
 */
export class QueryBudgetExceededError extends Error {
  /**
   * @param {Object} options
   * @param {string} options.operator - buffering operator that hit the ceiling (e.g. 'ORDER BY')
   * @param {'rows' | 'bytes'} options.limitKind - which ceiling tripped
   * @param {number} options.limit - configured ceiling value that was exceeded
   * @param {number} options.observed - buffered rows/bytes at the point of refusal
   */
  constructor({ operator, limitKind, limit, observed }) {
    super(`Query execution budget exceeded: ${operator} buffered ${observed} ${limitKind}, over the ${limitKind} ceiling of ${limit}`)
    this.name = 'QueryBudgetExceededError'
    /** @type {string} */
    this.operator = operator
    /** @type {'rows' | 'bytes'} */
    this.limitKind = limitKind
    /** @type {number} */
    this.limit = limit
    /** @type {number} */
    this.observed = observed
  }
}

/**
 * Per-operator accountant for the execution budget. An operator constructs one
 * of these and charges every row (or retained dedup key) it accumulates in
 * memory; when the configured row or byte ceiling is crossed it
 * throws {@link QueryBudgetExceededError} instead of continuing to buffer. An
 * undefined budget — or an undefined ceiling within it — is unbounded: `charge`
 * only counts and never throws, so callers that pass no budget are unaffected.
 */
export class BufferBudget {
  /**
   * @param {ExecutionBudget} [budget] - the execution budget, or undefined for unbounded
   * @param {string} [operator] - operator label for the refusal message
   */
  constructor(budget, operator = 'query') {
    /** @type {number | undefined} */
    this.maxRows = budget?.maxBufferedRows
    /** @type {number | undefined} */
    this.maxBytes = budget?.maxBufferedBytes
    /** @type {string} */
    this.operator = operator
    /** @type {number} */
    this.rows = 0
    /** @type {number} */
    this.bytes = 0
  }

  /**
   * Charges one buffered entry of the given estimated size, refusing if either
   * the row or byte ceiling is crossed.
   *
   * @param {number} bytes - estimated bytes retained by this entry
   * @returns {void}
   */
  charge(bytes) {
    this.rows++
    if (this.maxRows !== undefined && this.rows > this.maxRows) {
      throw new QueryBudgetExceededError({
        operator: this.operator,
        limitKind: 'rows',
        limit: this.maxRows,
        observed: this.rows,
      })
    }
    if (this.maxBytes !== undefined) {
      this.bytes += bytes
      if (this.bytes > this.maxBytes) {
        throw new QueryBudgetExceededError({
          operator: this.operator,
          limitKind: 'bytes',
          limit: this.maxBytes,
          observed: this.bytes,
        })
      }
    }
  }

  /**
   * Charges one buffered row (sort / aggregate row collection).
   *
   * @param {AsyncRow} row
   * @returns {void}
   */
  addRow(row) {
    this.charge(this.maxBytes === undefined ? 0 : estimateRowBytes(row))
  }

  /**
   * Charges one retained dedup key (DISTINCT / set-membership buffers).
   *
   * @param {SqlPrimitive} key
   * @returns {void}
   */
  addKey(key) {
    this.charge(this.maxBytes === undefined ? 0 : estimateValueBytes(key))
  }
}
