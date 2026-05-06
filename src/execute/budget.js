/**
 * @import { BudgetOperator, BudgetTracker, SqlExecutionBudget } from '../types.js'
 */

const DEFAULT_ROW_BYTES = 64

/**
 * Structured error thrown when a SQL execution budget is exceeded. The
 * `limit` field identifies which budget field was breached so callers can
 * decide whether to abort, fall back to a different strategy, or surface a
 * user-facing message.
 */
export class SqlBudgetError extends Error {
  /**
   * @param {Object} options
   * @param {'maxRowsToMaterialize' | 'maxHeapBytes' | 'maxIntermediateBytes' | 'timeoutMs'} options.limit
   * @param {number} options.value - actual measured value at the time of abort
   * @param {number} options.max - configured budget limit
   * @param {string} [options.operator] - operator name where the limit was hit
   */
  constructor({ limit, value, max, operator }) {
    const where = operator ? ` (operator=${operator})` : ''
    const message = `SQL execution budget exceeded: ${limit}=${value} > ${max}${where}`
    super(message)
    this.name = 'SqlBudgetError'
    this.limit = limit
    this.value = value
    this.max = max
    if (operator !== undefined) this.operator = operator
  }
}

/**
 * Creates a budget tracker for a single query execution. Returns undefined
 * when no budget is provided so callers can use `tracker?.operator(...)`
 * without conditional plumbing through every operator.
 *
 * @param {SqlExecutionBudget} [budget]
 * @returns {BudgetTracker | undefined}
 */
export function createBudget(budget) {
  if (!budget) return undefined
  const startTime = Date.now()
  const { timeoutMs } = budget
  const deadline = timeoutMs !== undefined ? startTime + timeoutMs : undefined

  let totalRows = 0
  let totalHeapBytes = 0

  /**
   * @param {string} [operator]
   */
  function timeoutCheck(operator) {
    if (deadline === undefined || timeoutMs === undefined) return
    if (Date.now() > deadline) {
      throw new SqlBudgetError({
        limit: 'timeoutMs',
        value: Date.now() - startTime,
        max: timeoutMs,
        operator,
      })
    }
  }

  return {
    budget,
    allowDerivedColumnScan: budget.allowDerivedColumnScan !== false,
    checkTimeout() { timeoutCheck() },
    operator(name) {
      let opBytes = 0
      /** @type {BudgetOperator} */
      const handle = {
        addRow(approxBytes) {
          const bytes = approxBytes ?? DEFAULT_ROW_BYTES
          totalRows++
          if (budget.maxRowsToMaterialize !== undefined && totalRows > budget.maxRowsToMaterialize) {
            throw new SqlBudgetError({
              limit: 'maxRowsToMaterialize',
              value: totalRows,
              max: budget.maxRowsToMaterialize,
              operator: name,
            })
          }
          opBytes += bytes
          if (budget.maxIntermediateBytes !== undefined && opBytes > budget.maxIntermediateBytes) {
            throw new SqlBudgetError({
              limit: 'maxIntermediateBytes',
              value: opBytes,
              max: budget.maxIntermediateBytes,
              operator: name,
            })
          }
          totalHeapBytes += bytes
          if (budget.maxHeapBytes !== undefined && totalHeapBytes > budget.maxHeapBytes) {
            throw new SqlBudgetError({
              limit: 'maxHeapBytes',
              value: totalHeapBytes,
              max: budget.maxHeapBytes,
              operator: name,
            })
          }
          timeoutCheck(name)
        },
        checkTimeout() { timeoutCheck(name) },
      }
      return handle
    },
  }
}
