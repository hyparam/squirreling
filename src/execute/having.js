import { unknownFunctionError } from '../parseErrors.js'
import { isAggregateFunc } from '../validation.js'
import { evaluateExpr } from './expression.js'
import { applyBinaryOp } from './utils.js'

/**
 * @import { AggregateFunc, AsyncDataSource, ExprNode, AsyncRow, SqlPrimitive, UserDefinedFunction } from '../types.js'
 */

/**
 * Evaluates a HAVING expression with support for aggregate functions
 *
 * @param {Object} options
 * @param {ExprNode} options.expr - the HAVING expression
 * @param {AsyncRow} options.row - the aggregated result row
 * @param {AsyncRow[]} options.group - the group of rows for re-evaluating aggregates
 * @param {Record<string, AsyncDataSource>} options.tables
 * @param {Record<string, UserDefinedFunction>} [options.functions]
 * @returns {Promise<boolean>} whether the HAVING condition is satisfied
 */
export async function evaluateHavingExpr({ expr, row, group, tables, functions }) {
  // Having context
  const context = { ...group[0], ...row }

  // For HAVING, we need special handling of aggregate functions
  // They need to be re-evaluated against the group
  if (expr.type === 'function') {
    const funcName = expr.name.toUpperCase()
    if (isAggregateFunc(funcName)) {
      // Evaluate aggregate function on the group
      return Boolean(await evaluateAggregateFunction({ funcName, args: expr.args, group, tables, functions }))
    }
  }

  if (expr.type === 'binary') {
    const left = await evaluateHavingValue({ expr: expr.left, context, group, tables, functions })

    // Short-circuit evaluation for AND and OR
    if (expr.op === 'AND') {
      if (!left) return false
    }
    if (expr.op === 'OR') {
      if (left) return true
    }

    const right = await evaluateHavingValue({ expr: expr.right, context, group, tables, functions })
    return Boolean(applyBinaryOp(expr.op, left, right))
  }

  if (expr.type === 'unary') {
    if (expr.op === 'NOT') {
      return !await evaluateHavingExpr({ expr: expr.argument, row: context, group, tables, functions })
    }
    if (expr.op === 'IS NULL') {
      return await evaluateHavingValue({ expr: expr.argument, context, group, tables, functions }) == null
    }
    if (expr.op === 'IS NOT NULL') {
      return await evaluateHavingValue({ expr: expr.argument, context, group, tables, functions }) != null
    }
  }

  // For other expression types, use the context row
  return Boolean(await evaluateExpr({ node: expr, row: context, tables, functions }))
}

/**
 * Evaluates a value in a HAVING expression
 *
 * @param {Object} options
 * @param {ExprNode} options.expr
 * @param {AsyncRow} options.context - the context row
 * @param {AsyncRow[]} options.group - the group of rows
 * @param {Record<string, AsyncDataSource>} options.tables
 * @param {Record<string, UserDefinedFunction>} [options.functions]
 * @returns {Promise<SqlPrimitive>} the evaluated value
 */
function evaluateHavingValue({ expr, context, group, tables, functions }) {
  if (expr.type === 'function') {
    const funcName = expr.name.toUpperCase()
    if (isAggregateFunc(funcName)) {
      return evaluateAggregateFunction({ funcName, args: expr.args, group, tables, functions })
    }
  }

  // For binary expressions, we need to use evaluateHavingExpr to properly handle aggregates
  if (expr.type === 'binary' || expr.type === 'unary') {
    return evaluateHavingExpr({ expr, row: context, group, tables, functions })
  }

  return evaluateExpr({ node: expr, row: context, tables, functions })
}

/**
 * Evaluates an aggregate function on a group
 *
 * @param {Object} options
 * @param {AggregateFunc} options.funcName - aggregate function name
 * @param {ExprNode[]} options.args - function arguments
 * @param {AsyncRow[]} options.group - the group of rows
 * @param {Record<string, AsyncDataSource>} options.tables
 * @param {Record<string, UserDefinedFunction>} [options.functions]
 * @returns {Promise<SqlPrimitive>} the aggregate result
 */
async function evaluateAggregateFunction({ funcName, args, group, tables, functions }) {
  if (funcName === 'COUNT') {
    if (args.length === 1 && args[0].type === 'identifier' && args[0].name === '*') {
      return group.length
    }
    // COUNT(column) - count non-null values
    let count = 0
    for (const row of group) {
      const val = await evaluateExpr({ node: args[0], row, tables, functions })
      if (val != null) count++
    }
    return count
  }

  if (funcName === 'SUM') {
    let sum = 0
    for (const row of group) {
      const val = await evaluateExpr({ node: args[0], row, tables, functions })
      if (val != null) sum += Number(val)
    }
    return sum
  }

  if (funcName === 'AVG') {
    let sum = 0
    let count = 0
    for (const row of group) {
      const val = await evaluateExpr({ node: args[0], row, tables, functions })
      if (val != null) {
        sum += Number(val)
        count++
      }
    }
    return count > 0 ? sum / count : null
  }

  if (funcName === 'MIN') {
    let min = null
    for (const row of group) {
      const val = await evaluateExpr({ node: args[0], row, tables, functions })
      if (val != null && (min == null || val < min)) {
        min = val
      }
    }
    return min
  }

  if (funcName === 'MAX') {
    let max = null
    for (const row of group) {
      const val = await evaluateExpr({ node: args[0], row, tables, functions })
      if (val != null && (max == null || val > max)) {
        max = val
      }
    }
    return max
  }

  if (funcName === 'STDDEV_SAMP' || funcName === 'STDDEV_POP') {
    const values = []
    for (const row of group) {
      const val = await evaluateExpr({ node: args[0], row, tables, functions })
      if (val == null) continue
      const num = Number(val)
      if (!Number.isFinite(num)) continue
      values.push(num)
    }
    const n = values.length
    if (n === 0) return null
    if (funcName === 'STDDEV_SAMP' && n === 1) return null

    const mean = values.reduce((a, b) => a + b, 0) / n
    const squaredDiffs = values.reduce((acc, val) => acc + (val - mean) ** 2, 0)
    const divisor = funcName === 'STDDEV_SAMP' ? n - 1 : n
    return Math.sqrt(squaredDiffs / divisor)
  }

  throw unknownFunctionError({
    funcName,
    positionStart: 0,
    positionEnd: 0,
    validFunctions: 'COUNT, SUM, AVG, MIN, MAX, STDDEV_SAMP, STDDEV_POP',
  })
}
