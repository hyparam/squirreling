import { unknownFunctionError } from '../errors.js'
import { isAggregateFunc } from '../validation.js'
import { evaluateExpr } from './expression.js'
import { applyBinaryOp } from './utils.js'

/**
 * @import { AggregateFunc, AsyncDataSource, ExprNode, AsyncRow, SqlPrimitive } from '../types.js'
 */

/**
 * Evaluates a HAVING expression with support for aggregate functions
 *
 * @param {ExprNode} expr - the HAVING expression
 * @param {AsyncRow} row - the aggregated result row
 * @param {AsyncRow[]} group - the group of rows for re-evaluating aggregates
 * @param {Record<string, AsyncDataSource>} tables
 * @returns {Promise<boolean>} whether the HAVING condition is satisfied
 */
export async function evaluateHavingExpr(expr, row, group, tables) {
  // Having context
  const context = { ...group[0] ?? {}, ...row }

  // For HAVING, we need special handling of aggregate functions
  // They need to be re-evaluated against the group
  if (expr.type === 'function') {
    const funcName = expr.name.toUpperCase()
    if (isAggregateFunc(funcName)) {
      // Evaluate aggregate function on the group
      return Boolean(await evaluateAggregateFunction(funcName, expr.args, group, tables))
    }
  }

  if (expr.type === 'binary') {
    const left = await evaluateHavingValue(expr.left, context, group, tables)

    // Short-circuit evaluation for AND and OR
    if (expr.op === 'AND') {
      if (!left) return false
    }
    if (expr.op === 'OR') {
      if (left) return true
    }

    const right = await evaluateHavingValue(expr.right, context, group, tables)
    return Boolean(applyBinaryOp(expr.op, left, right))
  }

  if (expr.type === 'unary') {
    if (expr.op === 'NOT') {
      return !await evaluateHavingExpr(expr.argument, context, group, tables)
    }
    if (expr.op === 'IS NULL') {
      return await evaluateHavingValue(expr.argument, context, group, tables) == null
    }
    if (expr.op === 'IS NOT NULL') {
      return await evaluateHavingValue(expr.argument, context, group, tables) != null
    }
  }

  // For other expression types, use the context row
  return Boolean(await evaluateExpr({ node: expr, row: context, tables }))
}

/**
 * Evaluates a value in a HAVING expression
 *
 * @param {ExprNode} expr
 * @param {AsyncRow} context - the context row
 * @param {AsyncRow[]} group - the group of rows
 * @param {Record<string, AsyncDataSource>} tables
 * @returns {Promise<SqlPrimitive>} the evaluated value
 */
function evaluateHavingValue(expr, context, group, tables) {
  if (expr.type === 'function') {
    const funcName = expr.name.toUpperCase()
    if (isAggregateFunc(funcName)) {
      return evaluateAggregateFunction(funcName, expr.args, group, tables)
    }
  }

  // For binary expressions, we need to use evaluateHavingExpr to properly handle aggregates
  if (expr.type === 'binary' || expr.type === 'unary') {
    return evaluateHavingExpr(expr, context, group, tables)
  }

  return evaluateExpr({ node: expr, row: context, tables })
}

/**
 * Evaluates an aggregate function on a group
 *
 * @param {AggregateFunc} funcName - aggregate function name
 * @param {ExprNode[]} args - function arguments
 * @param {AsyncRow[]} group - the group of rows
 * @param {Record<string, AsyncDataSource>} tables
 * @returns {Promise<SqlPrimitive>} the aggregate result
 */
async function evaluateAggregateFunction(funcName, args, group, tables) {
  if (funcName === 'COUNT') {
    if (args.length === 1 && args[0].type === 'identifier' && args[0].name === '*') {
      return group.length
    }
    // COUNT(column) - count non-null values
    let count = 0
    for (const row of group) {
      const val = await evaluateExpr({ node: args[0], row, tables })
      if (val != null) count++
    }
    return count
  }

  if (funcName === 'SUM') {
    let sum = 0
    for (const row of group) {
      const val = await evaluateExpr({ node: args[0], row, tables })
      if (val != null) sum += Number(val)
    }
    return sum
  }

  if (funcName === 'AVG') {
    let sum = 0
    let count = 0
    for (const row of group) {
      const val = await evaluateExpr({ node: args[0], row, tables })
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
      const val = await evaluateExpr({ node: args[0], row, tables })
      if (val != null && (min == null || val < min)) {
        min = val
      }
    }
    return min
  }

  if (funcName === 'MAX') {
    let max = null
    for (const row of group) {
      const val = await evaluateExpr({ node: args[0], row, tables })
      if (val != null && (max == null || val > max)) {
        max = val
      }
    }
    return max
  }

  throw unknownFunctionError(funcName, undefined, 'COUNT, SUM, AVG, MIN, MAX')
}
