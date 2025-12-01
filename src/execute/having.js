/**
 * @import { AggregateFunc, AsyncDataSource, ExprNode, AsyncRow, SqlPrimitive } from '../types.js'
 */

import { isAggregateFunc } from '../validation.js'
import { evaluateExpr } from './expression.js'

/**
 * Creates a context for evaluating HAVING expressions
 *
 * @param {Record<string, SqlPrimitive> | AsyncRow} resultRow - the aggregated result row
 * @param {AsyncRow[]} group - the group of rows
 * @returns {AsyncRow} a context row for HAVING evaluation
 */
function createHavingContext(resultRow, group) {
  // Include the first row of the group (for GROUP BY columns)
  const firstRow = group[0]
  /** @type {AsyncRow} */
  const context = {}
  if (firstRow) {
    for (const [key, cell] of Object.entries(firstRow)) {
      context[key] = cell
    }
  }
  // Merge with result row (which has aggregates computed)
  for (const [key, value] of Object.entries(resultRow)) {
    context[key] = typeof value === 'function' ? value : () => Promise.resolve(value)
  }

  return context
}

/**
 * Evaluates a HAVING expression with support for aggregate functions
 *
 * @param {ExprNode} expr - the HAVING expression
 * @param {Record<string, any>} row - the aggregated result row
 * @param {AsyncRow[]} group - the group of rows for re-evaluating aggregates
 * @param {Record<string, AsyncDataSource>} tables
 * @returns {Promise<boolean>} whether the HAVING condition is satisfied
 */
export async function evaluateHavingExpr(expr, row, group, tables) {
  const context = createHavingContext(row, group)

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
    const right = await evaluateHavingValue(expr.right, context, group, tables)

    if (expr.op === 'AND') {
      return Boolean(left && right)
    }
    if (expr.op === 'OR') {
      return Boolean(left || right)
    }

    // Handle NULL comparisons
    if (left == null || right == null) {
      if (expr.op === '=' || expr.op === '!=' || expr.op === '<>') {
        return false
      }
    }

    if (expr.op === '=') return left === right
    if (expr.op === '!=' || expr.op === '<>') return left !== right
    if (expr.op === '<') return left < right
    if (expr.op === '>') return left > right
    if (expr.op === '<=') return left <= right
    if (expr.op === '>=') return left >= right
    if (expr.op === 'LIKE') {
      const str = String(left)
      const pattern = String(right)
      const regexPattern = pattern
        .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        .replace(/%/g, '.*')
        .replace(/_/g, '.')
      const regex = new RegExp('^' + regexPattern + '$', 'i')
      return regex.test(str)
    }
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

  if (expr.type === 'between' || expr.type === 'not between') {
    const exprVal = await evaluateHavingValue(expr.expr, context, group, tables)
    const lower = await evaluateHavingValue(expr.lower, context, group, tables)
    const upper = await evaluateHavingValue(expr.upper, context, group, tables)

    // If any value is NULL, return false (SQL behavior)
    if (exprVal == null || lower == null || upper == null) {
      return false
    }

    const isBetween = exprVal >= lower && exprVal <= upper
    return expr.type === 'between' ? isBetween : !isBetween
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
  if (expr.type === 'binary' || expr.type === 'unary' || expr.type === 'between' || expr.type === 'not between') {
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

  throw new Error('Unsupported aggregate function: ' + funcName)
}
