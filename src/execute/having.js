/**
 * @import { AggregateFunc, ExprNode, RowSource, SqlPrimitive } from '../types.js'
 */

import { isAggregateFunc } from '../validation.js'
import { evaluateExpr } from './expression.js'

/**
 * Creates a context for evaluating HAVING expressions
 *
 * @param {Record<string, any>} resultRow - the aggregated result row
 * @param {RowSource[]} group - the group of rows
 * @returns {RowSource} a context row for HAVING evaluation
 */
function createHavingContext(resultRow, group) {
  // Include the first row of the group (for GROUP BY columns)
  const firstRow = group[0]
  /** @type {Record<string, any>} */
  const context = {}
  if (firstRow) {
    const keys = firstRow.getKeys()
    for (const key of keys) {
      context[key] = firstRow.getCell(key)
    }
  }
  // Merge with result row (which has aggregates computed)
  Object.assign(context, resultRow)

  // Return a Row accessor wrapping the context
  return {
    getCell(name) {
      return context[name]
    },
    getKeys() {
      return Object.keys(context)
    },
  }
}

/**
 * Evaluates a HAVING expression with support for aggregate functions
 *
 * @param {ExprNode} expr - the HAVING expression
 * @param {Record<string, any>} row - the aggregated result row
 * @param {RowSource[]} group - the group of rows for re-evaluating aggregates
 * @returns {boolean} whether the HAVING condition is satisfied
 */
export function evaluateHavingExpr(expr, row, group) {
  const context = createHavingContext(row, group)

  // For HAVING, we need special handling of aggregate functions
  // They need to be re-evaluated against the group
  if (expr.type === 'function') {
    const funcName = expr.name.toUpperCase()
    if (isAggregateFunc(funcName)) {
      // Evaluate aggregate function on the group
      return Boolean(evaluateAggregateFunction(funcName, expr.args, group))
    }
  }

  if (expr.type === 'binary') {
    const left = evaluateHavingValue(expr.left, context, group)
    const right = evaluateHavingValue(expr.right, context, group)

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
      return !evaluateHavingExpr(expr.argument, context, group)
    }
    if (expr.op === 'IS NULL') {
      return evaluateHavingValue(expr.argument, context, group) == null
    }
    if (expr.op === 'IS NOT NULL') {
      return evaluateHavingValue(expr.argument, context, group) != null
    }
  }

  if (expr.type === 'between' || expr.type === 'not between') {
    const exprVal = evaluateHavingValue(expr.expr, context, group)
    const lower = evaluateHavingValue(expr.lower, context, group)
    const upper = evaluateHavingValue(expr.upper, context, group)

    // If any value is NULL, return false (SQL behavior)
    if (exprVal == null || lower == null || upper == null) {
      return false
    }

    const isBetween = exprVal >= lower && exprVal <= upper
    return expr.type === 'between' ? isBetween : !isBetween
  }

  // For other expression types, use the context row
  return Boolean(evaluateExpr(expr, context))
}

/**
 * Evaluates a value in a HAVING expression
 *
 * @param {ExprNode} expr
 * @param {RowSource} context - the context row
 * @param {RowSource[]} group - the group of rows
 * @returns {SqlPrimitive} the evaluated value
 */
function evaluateHavingValue(expr, context, group) {
  if (expr.type === 'function') {
    const funcName = expr.name.toUpperCase()
    if (isAggregateFunc(funcName)) {
      return evaluateAggregateFunction(funcName, expr.args, group)
    }
  }

  // For binary expressions, we need to use evaluateHavingExpr to properly handle aggregates
  if (expr.type === 'binary' || expr.type === 'unary' || expr.type === 'between' || expr.type === 'not between') {
    return evaluateHavingExpr(expr, context, group)
  }

  return evaluateExpr(expr, context)
}

/**
 * Evaluates an aggregate function on a group
 *
 * @param {AggregateFunc} funcName - aggregate function name
 * @param {ExprNode[]} args - function arguments
 * @param {RowSource[]} group - the group of rows
 * @returns {SqlPrimitive} the aggregate result
 */
function evaluateAggregateFunction(funcName, args, group) {
  if (funcName === 'COUNT') {
    if (args.length === 1 && args[0].type === 'identifier' && args[0].name === '*') {
      return group.length
    }
    // COUNT(column) - count non-null values
    let count = 0
    for (const row of group) {
      const val = evaluateExpr(args[0], row)
      if (val != null) count++
    }
    return count
  }

  if (funcName === 'SUM') {
    let sum = 0
    for (const row of group) {
      const val = evaluateExpr(args[0], row)
      if (val != null) sum += Number(val)
    }
    return sum
  }

  if (funcName === 'AVG') {
    let sum = 0
    let count = 0
    for (const row of group) {
      const val = evaluateExpr(args[0], row)
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
      const val = evaluateExpr(args[0], row)
      if (val != null && (min == null || val < min)) {
        min = val
      }
    }
    return min
  }

  if (funcName === 'MAX') {
    let max = null
    for (const row of group) {
      const val = evaluateExpr(args[0], row)
      if (val != null && (max == null || val > max)) {
        max = val
      }
    }
    return max
  }

  throw new Error('Unsupported aggregate function: ' + funcName)
}
