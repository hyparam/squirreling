/**
 * @import { ExprNode, Row, SqlPrimitive } from '../types.js'
 */

import { evaluateExpr } from './expression.js'

/**
 * Creates a context for evaluating HAVING expressions
 * @param {Row} resultRow - The aggregated result row
 * @param {Row[]} group - The group of rows
 * @returns {Row} A context row for HAVING evaluation
 */
export function createHavingContext(resultRow, group) {
  // Include the first row of the group (for GROUP BY columns)
  const firstRow = group[0] || {}
  // Merge with result row (which has aggregates computed)
  return { ...firstRow, ...resultRow }
}

/**
 * Evaluates a HAVING expression with support for aggregate functions
 * @param {ExprNode} expr - The HAVING expression
 * @param {Row} context - The context row with aggregated values
 * @param {Row[]} group - The group of rows for re-evaluating aggregates
 * @returns {boolean} Whether the HAVING condition is satisfied
 */
export function evaluateHavingExpr(expr, context, group) {
  // For HAVING, we need special handling of aggregate functions
  // They need to be re-evaluated against the group
  if (expr.type === 'function') {
    const funcName = expr.name.toUpperCase()
    if (['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(funcName)) {
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

  // For other expression types, use the context row
  return Boolean(evaluateExpr(expr, context))
}

/**
 * Evaluates a value in a HAVING expression
 * @param {ExprNode} expr - The expression
 * @param {Row} context - The context row
 * @param {Row[]} group - The group of rows
 * @returns {SqlPrimitive} The evaluated value
 */
function evaluateHavingValue(expr, context, group) {
  if (expr.type === 'function') {
    const funcName = expr.name.toUpperCase()
    if (['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(funcName)) {
      return evaluateAggregateFunction(funcName, expr.args, group)
    }
  }

  // For binary expressions, we need to use evaluateHavingExpr to properly handle aggregates
  if (expr.type === 'binary' || expr.type === 'unary') {
    return evaluateHavingExpr(expr, context, group)
  }

  return evaluateExpr(expr, context)
}

/**
 * Evaluates an aggregate function on a group
 * @param {string} funcName - The aggregate function name
 * @param {ExprNode[]} args - The function arguments
 * @param {Row[]} group - The group of rows
 * @returns {SqlPrimitive} The aggregate result
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
