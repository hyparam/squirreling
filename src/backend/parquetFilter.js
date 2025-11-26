/**
 * Converts squirreling WHERE AST to hyparquet MongoDB-style filter format.
 * Uses all-or-nothing strategy: returns undefined if any part can't be converted.
 *
 * @import { ParquetQueryFilter } from 'hyparquet'
 * @import { BetweenNode, BinaryNode, BinaryOp, ExprNode, InValuesNode } from '../types.js'
 */

/**
 * Converts a WHERE clause AST to hyparquet filter format.
 * Returns undefined if the expression cannot be fully converted.
 *
 * @param {ExprNode | undefined} where - the WHERE clause AST
 * @returns {ParquetQueryFilter | undefined}
 */
export function whereToParquetFilter(where) {
  if (!where) return undefined
  return convertExpr(where)
}

/**
 * Converts an expression node to filter format
 *
 * @param {ExprNode} node
 * @returns {ParquetQueryFilter | undefined}
 */
function convertExpr(node) {
  if (node.type === 'binary') {
    return convertBinary(node)
  }
  if (node.type === 'between' || node.type === 'not between') {
    return convertBetween(node)
  }
  if (node.type === 'in valuelist' || node.type === 'not in valuelist') {
    return convertInValues(node)
  }
  // Non-convertible types - return undefined to skip optimization
  return undefined
}

/**
 * Converts a binary expression to filter format
 *
 * @param {BinaryNode} node
 * @returns {ParquetQueryFilter | undefined}
 */
function convertBinary({ op, left, right }) {
  if (op === 'AND') {
    const leftFilter = convertExpr(left)
    const rightFilter = convertExpr(right)
    if (!leftFilter || !rightFilter) return undefined
    return { $and: [leftFilter, rightFilter] }
  }

  if (op === 'OR') {
    const leftFilter = convertExpr(left)
    const rightFilter = convertExpr(right)
    if (!leftFilter || !rightFilter) return undefined
    return { $or: [leftFilter, rightFilter] }
  }

  // LIKE is not supported by hyparquet filters
  if (op === 'LIKE') {
    return undefined
  }

  // Comparison operators: need identifier on one side and literal on the other
  const { column, value, flipped } = extractColumnAndValue(left, right)
  if (!column || value === undefined) return undefined

  // Map SQL operator to MongoDB operator
  const mongoOp = mapOperator(op, flipped)
  if (!mongoOp) return undefined

  return { [column]: { [mongoOp]: value } }
}

/**
 * Extracts column name and literal value from binary operands.
 * Handles both "column op value" and "value op column" patterns.
 *
 * @param {ExprNode} left
 * @param {ExprNode} right
 * @returns {{ column: string | undefined, value: any, flipped: boolean }}
 */
function extractColumnAndValue(left, right) {
  // column op value
  if (left.type === 'identifier' && right.type === 'literal') {
    return { column: left.name, value: right.value, flipped: false }
  }
  // value op column (flipped)
  if (left.type === 'literal' && right.type === 'identifier') {
    return { column: right.name, value: left.value, flipped: true }
  }
  // Neither pattern matches
  return { column: undefined, value: undefined, flipped: false }
}

/**
 * Maps SQL operator to MongoDB operator, accounting for flipped operands
 *
 * @param {BinaryOp} op
 * @param {boolean} flipped - true if operands were swapped (value op column)
 * @returns {string | undefined}
 */
function mapOperator(op, flipped) {
  // Symmetric operators (same when flipped)
  if (op === '=') return '$eq'
  if (op === '!=' || op === '<>') return '$ne'

  // Asymmetric operators (flip when operands are swapped)
  if (flipped) {
    // "5 > column" means "column < 5"
    if (op === '<') return '$gt'
    if (op === '<=') return '$gte'
    if (op === '>') return '$lt'
    if (op === '>=') return '$lte'
  } else {
    if (op === '<') return '$lt'
    if (op === '<=') return '$lte'
    if (op === '>') return '$gt'
    if (op === '>=') return '$gte'
  }

  return undefined
}

/**
 * Converts BETWEEN expression to filter format
 *
 * @param {BetweenNode} node
 * @returns {ParquetQueryFilter | undefined}
 */
function convertBetween(node) {
  if (node.expr.type !== 'identifier') return undefined
  if (node.lower.type !== 'literal') return undefined
  if (node.upper.type !== 'literal') return undefined

  const column = node.expr.name
  const lower = node.lower.value
  const upper = node.upper.value

  if (node.type === 'not between') {
    // NOT BETWEEN: value < lower OR value > upper
    return {
      $or: [
        { [column]: { $lt: lower } },
        { [column]: { $gt: upper } },
      ],
    }
  }

  // BETWEEN: value >= lower AND value <= upper
  return {
    $and: [
      { [column]: { $gte: lower } },
      { [column]: { $lte: upper } },
    ],
  }
}

/**
 * Converts IN/NOT IN value list expression to filter format
 *
 * @param {InValuesNode} node
 * @returns {ParquetQueryFilter | undefined}
 */
function convertInValues(node) {
  if (node.expr.type !== 'identifier') return undefined

  // All values must be literals
  const values = []
  for (const val of node.values) {
    if (val.type !== 'literal') return undefined
    values.push(val.value)
  }

  const column = node.expr.name
  const mongoOp = node.type === 'not in valuelist' ? '$nin' : '$in'

  return { [column]: { [mongoOp]: values } }
}
