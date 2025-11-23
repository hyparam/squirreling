/**
 * Converts squirreling WHERE AST to hyparquet MongoDB-style filter format.
 * Uses all-or-nothing strategy: returns undefined if any part can't be converted.
 *
 * @import { ParquetQueryFilter } from 'hyparquet'
 * @import { ParquetQueryOperator } from 'hyparquet/src/types.js'
 * @import { BinaryNode, BinaryOp, ComparisonOp, ExprNode, InValuesNode, SqlPrimitive } from '../types.js'
 */

/**
 * Converts a squirreling WHERE clause AST to hyparquet filter format.
 * Returns undefined if the expression cannot be fully converted.
 *
 * @param {ExprNode | undefined} where - the WHERE clause AST
 * @returns {ParquetQueryFilter | undefined}
 */
export function whereToParquetFilter(where) {
  if (!where) return undefined
  return convertExpr(where, false)
}

/**
 * Converts an expression node to filter format
 *
 * @param {ExprNode} node
 * @param {boolean} negate
 * @returns {ParquetQueryFilter | undefined}
 */
function convertExpr(node, negate) {
  if (node.type === 'unary' && node.op === 'NOT') {
    return convertExpr(node.argument, !negate)
  }
  if (node.type === 'binary') {
    return convertBinary(node, negate)
  }
  if (node.type === 'in valuelist') {
    return convertInValues(node, negate)
  }
  if (node.type === 'cast') {
    return convertExpr(node.expr, negate)
  }
  // Non-convertible types - return undefined to skip optimization
  return undefined
}

/**
 * Converts a binary expression to filter format
 *
 * @param {BinaryNode} node
 * @param {boolean} negate
 * @returns {ParquetQueryFilter | undefined}
 */
function convertBinary({ op, left, right }, negate) {
  if (op === 'AND') {
    const leftFilter = convertExpr(left, negate)
    const rightFilter = convertExpr(right, negate)
    if (!leftFilter || !rightFilter) return
    return negate
      ? { $or: [leftFilter, rightFilter] }
      : { $and: [leftFilter, rightFilter] }
  }
  if (op === 'OR') {
    const leftFilter = convertExpr(left, false)
    const rightFilter = convertExpr(right, false)
    if (!leftFilter || !rightFilter) return
    return negate
      ? { $nor: [leftFilter, rightFilter] }
      : { $or: [leftFilter, rightFilter] }
  }

  // LIKE is not supported by hyparquet filters
  if (op === 'LIKE') return

  // Comparison operators: need identifier on one side and literal on the other
  const { column, value, flipped } = extractColumnAndValue(left, right)
  if (!column || value === undefined) return

  // Map SQL operator to MongoDB operator
  const mongoOp = mapOperator(op, flipped, negate)
  if (!mongoOp) return
  return { [column]: { [mongoOp]: value } }
}

/**
 * Extracts column name and literal value from binary operands.
 * Handles both "column op value" and "value op column" patterns.
 *
 * @param {ExprNode} left
 * @param {ExprNode} right
 * @returns {{ column: string | undefined, value: SqlPrimitive, flipped: boolean }}
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
 * @param {boolean} negate - true if the expression is negated
 * @returns {keyof ParquetQueryOperator}
 */
function mapOperator(op, flipped, negate) {
  if (!isComparisonOp(op)) return

  /** @type {ComparisonOp} */
  let mappedOp = op
  if (negate) mappedOp = neg(mappedOp)
  if (flipped) mappedOp = flip(mappedOp)
  if (mappedOp === '<') return '$lt'
  if (mappedOp === '<=') return '$lte'
  if (mappedOp === '>') return '$gt'
  if (mappedOp === '>=') return '$gte'
  if (mappedOp === '=') return '$eq'
  return '$ne'
}
/**
 * @param {ComparisonOp} op
 * @returns {ComparisonOp}
 */
function neg(op) {
  if (op === '<') return '>='
  if (op === '<=') return '>'
  if (op === '>') return '<='
  if (op === '>=') return '<'
  if (op === '=') return '!='
  return '='
}

/**
 * @param {ComparisonOp} op
 * @returns {ComparisonOp}
 */
function flip(op) {
  if (op === '<') return '>'
  if (op === '<=') return '>='
  if (op === '>') return '<'
  if (op === '>=') return '<='
  return op
}

/**
 * @param {string} op
 * @returns {op is ComparisonOp}
 */
function isComparisonOp(op) {
  return ['=', '!=', '<>', '<', '>', '<=', '>='].includes(op)
}

/**
 * Converts IN/NOT IN value list expression to filter format
 *
 * @param {InValuesNode} node
 * @param {boolean} negate
 * @returns {ParquetQueryFilter | undefined}
 */
function convertInValues(node, negate) {
  if (node.expr.type !== 'identifier') return

  // All values must be literals
  const values = []
  for (const val of node.values) {
    if (val.type !== 'literal') return
    values.push(val.value)
  }

  return { [node.expr.name]: { [negate ? '$nin' : '$in']: values } }
}
