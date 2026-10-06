/**
 * @import { ExprNode, Field } from '../types.js'
 */

/**
 * Prove that a sort column has one value after an equality conjunct.
 * Require a declared scalar type: loose SQL equality on unknown/mixed values
 * does not imply a sort tie (both '01' and '1' can equal the number 1).
 *
 * @param {ExprNode | undefined} where
 * @param {ExprNode} sortKey - Projection aliases have already been resolved.
 * @param {readonly Field[]} fields
 * @param {string | undefined} sourceAlias - Only a single local table is in scope.
 * @returns {boolean}
 */
export function filterFixesSortKey(where, sortKey, fields, sourceAlias) {
  if (!where || sortKey.type !== 'identifier' || sortKey.prefix && sortKey.prefix !== sourceAlias) return false
  const field = fields.find(field => field.name === sortKey.name)
  if (!field) return false
  if (where.type === 'binary' && where.op === 'AND') {
    return filterFixesSortKey(where.left, sortKey, fields, sourceAlias) ||
      filterFixesSortKey(where.right, sortKey, fields, sourceAlias)
  }
  if (where.type === 'unary' && where.op === 'IS NULL') return sameColumn(where.argument)
  if (where.type !== 'binary' || where.op !== '=' && where.op !== '==') return false
  const literal = sameColumn(where.left) ? where.right : sameColumn(where.right) ? where.left : undefined
  if (literal?.type !== 'literal' || literal.value == null) return false
  const { value } = literal
  switch (field.dataType.type) {
  case 'string': return typeof value === 'string'
  case 'number': return typeof value === 'number'
  case 'boolean': return typeof value === 'boolean'
  case 'bigint': return typeof value === 'bigint' || typeof value === 'number' && Number.isSafeInteger(value)
  case 'date': return value instanceof Date
  default: return false
  }

  /**
   * @param {ExprNode} node
   * @returns {boolean}
   */
  function sameColumn(node) {
    return node.type === 'identifier' && node.name === field.name && (!node.prefix || node.prefix === sourceAlias)
  }
}
