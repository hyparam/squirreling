/**
 * @import { BinaryOp, SqlPrimitive } from '../types.js'
 */

/**
 * Applies a binary operator to two values, handling nulls according to SQL semantics
 *
 * @param {BinaryOp} op
 * @param {SqlPrimitive} a
 * @param {SqlPrimitive} b
 * @returns {SqlPrimitive}
 */
export function applyBinaryOp(op, a, b) {
  // Arithmetic operators return null if either operand is null
  if (op === '+' || op === '-' || op === '*' || op === '/' || op === '%') {
    if (a == null || b == null) return null
    const numA = Number(a)
    const numB = Number(b)
    if (op === '+') return numA + numB
    if (op === '-') return numA - numB
    if (op === '*') return numA * numB
    if (op === '/') return numB === 0 ? null : numA / numB
    if (op === '%') return numB === 0 ? null : numA % numB
  }

  // Comparison and logical operators
  if (a == null || b == null) {
    return false
  }
  if (op === 'AND') return Boolean(a) && Boolean(b)
  if (op === 'OR') return Boolean(a) || Boolean(b)
  // Compare Date values by their time so distinct instances for the same
  // instant are equal, matching SQL TIMESTAMP semantics rather than JS identity.
  if (a instanceof Date && b instanceof Date) {
    const at = a.getTime()
    const bt = b.getTime()
    if (op === '!=' || op === '<>') return at !== bt
    if (op === '=' || op === '==') return at === bt
    if (op === '<') return at < bt
    if (op === '<=') return at <= bt
    if (op === '>') return at > bt
    if (op === '>=') return at >= bt
  }
  if (op === '!=' || op === '<>') return a != b
  if (op === '=' || op === '==') return a == b
  if (op === '<') return a < b
  if (op === '<=') return a <= b
  if (op === '>') return a > b
  if (op === '>=') return a >= b

  if (op === 'LIKE') {
    const str = String(a)
    const pattern = String(b)
    const regexPattern = pattern
      .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      .replace(/%/g, '.*')
      .replace(/_/g, '.')
    const regex = new RegExp(`^${regexPattern}$`, 'is')
    return regex.test(str)
  }

  return null
}
