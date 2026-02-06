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
  if (op === '!=' || op === '<>') return a != b
  if (op === '=') return a == b
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
    const regex = new RegExp(`^${regexPattern}$`, 'i')
    return regex.test(str)
  }

  return null
}
