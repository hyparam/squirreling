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

  // String concatenation returns null if either operand is null
  if (op === '||') {
    if (a == null || b == null) return null
    return String(a) + String(b)
  }

  // Logical operators use Kleene three-valued logic: null is UNKNOWN, which
  // only resolves when the other operand decides the answer on its own
  if (op === 'AND') {
    if (a != null && !a || b != null && !b) return false
    if (a == null || b == null) return null
    return true
  }
  if (op === 'OR') {
    if (a != null && Boolean(a) || b != null && Boolean(b)) return true
    if (a == null || b == null) return null
    return false
  }

  // A comparison with a null operand is UNKNOWN, not false. The difference is
  // invisible to a WHERE (both exclude the row) but not to a NOT above it,
  // which must keep UNKNOWN as UNKNOWN rather than flip false to true
  if (a == null || b == null) {
    return null
  }
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
