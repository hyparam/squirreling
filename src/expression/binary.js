import { stringify } from '../execute/utils.js'

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

  if (op === 'LIKE' || op === 'ILIKE') {
    // Objects, arrays, and Dates stringify as JSON, the same coercion as
    // CAST(x AS VARCHAR), so `x LIKE p` and `CAST(x AS VARCHAR) LIKE p` agree.
    // String() would collapse every object to '[object Object]', making LIKE
    // a silent never-match on JSON-typed columns.
    const str = typeof a === 'object' ? stringify(a) : String(a)
    const pattern = String(b)
    return matchLike(str, pattern, op === 'ILIKE')
  }

  return null
}

/**
 * Match a LIKE pattern. Leading and trailing `%` drop the anchor on that side
 * instead of becoming `.*`: an anchored `^.*x.*$` backtracks from every start
 * position, which is quadratic on long strings, while an unanchored search
 * uses the engine's substring scan. Case-sensitive patterns with no inner
 * wildcard skip regex entirely. V8 caches compiled regexes by source and
 * flags, so building one per call is cheap.
 *
 * @param {string} str
 * @param {string} pattern
 * @param {boolean} ignoreCase
 * @returns {boolean}
 */
function matchLike(str, pattern, ignoreCase) {
  let start = 0
  let end = pattern.length
  while (start < end && pattern[start] === '%') start++
  while (end > start && pattern[end - 1] === '%') end--
  const anchorStart = start === 0
  const anchorEnd = end === pattern.length
  const body = pattern.slice(start, end)
  if (!anchorStart && !anchorEnd && !body) return true
  if (!ignoreCase && !/[%_]/.test(body)) {
    if (anchorStart && anchorEnd) return str === body
    if (anchorStart) return str.startsWith(body)
    if (anchorEnd) return str.endsWith(body)
    return str.includes(body)
  }
  const source = body
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    .replace(/%+/g, '.*')
    .replace(/_/g, '.')
  return new RegExp(`${anchorStart ? '^' : ''}${source}${anchorEnd ? '$' : ''}`, ignoreCase ? 'is' : 's').test(str)
}
