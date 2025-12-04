/**
 * @import {AsyncRow, ExprNode, OrderByItem, SqlPrimitive} from '../types.js'
 */

/**
 * Compares two values for a single ORDER BY term, handling nulls and direction
 *
 * @param {SqlPrimitive} a
 * @param {SqlPrimitive} b
 * @param {OrderByItem} term
 * @returns {number} comparison result
 */
export function compareForTerm(a, b, term) {
  const aIsNull = a == null
  const bIsNull = b == null

  if (aIsNull || bIsNull) {
    if (aIsNull && bIsNull) return 0
    const nullsFirst = term.nulls !== 'LAST'
    if (aIsNull) return nullsFirst ? -1 : 1
    return nullsFirst ? 1 : -1
  }

  // Compare non-null values
  if (a === b) return 0

  const primitives = ['number', 'bigint', 'boolean', 'string']
  let cmp
  if (primitives.includes(typeof a) && primitives.includes(typeof b)) {
    cmp = a < b ? -1 : a > b ? 1 : 0
  } else {
    const aa = String(a)
    const bb = String(b)
    cmp = aa < bb ? -1 : aa > bb ? 1 : 0
  }

  return term.direction === 'DESC' ? -cmp : cmp
}

/**
 * Collects and materialize all results from an async row generator into an array
 *
 * @param {AsyncGenerator<AsyncRow>} asyncRows
 * @returns {Promise<Record<string, SqlPrimitive>[]>} array of all yielded values
 */
export async function collect(asyncRows) {
  /** @type {Record<string, SqlPrimitive>[]} */
  const results = []
  for await (const asyncRow of asyncRows) {
    /** @type {Record<string, SqlPrimitive>} */
    const item = {}
    for (const [key, cell] of Object.entries(asyncRow)) {
      item[key] = await cell()
    }
    results.push(item)
  }
  return results
}

/**
 * Generates a default alias for a derived column expression
 *
 * @param {ExprNode} expr - the expression node
 * @returns {string} the generated alias
 */
export function defaultDerivedAlias(expr) {
  if (expr.type === 'identifier') {
    // For qualified names like 'users.name', use just the column part as alias
    if (expr.name.includes('.')) {
      return expr.name.split('.').pop()
    }
    return expr.name
  }
  if (expr.type === 'literal') {
    return String(expr.value)
  }
  if (expr.type === 'cast') {
    return defaultDerivedAlias(expr.expr) + '_as_' + expr.toType
  }
  if (expr.type === 'unary') {
    return expr.op + '_' + defaultDerivedAlias(expr.argument)
  }
  if (expr.type === 'binary') {
    return defaultDerivedAlias(expr.left) + '_' + expr.op + '_' + defaultDerivedAlias(expr.right)
  }
  if (expr.type === 'function') {
    return expr.name.toLowerCase() + '_' + expr.args.map(defaultDerivedAlias).join('_')
  }
  return 'expr'
}
