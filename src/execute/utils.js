/**
 * Collects and materialize all results from an async row generator into an array
 *
 * @import {AsyncRow, ExprNode, SqlPrimitive} from '../types.js'
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
