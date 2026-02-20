/**
 * @import { ExprNode } from '../types.js'
 */

/**
 * Generates a default alias for a derived column expression
 *
 * @param {ExprNode} expr - the expression node
 * @returns {string} the generated alias
 */
export function derivedAlias(expr) {
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
    return derivedAlias(expr.expr) + '_as_' + expr.toType
  }
  if (expr.type === 'unary') {
    return expr.op + '_' + derivedAlias(expr.argument)
  }
  if (expr.type === 'binary') {
    return derivedAlias(expr.left) + '_' + expr.op + '_' + derivedAlias(expr.right)
  }
  if (expr.type === 'function') {
    // Handle aggregate functions with star (COUNT(*) -> count_all)
    if (expr.args.length === 1 && expr.args[0].type === 'star') {
      return expr.name.toLowerCase() + '_all'
    }
    return expr.name.toLowerCase() + '_' + expr.args.map(derivedAlias).join('_')
  }
  if (expr.type === 'interval') {
    return `interval_${expr.value}_${expr.unit.toLowerCase()}`
  }
  return 'expr'
}
