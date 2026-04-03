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
      return expr.funcName.toLowerCase() + '_all'
    }
    return expr.funcName.toLowerCase() + '_' + expr.args.map(derivedAlias).join('_')
  }
  if (expr.type === 'interval') {
    return `interval_${expr.value}_${expr.unit.toLowerCase()}`
  }
  return 'expr'
}
