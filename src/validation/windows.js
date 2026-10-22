/** @import { ExprNode, WindowFunctionNode } from '../types.js' */

/**
 * @param {ExprNode | undefined} expr
 * @returns {WindowFunctionNode | undefined}
 */
export function findWindow(expr) {
  if (!expr) return undefined
  if (expr.type === 'window') return expr
  if (expr.type === 'binary') return findWindow(expr.left) || findWindow(expr.right)
  if (expr.type === 'unary') return findWindow(expr.argument)
  if (expr.type === 'function') {
    for (const arg of expr.args) {
      const found = findWindow(arg)
      if (found) return found
    }
    return undefined
  }
  if (expr.type === 'cast') return findWindow(expr.expr)
  if (expr.type === 'subscript') return findWindow(expr.expr) || findWindow(expr.index)
  if (expr.type === 'in valuelist') {
    const found = findWindow(expr.expr)
    if (found) return found
    for (const val of expr.values) {
      const f = findWindow(val)
      if (f) return f
    }
    return undefined
  }
  if (expr.type === 'case') {
    if (expr.caseExpr) {
      const f = findWindow(expr.caseExpr)
      if (f) return f
    }
    for (const w of expr.whenClauses) {
      const f = findWindow(w.condition) || findWindow(w.result)
      if (f) return f
    }
    if (expr.elseResult) return findWindow(expr.elseResult)
  }
  return undefined
}
