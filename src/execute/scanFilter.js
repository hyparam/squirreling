/**
 * @import { ExprNode } from '../types.js'
 * @import { ScanNode } from '../plan/types.js'
 */

/**
 * Translate local conjuncts into physical column names. Keep subqueries and
 * references outside this scan in the engine. Match scope names exactly, as
 * expression evaluation does, including quoted identifiers and case.
 *
 * @param {ScanNode} plan
 * @param {string[]} columns
 * @returns {{ where?: ExprNode, residual?: ExprNode }}
 */
export function scanFilter(plan, columns) {
  const scope = plan.alias ?? plan.table

  /**
   * @param {ExprNode} node
   * @returns {{ where?: ExprNode, residual?: ExprNode }}
   */
  function split(node) {
    if (!node) return {}
    if (node.type === 'binary' && node.op === 'AND') {
      const left = split(node.left)
      const right = split(node.right)
      return {
        where: combine(left.where, right.where),
        residual: combine(left.residual, right.residual),
      }
    }
    let safe = true
    /**
     * @param {ExprNode} expr
     * @returns {ExprNode}
     */
    function rewrite(expr) {
      if (!expr) return expr
      if (expr.type === 'identifier') {
        if (expr.prefix && expr.prefix !== scope || !columns.includes(expr.name) ||
            expr.prefix && columns.includes(`${expr.prefix}.${expr.name}`)) {
          safe = false
          return expr
        }
        if (!expr.prefix) return expr
        const bare = { ...expr }
        delete bare.prefix
        return bare
      }
      if (expr.type === 'literal' || expr.type === 'interval') return expr
      if (expr.type === 'binary') return { ...expr, left: rewrite(expr.left), right: rewrite(expr.right) }
      if (expr.type === 'unary') return { ...expr, argument: rewrite(expr.argument) }
      if (expr.type === 'cast') return { ...expr, expr: rewrite(expr.expr) }
      if (expr.type === 'function') return { ...expr, args: expr.args.map(rewrite), ...expr.filter ? { filter: rewrite(expr.filter) } : {} }
      if (expr.type === 'in valuelist') return { ...expr, expr: rewrite(expr.expr), values: expr.values.map(rewrite) }
      if (expr.type === 'subscript') return { ...expr, expr: rewrite(expr.expr), index: rewrite(expr.index) }
      if (expr.type === 'case') return {
        ...expr,
        caseExpr: rewrite(expr.caseExpr),
        whenClauses: expr.whenClauses.map(w => ({ ...w, condition: rewrite(w.condition), result: rewrite(w.result) })),
        elseResult: rewrite(expr.elseResult),
      }
      safe = false
      return expr
    }
    const where = rewrite(node)
    return safe ? { where } : { residual: node }
  }
  return split(plan.hints.where)
}

/**
 * @param {ExprNode} left
 * @param {ExprNode} right
 * @returns {ExprNode}
 */
function combine(left, right) {
  if (!left) return right
  if (!right) return left
  return { type: 'binary', op: 'AND', left, right, positionStart: left.positionStart, positionEnd: right.positionEnd }
}
