/** @import { ExprNode } from '../types.js' */

/**
 * Walks an ON expression, flattening top-level AND conjuncts. Non-AND nodes
 * are pushed verbatim. Used to expose individual predicates so equi-keys can
 * be lifted out of an AND chain.
 *
 * @param {ExprNode} node
 * @param {ExprNode[]} out
 */
export function collectConjuncts(node, out) {
  if (node.type === 'binary' && node.op === 'AND') {
    collectConjuncts(node.left, out)
    collectConjuncts(node.right, out)
    return
  }
  out.push(node)
}

/**
 * @param {ExprNode | undefined} left
 * @param {ExprNode} right
 * @returns {ExprNode}
 */
export function andPredicate(left, right) {
  return left
    ? { type: 'binary', op: 'AND', left, right, positionStart: left.positionStart, positionEnd: right.positionEnd }
    : right
}
