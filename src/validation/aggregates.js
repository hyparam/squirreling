import { isAggregateFunc } from './functions.js'
import { ParseError } from './parseErrors.js'

/**
 * @import { ExprNode, FunctionNode } from '../types.js'
 */

/**
 * Finds the first aggregate function call in an expression tree.
 * Does not recurse into subqueries (they have their own aggregate scope).
 *
 * @param {ExprNode | undefined} expr
 * @returns {FunctionNode | undefined}
 */
export function findAggregate(expr) {
  if (!expr) return undefined
  if (expr.type === 'function' && isAggregateFunc(expr.name.toUpperCase())) {
    return expr
  }
  if (expr.type === 'binary') {
    return findAggregate(expr.left) || findAggregate(expr.right)
  }
  if (expr.type === 'unary') {
    return findAggregate(expr.argument)
  }
  if (expr.type === 'cast') {
    return findAggregate(expr.expr)
  }
  if (expr.type === 'case') {
    if (expr.caseExpr) {
      const found = findAggregate(expr.caseExpr)
      if (found) return found
    }
    for (const when of expr.whenClauses) {
      const found = findAggregate(when.condition) || findAggregate(when.result)
      if (found) return found
    }
    return findAggregate(expr.elseResult)
  }
  if (expr.type === 'in valuelist') {
    const found = findAggregate(expr.expr)
    if (found) return found
    for (const val of expr.values) {
      const found = findAggregate(val)
      if (found) return found
    }
  }
  // Subqueries have their own aggregate scope
  return undefined
}

/**
 * Throws a ParseError if the expression contains an aggregate function.
 *
 * @param {ExprNode | undefined} expr - The expression to check
 * @param {string} clause - The clause name (e.g., 'WHERE', 'JOIN ON', 'GROUP BY')
 */
export function expectNoAggregate(expr, clause) {
  const agg = findAggregate(expr)
  if (agg) {
    const hint = clause === 'WHERE' ? '. Use HAVING instead.' : ''
    throw new ParseError({
      ...agg,
      message: `Aggregate function ${agg.name.toUpperCase()} is not allowed in ${clause} clause${hint}`,
    })
  }
}
