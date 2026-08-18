import { collectColumnsFromExpr } from '../plan/columns.js'

/**
 * @import { ExecuteContext, ExprNode, IdentifierNode } from '../types.js'
 */

/**
 * Returns whether an expression reads a qualified identifier from row scope.
 * A current-scope prefix is ambiguous only when it is also a child column and
 * could therefore mean struct-field access.
 *
 * @param {ExprNode} expression
 * @param {readonly string[]} columns
 * @param {ExecuteContext} context
 * @returns {boolean}
 */
export function referencesRowScope(expression, columns, context) {
  /** @type {IdentifierNode[]} */
  const identifiers = []
  collectColumnsFromExpr(expression, identifiers)
  return identifiers.some(function scopedIdentifier(identifier) {
    return Boolean(identifier.prefix && (
      context.outerAliases?.has(identifier.prefix) ||
      context.scope?.includes(identifier.prefix) && columns.includes(identifier.prefix)
    ))
  })
}
