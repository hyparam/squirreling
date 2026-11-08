import { isComparisonOp, stringLiteral } from '../expression/binary.js'
import { toDate } from '../expression/date.js'

/**
 * @import { ExprNode, LiteralNode, RelationSchema } from '../types.js'
 * @import { ScanNode } from '../plan/types.js'
 */

/**
 * Translate local conjuncts into physical column names. Keep subqueries and
 * references outside this scan in the engine. Match scope names exactly, as
 * expression evaluation does, including quoted identifiers and case.
 *
 * A `LIKE 'prefix%'` on a column the schema types as string also gains an
 * implied range (`>= prefix AND < next`) so sources can prune on min/max
 * statistics. The LIKE stays in the predicate, so the range only narrows.
 *
 * A string literal compared with a column the schema types as date becomes
 * `CAST(literal AS TIMESTAMP)`, the typing the engine applies at evaluation,
 * so sources compare and prune with a timestamp rather than a string.
 *
 * @param {ScanNode} plan
 * @param {string[]} columns
 * @param {RelationSchema} [schema]
 * @returns {{ where?: ExprNode, residual?: ExprNode }}
 */
export function scanFilter(plan, columns, schema) {
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
    return safe ? { where: withPrefixRange(withTimestampLiterals(where, schema), schema) } : { residual: node }
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

/**
 * Add the range implied by a case-sensitive `col LIKE 'prefix%'`. Only for
 * string-typed columns: LIKE stringifies other values, so `n LIKE '1%'` matches
 * 10 while `n >= '1'` compares numerically. Only for printable ASCII prefixes,
 * where UTF-16 order (engine comparisons) and UTF-8 byte order (parquet
 * statistics) agree and the successor of the last character stays ASCII.
 *
 * @param {ExprNode} node
 * @param {RelationSchema} [schema]
 * @returns {ExprNode}
 */
function withPrefixRange(node, schema) {
  if (node.type !== 'binary' || node.op !== 'LIKE') return node
  const { left, right } = node
  if (left.type !== 'identifier' || right.type !== 'literal' || typeof right.value !== 'string') return node
  const field = schema?.fields.find(f => f.name === left.name)
  if (field?.dataType.type !== 'string') return node
  const prefix = /^[\x20-\x24\x26-\x5e\x60-\x7e]*/.exec(right.value)?.[0] ?? ''
  if (!prefix || prefix === right.value) return node
  const upper = prefix.slice(0, -1) + String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1)
  /** @type {ExprNode} */
  const lower = { ...node, op: '>=', right: { ...right, value: prefix } }
  /** @type {ExprNode} */
  const below = { ...node, op: '<', right: { ...right, value: upper } }
  return combine(combine(lower, below), node)
}

/**
 * Casts string literals compared with date-typed columns to TIMESTAMP.
 *
 * @param {ExprNode} node
 * @param {RelationSchema} [schema]
 * @returns {ExprNode}
 */
function withTimestampLiterals(node, schema) {
  if (!schema) return node
  /**
   * @param {ExprNode} expr
   * @returns {boolean}
   */
  function isDateColumn(expr) {
    if (expr.type !== 'identifier') return false
    return schema?.fields.find(f => f.name === expr.name)?.dataType.type === 'date'
  }
  /**
   * @param {ExprNode} expr
   * @returns {ExprNode}
   */
  function rewrite(expr) {
    if (expr.type === 'binary') {
      if (expr.op === 'AND' || expr.op === 'OR') {
        return { ...expr, left: rewrite(expr.left), right: rewrite(expr.right) }
      }
      if (isComparisonOp(expr.op)) {
        const leftLiteral = timestampLiteral(expr.left)
        const rightLiteral = timestampLiteral(expr.right)
        if (rightLiteral && isDateColumn(expr.left)) return { ...expr, right: castTimestamp(rightLiteral) }
        if (leftLiteral && isDateColumn(expr.right)) return { ...expr, left: castTimestamp(leftLiteral) }
      }
      return expr
    }
    if (expr.type === 'unary' && expr.op === 'NOT') return { ...expr, argument: rewrite(expr.argument) }
    if (expr.type === 'in valuelist' && isDateColumn(expr.expr)) {
      return {
        ...expr,
        values: expr.values.map(value => {
          const literal = timestampLiteral(value)
          return literal ? castTimestamp(literal) : value
        }),
      }
    }
    return expr
  }
  return rewrite(node)
}

/**
 * A string literal that parses as a timestamp. One that does not stays a
 * string, so the engine reports it instead of a cast quietly yielding null.
 *
 * @param {ExprNode} node
 * @returns {LiteralNode | undefined}
 */
function timestampLiteral(node) {
  const literal = stringLiteral(node)
  return literal && toDate(literal.value) ? literal : undefined
}

/**
 * @param {LiteralNode} literal
 * @returns {ExprNode}
 */
function castTimestamp(literal) {
  return {
    type: 'cast',
    expr: literal,
    toType: 'TIMESTAMP',
    positionStart: literal.positionStart,
    positionEnd: literal.positionEnd,
  }
}
