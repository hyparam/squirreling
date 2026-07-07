/**
 * @import { ExprNode, Statement } from '../types.js'
 */

/**
 * Collect every external table referenced from FROM and JOIN clauses in a
 * parsed statement, including those inside subqueries (IN, EXISTS, derived
 * tables, scalar subqueries) and the branches of compound (UNION /
 * INTERSECT / EXCEPT) queries. CTE names defined by an enclosing WITH are
 * skipped, including across sibling CTEs and nested WITHs. The result is
 * the set of names a caller would need to provide as `tables` to
 * `executeSql`.
 *
 * Returned in first-seen order with duplicates removed. Names are returned
 * in the original case they were written in the query.
 *
 * @param {Statement} statement
 * @returns {string[]}
 */
export function extractTables(statement) {
  /** @type {Set<string>} */
  const refs = new Set()
  walkStatement(statement, new Set(), refs)
  return [...refs]
}

/**
 * @param {Statement} stmt
 * @param {Set<string>} cteScope - lowercased CTE names visible at this point
 * @param {Set<string>} refs
 * @returns {void}
 */
function walkStatement(stmt, cteScope, refs) {
  if (stmt.type === 'with') {
    const scope = new Set(cteScope)
    for (const cte of stmt.ctes) {
      walkStatement(cte.query, scope, refs)
      scope.add(cte.name.toLowerCase())
    }
    walkStatement(stmt.query, scope, refs)
    return
  }
  if (stmt.type === 'compound') {
    walkStatement(stmt.left, cteScope, refs)
    walkStatement(stmt.right, cteScope, refs)
    for (const o of stmt.orderBy) walkExpr(o.expr, cteScope, refs)
    return
  }
  // select
  if (!stmt.from) {
    // FROM-less SELECT (e.g. `SELECT 1`), no source tables
  } else if (stmt.from.type === 'table') {
    if (!cteScope.has(stmt.from.table.toLowerCase())) refs.add(stmt.from.table)
  } else if (stmt.from.type === 'subquery') {
    walkStatement(stmt.from.query, cteScope, refs)
  } else {
    for (const a of stmt.from.args) walkExpr(a, cteScope, refs)
  }
  for (const j of stmt.joins) {
    if (j.fromFunction) {
      for (const a of j.fromFunction.args) walkExpr(a, cteScope, refs)
    } else if (j.subquery) {
      walkStatement(j.subquery.query, cteScope, refs)
    } else if (!cteScope.has(j.table.toLowerCase())) {
      refs.add(j.table)
    }
    if (j.on) walkExpr(j.on, cteScope, refs)
  }
  for (const c of stmt.columns) {
    if (c.type === 'derived') walkExpr(c.expr, cteScope, refs)
  }
  if (stmt.where) walkExpr(stmt.where, cteScope, refs)
  for (const g of stmt.groupBy) walkExpr(g, cteScope, refs)
  if (stmt.having) walkExpr(stmt.having, cteScope, refs)
  for (const o of stmt.orderBy) walkExpr(o.expr, cteScope, refs)
}

/**
 * @param {ExprNode} expr
 * @param {Set<string>} cteScope
 * @param {Set<string>} refs
 * @returns {void}
 */
function walkExpr(expr, cteScope, refs) {
  switch (expr.type) {
  case 'unary':
    walkExpr(expr.argument, cteScope, refs)
    return
  case 'binary':
    walkExpr(expr.left, cteScope, refs)
    walkExpr(expr.right, cteScope, refs)
    return
  case 'function':
    for (const a of expr.args) walkExpr(a, cteScope, refs)
    if (expr.filter) walkExpr(expr.filter, cteScope, refs)
    return
  case 'window':
    for (const a of expr.args) walkExpr(a, cteScope, refs)
    for (const p of expr.partitionBy) walkExpr(p, cteScope, refs)
    for (const o of expr.orderBy) walkExpr(o.expr, cteScope, refs)
    return
  case 'cast':
    walkExpr(expr.expr, cteScope, refs)
    return
  case 'in':
    walkExpr(expr.expr, cteScope, refs)
    walkStatement(expr.subquery, cteScope, refs)
    return
  case 'in valuelist':
    walkExpr(expr.expr, cteScope, refs)
    for (const v of expr.values) walkExpr(v, cteScope, refs)
    return
  case 'subscript':
    walkExpr(expr.expr, cteScope, refs)
    walkExpr(expr.index, cteScope, refs)
    return
  case 'exists':
  case 'not exists':
    walkStatement(expr.subquery, cteScope, refs)
    return
  case 'case':
    if (expr.caseExpr) walkExpr(expr.caseExpr, cteScope, refs)
    for (const w of expr.whenClauses) {
      walkExpr(w.condition, cteScope, refs)
      walkExpr(w.result, cteScope, refs)
    }
    if (expr.elseResult) walkExpr(expr.elseResult, cteScope, refs)
    return
  case 'subquery':
    walkStatement(expr.subquery, cteScope, refs)
  }
  // 'literal' / 'identifier' / 'interval' / 'star' are leaves with no children.
}
