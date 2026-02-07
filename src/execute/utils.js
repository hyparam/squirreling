/**
 * @import { AsyncCells, AsyncRow, ExprNode, OrderByItem, SqlPrimitive } from '../types.js'
 */

/**
 * Compares two values for a single ORDER BY term, handling nulls and direction
 *
 * @param {SqlPrimitive} a
 * @param {SqlPrimitive} b
 * @param {OrderByItem} term
 * @returns {number}
 */
export function compareForTerm(a, b, term) {
  const aIsNull = a == null
  const bIsNull = b == null

  if (aIsNull || bIsNull) {
    if (aIsNull && bIsNull) return 0
    const nullsFirst = term.nulls !== 'LAST'
    if (aIsNull) return nullsFirst ? -1 : 1
    return nullsFirst ? 1 : -1
  }

  // Compare non-null values
  if (a == b) return 0

  const primitives = ['number', 'bigint', 'boolean', 'string']
  let cmp
  if (primitives.includes(typeof a) && primitives.includes(typeof b)) {
    cmp = a < b ? -1 : a > b ? 1 : 0
  } else {
    const aa = String(a)
    const bb = String(b)
    cmp = aa < bb ? -1 : aa > bb ? 1 : 0
  }

  return term.direction === 'DESC' ? -cmp : cmp
}

/**
 * Collects and materialize all results from an async row generator into an array
 *
 * @param {AsyncGenerator<AsyncRow>} asyncRows
 * @returns {Promise<Record<string, SqlPrimitive>[]>} array of all yielded values
 */
export async function collect(asyncRows) {
  /** @type {Record<string, SqlPrimitive>[]} */
  const results = []
  for await (const asyncRow of asyncRows) {
    /** @type {Record<string, SqlPrimitive>} */
    const item = {}
    for (const key of asyncRow.columns) {
      item[key] = await asyncRow.cells[key]()
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
    // Handle aggregate functions with star (COUNT(*) -> count_all)
    if (expr.args.length === 1 && expr.args[0].type === 'identifier' && expr.args[0].name === '*') {
      return expr.name.toLowerCase() + '_all'
    }
    return expr.name.toLowerCase() + '_' + expr.args.map(defaultDerivedAlias).join('_')
  }
  if (expr.type === 'interval') {
    return `interval_${expr.value}_${expr.unit.toLowerCase()}`
  }
  return 'expr'
}

/**
 * @param {SqlPrimitive} value
 * @returns {string}
 */
export function stringify(value) {
  if (value == null) return 'NULL'
  return JSON.stringify(value, (_, val) => {
    if (typeof val === 'bigint') {
      return val <= Number.MAX_SAFE_INTEGER ? Number(val) : val.toString()
    }
    return val
  })
}

/**
 * Creates a stable string key for a row to enable deduplication
 *
 * @param {AsyncCells} cells
 * @returns {Promise<string>}
 */
export async function stableRowKey(cells) {
  const keys = Object.keys(cells).sort()
  /** @type {string[]} */
  const parts = []
  for (const k of keys) {
    const v = await cells[k]()
    parts.push(k + ':' + stringify(v))
  }
  return parts.join('|')
}
