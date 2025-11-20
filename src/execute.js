/**
 * @import { SelectAst, ExprNode, AggregateColumn, OrderByItem, Row, SqlPrimitive } from './types.js'
 */

import { parseSql } from './parse.js'

/**
 * Evaluates an expression node against a row of data
 * @param {ExprNode} node - The expression node to evaluate
 * @param {Row} row - The data row to evaluate against
 * @returns {SqlPrimitive | boolean} The result of the evaluation
 */
function evaluateExpr(node, row) {
  if (node.type === 'literal') {
    return node.value
  }

  if (node.type === 'identifier') {
    return row[node.name]
  }

  if (node.type === 'unary') {
    if (node.op === 'NOT') {
      const val = evaluateExpr(node.argument, row)
      return !val
    }
    if (node.op === 'IS NULL') {
      const val = evaluateExpr(node.argument, row)
      return val === null || val === undefined
    }
    if (node.op === 'IS NOT NULL') {
      const val = evaluateExpr(node.argument, row)
      return val !== null && val !== undefined
    }
    throw new Error('Unsupported unary operator ' + (/** @type {any} */ (node).op))
  }

  if (node.type === 'binary') {
    if (node.op === 'AND') {
      const leftVal = evaluateExpr(node.left, row)
      if (!leftVal) return false
      return Boolean(evaluateExpr(node.right, row))
    }

    if (node.op === 'OR') {
      const leftVal = evaluateExpr(node.left, row)
      if (leftVal) return true
      return Boolean(evaluateExpr(node.right, row))
    }

    const left = evaluateExpr(node.left, row)
    const right = evaluateExpr(node.right, row)

    if (node.op === '=') return left === right
    if (node.op === '!=' || node.op === '<>') return left !== right
    if (node.op === '<') return left < right
    if (node.op === '>') return left > right
    if (node.op === '<=') return left <= right
    if (node.op === '>=') return left >= right

    throw new Error('Unsupported binary operator ' + node.op)
  }

  throw new Error('Unknown expression node type ' + (/** @type {any} */ (node).type))
}

/**
 * Evaluates an aggregate function over a set of rows
 * @param {AggregateColumn} col - The aggregate column definition
 * @param {Row[]} rows - The rows to aggregate
 * @returns {number | null} The aggregated result
 */
function evaluateAggregate(col, rows) {
  const { func } = col
  const { arg } = col

  if (func === 'COUNT') {
    if (arg.kind === 'star') return rows.length
    const field = arg.column
    let count = 0
    for (let i = 0; i < rows.length; i += 1) {
      const v = rows[i][field]
      if (v !== null && v !== undefined) {
        count += 1
      }
    }
    return count
  }

  if (func === 'SUM' || func === 'AVG' || func === 'MIN' || func === 'MAX') {
    if (arg.kind === 'star') {
      throw new Error(func + '(*) is not supported, use a column name')
    }
    const field = arg.column
    let sum = 0
    let count = 0
    /** @type {number | null} */
    let min = null
    /** @type {number | null} */
    let max = null

    for (let i = 0; i < rows.length; i += 1) {
      const raw = rows[i][field]
      if (raw === null || raw === undefined) continue
      const num = Number(raw)
      if (!Number.isFinite(num)) continue

      if (count === 0) {
        min = num
        max = num
      } else {
        if (min === null || num < min) min = num
        if (max === null || num > max) max = num
      }
      sum += num
      count += 1
    }

    if (func === 'SUM') return sum
    if (func === 'AVG') return count === 0 ? null : sum / count
    if (func === 'MIN') return min
    if (func === 'MAX') return max
  }

  throw new Error('Unsupported aggregate function ' + func)
}

/**
 * Generates a default alias name for an aggregate function
 * @param {AggregateColumn} col - The aggregate column definition
 * @returns {string} The generated alias (e.g., "count_all", "sum_amount")
 */
function defaultAggregateAlias(col) {
  const base = col.func.toLowerCase()
  if (col.arg.kind === 'star') return base + '_all'
  return base + '_' + col.arg.column
}

/**
 * Creates a stable string key for a row to enable deduplication
 * @param {Row} row - The data row
 * @returns {string} A stable string representation of the row
 */
function stableRowKey(row) {
  const keys = Object.keys(row).sort()
  /** @type {string[]} */
  const parts = []
  for (let i = 0; i < keys.length; i += 1) {
    const k = keys[i]
    const v = row[k]
    parts.push(k + ':' + JSON.stringify(v))
  }
  return parts.join('|')
}

/**
 * Compares two SQL values for sorting
 * @param {SqlPrimitive} a - First value to compare
 * @param {SqlPrimitive} b - Second value to compare
 * @returns {number} Negative if a < b, positive if a > b, 0 if equal
 */
function compareValues(a, b) {
  if (a === b) return 0
  if (a === null || a === undefined) return -1
  if (b === null || b === undefined) return 1

  if (typeof a === 'number' && typeof b === 'number') {
    if (a < b) return -1
    if (a > b) return 1
    return 0
  }

  const as = String(a)
  const bs = String(b)
  if (as < bs) return -1
  if (as > bs) return 1
  return 0
}

/**
 * Applies DISTINCT filtering to remove duplicate rows
 * @param {Row[]} rows - The input rows
 * @param {boolean} distinct - Whether to apply deduplication
 * @returns {Row[]} The deduplicated rows
 */
function applyDistinct(rows, distinct) {
  if (!distinct) return rows
  /** @type {Set<string>} */
  const seen = new Set()
  /** @type {Row[]} */
  const result = []
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i]
    const key = stableRowKey(row)
    if (seen.has(key)) continue
    seen.add(key)
    result.push(row)
  }
  return result
}

/**
 * Applies ORDER BY sorting to rows
 * @param {Row[]} rows - The input rows
 * @param {OrderByItem[]} orderBy - The sort specifications
 * @returns {Row[]} The sorted rows
 */
function applyOrderBy(rows, orderBy) {
  if (!orderBy || orderBy.length === 0) return rows

  const sorted = rows.slice()

  sorted.sort((a, b) => {
    for (let i = 0; i < orderBy.length; i += 1) {
      const term = orderBy[i]
      const key = term.expr
      const dir = term.direction
      const av = /** @type {any} */ a[key]
      const bv = /** @type {any} */ b[key]
      const cmp = compareValues(av, bv)
      if (cmp !== 0) {
        return dir === 'DESC' ? -cmp : cmp
      }
    }
    return 0
  })

  return sorted
}

/**
 * Evaluates a parsed SELECT AST against data rows
 * @param {SelectAst} ast - The parsed SQL AST
 * @param {Row[]} rows - The data rows
 * @returns {Row[]} The filtered, projected, and sorted result rows
 */
function evaluateSelectAst(ast, rows) {
  // Check for unsupported JOIN operations
  if (ast.joins && ast.joins.length > 0) {
    throw new Error('JOIN is not supported')
  }

  // WHERE
  let working = rows
  if (ast.where) {
    /** @type {Row[]} */
    const filtered = []
    for (let i = 0; i < rows.length; i += 1) {
      const row = rows[i]
      if (evaluateExpr(ast.where, row)) {
        filtered.push(row)
      }
    }
    working = filtered
  }

  const hasAggregate = ast.columns.some(col => col.kind === 'aggregate')
  const useGrouping = hasAggregate || ast.groupBy && ast.groupBy.length > 0

  /** @type {Row[]} */
  const projected = []

  if (useGrouping) {
    /** @typedef {{ groupValues: Row, rows: Row[] }} Group */
    /** @type {Group[]} */
    const groups = []

    if (ast.groupBy && ast.groupBy.length > 0) {
      /** @type {Map<string, Group>} */
      const map = new Map()
      for (let i = 0; i < working.length; i += 1) {
        const row = working[i]
        /** @type {string[]} */
        const keyParts = []
        /** @type {Row} */
        const groupValues = {}
        for (let j = 0; j < ast.groupBy.length; j += 1) {
          const colName = ast.groupBy[j]
          const v = row[colName]
          keyParts.push(JSON.stringify(v))
          groupValues[colName] = v
        }
        const key = keyParts.join('|')
        let group = map.get(key)
        if (!group) {
          group = { groupValues, rows: [] }
          map.set(key, group)
          groups.push(group)
        }
        group.rows.push(row)
      }
    } else {
      groups.push({
        groupValues: {},
        rows: working,
      })
    }

    const hasStar = ast.columns.some(col => col.kind === 'star')
    if (hasStar && hasAggregate) {
      throw new Error('SELECT * with aggregate functions is not supported in this implementation')
    }

    for (let g = 0; g < groups.length; g += 1) {
      const group = groups[g]
      /** @type {Row} */
      const resultRow = {}
      for (let c = 0; c < ast.columns.length; c += 1) {
        const col = ast.columns[c]
        if (col.kind === 'star') {
          const firstRow = group.rows[0] || {}
          const keys = Object.keys(firstRow)
          for (let k = 0; k < keys.length; k += 1) {
            const key = keys[k]
            resultRow[key] = firstRow[key]
          }
          continue
        }

        if (col.kind === 'column') {
          const name = col.column
          const alias = col.alias ?? name
          /** @type {SqlPrimitive} */
          let value = null
          if (ast.groupBy && ast.groupBy.indexOf(name) !== -1) {
            value = group.groupValues[name]
          } else if (group.rows.length > 0) {
            value = group.rows[0][name]
          }
          resultRow[alias] = value
          continue
        }

        if (col.kind === 'aggregate') {
          const alias = col.alias ?? defaultAggregateAlias(col)
          const value = evaluateAggregate(col, group.rows)
          resultRow[alias] = value
          continue
        }
      }
      projected.push(resultRow)
    }
  } else {
    for (let i = 0; i < working.length; i += 1) {
      const row = working[i]
      /** @type {Row} */
      const outRow = {}
      for (let c = 0; c < ast.columns.length; c += 1) {
        const col = ast.columns[c]
        if (col.kind === 'star') {
          const keys = Object.keys(row)
          for (let k = 0; k < keys.length; k += 1) {
            const key = keys[k]
            outRow[key] = row[key]
          }
        } else if (col.kind === 'column') {
          const name = col.column
          const alias = col.alias ?? name
          outRow[alias] = row[name]
        } else if (col.kind === 'aggregate') {
          throw new Error(
            'Aggregate functions require GROUP BY or will act on the whole dataset; add GROUP BY or remove aggregates'
          )
        }
      }
      projected.push(outRow)
    }
  }

  let result = projected

  result = applyDistinct(result, ast.distinct)
  result = applyOrderBy(result, ast.orderBy)

  if (typeof ast.offset === 'number' && ast.offset > 0) {
    result = result.slice(ast.offset)
  }

  if (typeof ast.limit === 'number') {
    result = result.slice(0, ast.limit)
  }

  return result
}

/**
 * Executes a SQL SELECT query against an array of data rows
 * @param {Row[]} rows - The data rows to query
 * @param {string} sql - The SQL query string
 * @returns {Row[]} The result rows matching the query
 */
export function executeSql(rows, sql) {
  const ast = parseSql(sql)
  return evaluateSelectAst(ast, rows)
}
