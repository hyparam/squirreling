/**
 * @import { SelectAst, FunctionColumn, FunctionNode, OrderByItem, Row, SqlPrimitive } from '../types.js'
 */

import { defaultAggregateAlias, evaluateAggregate } from './aggregates.js'
import { evaluateExpr } from './expression.js'
import { parseSql } from '../parse/parse.js'

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

/**
 * Generates a default alias name for a string function
 * @param {FunctionColumn} col - The function column definition
 * @returns {string} The generated alias (e.g., "upper_name", "concat_a_b")
 */
function defaultFunctionAlias(col) {
  const base = col.func.toLowerCase()
  // Try to extract column names from identifier arguments
  const columnNames = col.args
    .filter(arg => arg.type === 'identifier')
    .map(arg => arg.name)
  if (columnNames.length > 0) {
    return base + '_' + columnNames.join('_')
  }
  return base
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
      const dir = term.direction
      const av = evaluateExpr(term.expr, a)
      const bv = evaluateExpr(term.expr, b)
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
    /** @typedef {Row[]} Group */
    /** @type {Group[]} */
    const groups = []

    if (ast.groupBy && ast.groupBy.length > 0) {
      /** @type {Map<string, Group>} */
      const map = new Map()
      for (let i = 0; i < working.length; i += 1) {
        const row = working[i]
        /** @type {string[]} */
        const keyParts = []
        for (let j = 0; j < ast.groupBy.length; j += 1) {
          const expr = ast.groupBy[j]
          const v = evaluateExpr(expr, row)
          keyParts.push(JSON.stringify(v))
        }
        const key = keyParts.join('|')
        let group = map.get(key)
        if (!group) {
          group = []
          map.set(key, group)
          groups.push(group)
        }
        group.push(row)
      }
    } else {
      groups.push(working)
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
          const firstRow = group[0] || {}
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
          // Evaluate on first row of group (all rows have same value for GROUP BY columns)
          const value = group.length > 0 ? group[0][name] : null
          resultRow[alias] = value
          continue
        }

        if (col.kind === 'function') {
          // Evaluate function on the first row of the group
          /** @type {FunctionNode} */
          const funcNode = { type: 'function', name: col.func, args: col.args }
          const value = group.length > 0 ? evaluateExpr(funcNode, group[0]) : null
          const alias = col.alias ?? defaultFunctionAlias(col)
          resultRow[alias] = value
          continue
        }

        if (col.kind === 'aggregate') {
          const alias = col.alias ?? defaultAggregateAlias(col)
          const value = evaluateAggregate(col, group)
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
        } else if (col.kind === 'function') {
          /** @type {FunctionNode} */
          const funcNode = { type: 'function', name: col.func, args: col.args }
          const value = evaluateExpr(funcNode, row)
          const alias = col.alias ?? defaultFunctionAlias(col)
          outRow[alias] = value
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
