import { executeSelect } from './execute.js'
import { collect } from './utils.js'

/**
 * @import { ExprNode, AsyncRow, SqlPrimitive, AsyncDataSource } from '../types.js'
 */

/**
 * Evaluates an expression node against a row of data (async version)
 *
 * @param {Object} params
 * @param {ExprNode} params.node - The expression node to evaluate
 * @param {AsyncRow} params.row - The data row to evaluate against
 * @param {Record<string, AsyncDataSource>} params.tables
 * @returns {Promise<SqlPrimitive>} The result of the evaluation
 */
export async function evaluateExpr({ node, row, tables }) {
  if (node.type === 'literal') {
    return node.value
  }

  if (node.type === 'identifier') {
    return row[node.name]?.()
  }

  // Scalar subquery - returns a single value
  if (node.type === 'subquery') {
    const results = await collect(executeSelect(node.subquery, tables))
    if (results.length === 0) return null
    // Return the first column of the first row
    const firstRow = results[0]
    const firstKey = Object.keys(firstRow)[0]
    return firstRow[firstKey]
  }

  // Unary operators
  if (node.type === 'unary') {
    if (node.op === 'NOT') {
      return !await evaluateExpr({ node: node.argument, row, tables })
    }
    if (node.op === 'IS NULL') {
      return await evaluateExpr({ node: node.argument, row, tables }) == null
    }
    if (node.op === 'IS NOT NULL') {
      return await evaluateExpr({ node: node.argument, row, tables }) != null
    }
    if (node.op === '-') {
      const val = await evaluateExpr({ node: node.argument, row, tables })
      if (val == null) return null
      return -Number(val)
    }
  }

  // Binary operators
  if (node.type === 'binary') {
    if (node.op === 'AND') {
      const leftVal = await evaluateExpr({ node: node.left, row, tables })
      if (!leftVal) return false
      return Boolean(await evaluateExpr({ node: node.right, row, tables }))
    }

    if (node.op === 'OR') {
      const leftVal = await evaluateExpr({ node: node.left, row, tables })
      if (leftVal) return true
      return Boolean(await evaluateExpr({ node: node.right, row, tables }))
    }

    const left = await evaluateExpr({ node: node.left, row, tables })
    const right = await evaluateExpr({ node: node.right, row, tables })

    // In SQL, NULL comparisons with =, !=, <> always return false (unknown)
    // You must use IS NULL or IS NOT NULL to check for NULL
    if (left == null || right == null) {
      if (node.op === '=' || node.op === '!=' || node.op === '<>') {
        return false
      }
    }

    if (node.op === '=') return left === right
    if (node.op === '!=' || node.op === '<>') return left !== right
    if (node.op === '<') return left < right
    if (node.op === '>') return left > right
    if (node.op === '<=') return left <= right
    if (node.op === '>=') return left >= right

    if (node.op === 'LIKE') {
      const str = String(left)
      const pattern = String(right)
      // Convert SQL LIKE pattern to regex
      // % matches zero or more characters
      // _ matches exactly one character
      const regexPattern = pattern
        .replace(/[.*+?^${}()|[\]\\]/g, '\\$&') // Escape regex special chars
        .replace(/%/g, '.*') // Replace % with .*
        .replace(/_/g, '.') // Replace _ with .
      const regex = new RegExp('^' + regexPattern + '$', 'i')
      return regex.test(str)
    }
  }

  // BETWEEN and NOT BETWEEN
  if (node.type === 'between' || node.type === 'not between') {
    const expr = await evaluateExpr({ node: node.expr, row, tables })
    const lower = await evaluateExpr({ node: node.lower, row, tables })
    const upper = await evaluateExpr({ node: node.upper, row, tables })

    // If any value is NULL, return false (SQL behavior)
    if (expr == null || lower == null || upper == null) {
      return false
    }

    const isBetween = expr >= lower && expr <= upper
    return node.type === 'between' ? isBetween : !isBetween
  }

  // Function calls
  if (node.type === 'function') {
    const funcName = node.name.toUpperCase()
    const args = await Promise.all(node.args.map(arg => evaluateExpr({ node: arg, row, tables })))

    if (funcName === 'UPPER') {
      if (args.length !== 1) throw new Error('UPPER requires exactly 1 argument')
      const val = args[0]
      if (val == null) return null
      return String(val).toUpperCase()
    }

    if (funcName === 'LOWER') {
      if (args.length !== 1) throw new Error('LOWER requires exactly 1 argument')
      const val = args[0]
      if (val == null) return null
      return String(val).toLowerCase()
    }

    if (funcName === 'CONCAT') {
      if (args.length < 1) throw new Error('CONCAT requires at least 1 argument')
      // SQL CONCAT returns NULL if any argument is NULL
      for (let i = 0; i < args.length; i += 1) {
        if (args[i] == null) return null
      }
      return args.map(a => String(a)).join('')
    }

    if (funcName === 'LENGTH') {
      if (args.length !== 1) throw new Error('LENGTH requires exactly 1 argument')
      const val = args[0]
      if (val == null) return null
      return String(val).length
    }

    if (funcName === 'SUBSTRING' || funcName === 'SUBSTR') {
      if (args.length < 2 || args.length > 3) {
        throw new Error(`${funcName} requires 2 or 3 arguments`)
      }
      const str = args[0]
      if (str == null) return null
      const strVal = String(str)
      const start = Number(args[1])
      if (!Number.isInteger(start) || start < 1) {
        throw new Error(`${funcName} start position must be a positive integer`)
      }
      // SQL uses 1-based indexing
      const startIdx = start - 1
      if (args.length === 3) {
        const len = Number(args[2])
        if (!Number.isInteger(len) || len < 0) {
          throw new Error(`${funcName} length must be a non-negative integer`)
        }
        return strVal.substring(startIdx, startIdx + len)
      }
      return strVal.substring(startIdx)
    }

    if (funcName === 'TRIM') {
      if (args.length !== 1) throw new Error('TRIM requires exactly 1 argument')
      const val = args[0]
      if (val == null) return null
      return String(val).trim()
    }

    if (funcName === 'REPLACE') {
      if (args.length !== 3) throw new Error('REPLACE requires exactly 3 arguments')
      const str = args[0]
      const searchStr = args[1]
      const replaceStr = args[2]
      // SQL REPLACE returns NULL if any argument is NULL
      if (str == null || searchStr == null || replaceStr == null) return null
      return String(str).replaceAll(String(searchStr), String(replaceStr))
    }

    throw new Error('Unsupported function ' + funcName)
  }

  if (node.type === 'cast') {
    const val = await evaluateExpr({ node: node.expr, row, tables })
    if (val == null) return null
    const toType = node.toType.toUpperCase()
    if (toType === 'INTEGER' || toType === 'INT') {
      const num = Number(val)
      if (isNaN(num)) return null
      return Math.trunc(num)
    }
    if (toType === 'BIGINT') {
      return BigInt(val)
    }
    if (toType === 'FLOAT' || toType === 'REAL' || toType === 'DOUBLE') {
      const num = Number(val)
      if (isNaN(num)) return null
      return num
    }
    if (toType === 'TEXT' || toType === 'STRING') {
      return String(val)
    }
    if (toType === 'BOOLEAN' || toType === 'BOOL') {
      return Boolean(val)
    }
    throw new Error('Unsupported CAST to type ' + node.toType)
  }

  // IN and NOT IN with value lists
  if (node.type === 'in valuelist') {
    const exprVal = await evaluateExpr({ node: node.expr, row, tables })
    for (const valueNode of node.values) {
      const val = await evaluateExpr({ node: valueNode, row, tables })
      if (exprVal === val) return true
    }
    return false
  }
  if (node.type === 'not in valuelist') {
    const exprVal = await evaluateExpr({ node: node.expr, row, tables })
    for (const valueNode of node.values) {
      const val = await evaluateExpr({ node: valueNode, row, tables })
      if (exprVal === val) return false
    }
    return true
  }

  // IN and NOT IN with subqueries
  if (node.type === 'in') {
    const exprVal = await evaluateExpr({ node: node.expr, row, tables })
    const results = await collect(executeSelect(node.subquery, tables))
    if (results.length === 0) return false
    const firstKey = Object.keys(results[0])[0]
    const values = results.map(r => r[firstKey])
    return values.includes(exprVal)
  }
  if (node.type === 'not in') {
    const exprVal = await evaluateExpr({ node: node.expr, row, tables })
    const results = await collect(executeSelect(node.subquery, tables))
    if (results.length === 0) return true
    const firstKey = Object.keys(results[0])[0]
    const values = results.map(r => r[firstKey])
    return !values.includes(exprVal)
  }

  // EXISTS and NOT EXISTS with subqueries
  if (node.type === 'exists') {
    const results = await collect(executeSelect(node.subquery, tables))
    return results.length > 0
  }
  if (node.type === 'not exists') {
    const results = await collect(executeSelect(node.subquery, tables))
    return results.length === 0
  }

  // CASE expressions
  if (node.type === 'case') {
    // For simple CASE: evaluate the case expression once
    const caseValue = node.caseExpr ? await evaluateExpr({ node: node.caseExpr, row, tables }) : undefined

    // Iterate through WHEN clauses
    for (const whenClause of node.whenClauses) {
      let conditionResult
      if (caseValue !== undefined) {
        // Simple CASE: compare caseValue with condition
        const whenValue = await evaluateExpr({ node: whenClause.condition, row, tables })
        conditionResult = caseValue === whenValue
      } else {
        // Searched CASE: evaluate condition as boolean
        conditionResult = await evaluateExpr({ node: whenClause.condition, row, tables })
      }

      if (conditionResult) {
        return evaluateExpr({ node: whenClause.result, row, tables })
      }
    }

    // No WHEN clause matched, return ELSE result or NULL
    if (node.elseResult) {
      return evaluateExpr({ node: node.elseResult, row, tables })
    }
    return null
  }

  throw new Error('Unknown expression node type ' + node.type)
}
