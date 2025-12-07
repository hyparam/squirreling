import {
  argCountError,
  argValueError,
  castError,
  invalidContextError,
  unknownFunctionError,
} from '../errors.js'
import { applyIntervalToDate } from './date.js'
import { executeSelect } from './execute.js'
import { applyBinaryOp, stringify } from './utils.js'

/**
 * @import { ExprNode, AsyncRow, SqlPrimitive, AsyncDataSource, IntervalUnit } from '../types.js'
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
    // Try exact match first (handles both qualified and unqualified names)
    if (row[node.name]) {
      return row[node.name]()
    }
    // For qualified names like 'users.id', also try just the column part
    if (node.name.includes('.')) {
      const colName = node.name.split('.').pop()
      if (colName && row[colName]) {
        return row[colName]()
      }
    }
    return null
  }

  // Scalar subquery - returns a single value
  if (node.type === 'subquery') {
    const gen = executeSelect(node.subquery, tables)
    const first = await gen.next() // Start the generator
    gen.return(undefined) // Stop further execution
    if (!first.value) return null
    /** @type {AsyncRow} */
    const firstRow = first.value
    const firstKey = Object.keys(firstRow)[0]
    return firstRow[firstKey]()
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
      return -val
    }
  }

  // Binary operators
  if (node.type === 'binary') {
    // Handle date +/- interval at AST level
    if ((node.op === '+' || node.op === '-') && node.right.type === 'interval') {
      const dateVal = await evaluateExpr({ node: node.left, row, tables })
      return applyIntervalToDate(dateVal, node.right.value, node.right.unit, node.op)
    }
    if (node.op === '+' && node.left.type === 'interval') {
      const dateVal = await evaluateExpr({ node: node.right, row, tables })
      return applyIntervalToDate(dateVal, node.left.value, node.left.unit, '+')
    }

    const left = await evaluateExpr({ node: node.left, row, tables })

    // Short-circuit evaluation for AND and OR
    if (node.op === 'AND') {
      if (!left) return false
    }
    if (node.op === 'OR') {
      if (left) return true
    }

    const right = await evaluateExpr({ node: node.right, row, tables })
    return applyBinaryOp(node.op, left, right)
  }

  // Function calls
  if (node.type === 'function') {
    const funcName = node.name.toUpperCase()
    /** @type {SqlPrimitive[]} */
    const args = await Promise.all(node.args.map(arg => evaluateExpr({ node: arg, row, tables })))

    if (funcName === 'UPPER') {
      if (args.length !== 1) throw argCountError('UPPER', 1, args.length)
      const val = args[0]
      if (val == null) return null
      return String(val).toUpperCase()
    }

    if (funcName === 'LOWER') {
      if (args.length !== 1) throw argCountError('LOWER', 1, args.length)
      const val = args[0]
      if (val == null) return null
      return String(val).toLowerCase()
    }

    if (funcName === 'CONCAT') {
      if (args.length < 1) throw argCountError('CONCAT', 'at least 1', args.length)
      // SQL CONCAT returns NULL if any argument is NULL
      if (args.some(a => a == null)) return null
      if (args.some(a => typeof a === 'object')) {
        throw argValueError({
          funcName: 'CONCAT',
          message: 'does not support object arguments',
          hint: 'Use CAST to convert objects to strings first.',
        })
      }
      return args.map(a => String(a)).join('')
    }

    if (funcName === 'LENGTH') {
      if (args.length !== 1) throw argCountError('LENGTH', 1, args.length)
      const val = args[0]
      if (val == null) return null
      return String(val).length
    }

    if (funcName === 'SUBSTRING' || funcName === 'SUBSTR') {
      if (args.length < 2 || args.length > 3) {
        throw argCountError(funcName, '2 or 3', args.length)
      }
      const str = args[0]
      if (str == null) return null
      const strVal = String(str)
      const start = Number(args[1])
      if (!Number.isInteger(start) || start < 1) {
        throw argValueError({
          funcName,
          message: `start position must be a positive integer, got ${args[1]}`,
          hint: 'SQL uses 1-based indexing.',
        })
      }
      // SQL uses 1-based indexing
      const startIdx = start - 1
      if (args.length === 3) {
        const len = Number(args[2])
        if (!Number.isInteger(len) || len < 0) {
          throw argValueError({
            funcName,
            message: `length must be a non-negative integer, got ${args[2]}`,
          })
        }
        return strVal.substring(startIdx, startIdx + len)
      }
      return strVal.substring(startIdx)
    }

    if (funcName === 'TRIM') {
      if (args.length !== 1) throw argCountError('TRIM', 1, args.length)
      const val = args[0]
      if (val == null) return null
      return String(val).trim()
    }

    if (funcName === 'REPLACE') {
      if (args.length !== 3) throw argCountError('REPLACE', 3, args.length)
      const str = args[0]
      const searchStr = args[1]
      const replaceStr = args[2]
      // SQL REPLACE returns NULL if any argument is NULL
      if (str == null || searchStr == null || replaceStr == null) return null
      return String(str).replaceAll(String(searchStr), String(replaceStr))
    }

    if (funcName === 'RANDOM' || funcName === 'RAND') {
      if (args.length !== 0) throw argCountError(funcName, 0, args.length)
      return Math.random()
    }

    if (funcName === 'CURRENT_DATE') {
      if (args.length !== 0) throw argCountError('CURRENT_DATE', 0, args.length)
      return new Date().toISOString().split('T')[0]
    }

    if (funcName === 'CURRENT_TIME') {
      if (args.length !== 0) throw argCountError('CURRENT_TIME', 0, args.length)
      return new Date().toISOString().split('T')[1].replace('Z', '')
    }

    if (funcName === 'CURRENT_TIMESTAMP') {
      if (args.length !== 0) throw argCountError('CURRENT_TIMESTAMP', 0, args.length)
      return new Date().toISOString()
    }

    if (funcName === 'JSON_OBJECT') {
      if (args.length % 2 !== 0) {
        throw argCountError('JSON_OBJECT', 'even number', args.length)
      }
      /** @type {Record<string, SqlPrimitive>} */
      const result = {}
      for (let i = 0; i < args.length; i += 2) {
        const key = args[i]
        const value = args[i + 1]
        if (key == null) {
          throw argValueError({
            funcName: 'JSON_OBJECT',
            message: 'key cannot be null',
            hint: 'All keys must be non-null values.',
          })
        }
        result[String(key)] = value
      }
      return result
    }

    if (funcName === 'JSON_VALUE' || funcName === 'JSON_QUERY') {
      if (args.length !== 2) throw argCountError(funcName, 2, args.length)
      let jsonArg = args[0]
      const pathArg = args[1]
      if (jsonArg == null || pathArg == null) return null

      // Parse JSON if string, otherwise use directly
      if (typeof jsonArg === 'string') {
        try {
          jsonArg = JSON.parse(jsonArg)
        } catch {
          throw argValueError({
            funcName,
            message: 'invalid JSON string',
            hint: 'First argument must be valid JSON.',
          })
        }
      }
      if (typeof jsonArg !== 'object' || jsonArg instanceof Date) {
        throw argValueError({
          funcName,
          message: `first argument must be JSON string or object, got ${typeof jsonArg}`,
        })
      }

      // Parse path ("$.foo.bar[0].baz" or "foo.bar[0]")
      const path = String(pathArg)
      const normalizedPath = path.startsWith('$') ? path.slice(1) : path

      // Navigate the path
      let current = jsonArg
      const segments = normalizedPath.match(/\.?([^.[]+)|\[(\d+)\]/g) || []
      for (const segment of segments) {
        if (current == null) return null
        if (segment.startsWith('[')) {
          // Array index access
          const index = parseInt(segment.slice(1, -1), 10)
          if (!Array.isArray(current)) return null
          current = current[index]
        } else {
          // Property access
          const key = segment.startsWith('.') ? segment.slice(1) : segment
          if (typeof current !== 'object' || Array.isArray(current)) return null
          current = current[key]
        }
      }

      if (current == null) return null
      return current
    }

    throw unknownFunctionError(funcName)
  }

  if (node.type === 'cast') {
    const val = await evaluateExpr({ node: node.expr, row, tables })
    if (val == null) return null
    const toType = node.toType.toUpperCase()
    if (toType === 'TEXT' || toType === 'STRING' || toType === 'VARCHAR') {
      if (typeof val === 'object') return stringify(val)
      return String(val)
    }
    // Can only cast primitives to other primitive types
    if (typeof val === 'object') throw castError(node.toType, 'object')
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
    if (toType === 'BOOLEAN' || toType === 'BOOL') {
      return Boolean(val)
    }
    throw castError(node.toType)
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
  // IN with subqueries
  if (node.type === 'in') {
    const exprVal = await evaluateExpr({ node: node.expr, row, tables })
    const results = executeSelect(node.subquery, tables)
    /** @type {SqlPrimitive[]} */
    const values = []
    for await (const resRow of results) {
      const firstKey = Object.keys(resRow)[0]
      const val = await resRow[firstKey]()
      values.push(val)
    }
    return values.includes(exprVal)
  }

  // EXISTS and NOT EXISTS with subqueries
  if (node.type === 'exists') {
    const results = await executeSelect(node.subquery, tables).next()
    return results.done === false
  }
  if (node.type === 'not exists') {
    const results = await executeSelect(node.subquery, tables).next()
    return results.done === true
  }

  // CASE expressions
  if (node.type === 'case') {
    // For simple CASE: evaluate the case expression once
    const caseValue = node.caseExpr && await evaluateExpr({ node: node.caseExpr, row, tables })

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

  // INTERVAL expressions should only appear as part of binary +/- operations
  // which are handled above. A standalone interval is an error.
  if (node.type === 'interval') {
    throw invalidContextError({
      item: 'INTERVAL',
      validContext: 'date arithmetic (+ or -)',
    })
  }

  throw new Error(`Unknown expression node type: ${node.type}. This is an internal error - the query may contain unsupported syntax.`)
}
