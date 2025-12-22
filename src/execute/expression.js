import { unknownFunctionError } from '../parseErrors.js'
import { invalidContextError } from '../executionErrors.js'
import {
  aggregateError,
  argValueError,
  castError,
} from '../validationErrors.js'
import { isAggregateFunc, isMathFunc } from '../validation.js'
import { applyIntervalToDate } from './date.js'
import { executeSelect } from './execute.js'
import { evaluateMathFunc } from './math.js'
import { applyBinaryOp, stringify } from './utils.js'

/**
 * @import { ExprNode, AsyncRow, SqlPrimitive, AsyncDataSource, IntervalUnit, UserDefinedFunction } from '../types.js'
 */

/**
 * Evaluates an expression node against a row of data (async version)
 *
 * @param {Object} params
 * @param {ExprNode} params.node - The expression node to evaluate
 * @param {AsyncRow} params.row - The data row to evaluate against
 * @param {Record<string, AsyncDataSource>} params.tables
 * @param {Record<string, UserDefinedFunction>} [params.functions] - User-defined functions
 * @param {number} [params.rowIndex] - 1-based row index for error reporting
 * @param {AsyncRow[]} [params.rows] - Group of rows for aggregate functions
 * @returns {Promise<SqlPrimitive>} The result of the evaluation
 */
export async function evaluateExpr({ node, row, tables, functions, rowIndex, rows }) {
  if (node.type === 'literal') {
    return node.value
  }

  if (node.type === 'identifier') {
    // Try exact match first (handles both qualified and unqualified names)
    if (row.cells[node.name]) {
      return row.cells[node.name]()
    }
    // For qualified names like 'users.id', also try just the column part
    if (node.name.includes('.')) {
      const colName = node.name.split('.').pop()
      if (colName && row.cells[colName]) {
        return row.cells[colName]()
      }
    }
    return null
  }

  // Scalar subquery - returns a single value
  if (node.type === 'subquery') {
    const gen = executeSelect({ select: node.subquery, tables })
    const { value } = await gen.next() // Start the generator
    gen.return(undefined) // Stop further execution
    if (!value) return null
    return value.cells[value.columns[0]]()
  }

  // Unary operators
  if (node.type === 'unary') {
    if (node.op === 'NOT') {
      return !await evaluateExpr({ node: node.argument, row, tables, functions, rowIndex, rows })
    }
    if (node.op === 'IS NULL') {
      return await evaluateExpr({ node: node.argument, row, tables, functions, rowIndex, rows }) == null
    }
    if (node.op === 'IS NOT NULL') {
      return await evaluateExpr({ node: node.argument, row, tables, functions, rowIndex, rows }) != null
    }
    if (node.op === '-') {
      const val = await evaluateExpr({ node: node.argument, row, tables, functions, rowIndex, rows })
      if (val == null) return null
      return -val
    }
  }

  // Binary operators
  if (node.type === 'binary') {
    // Handle date +/- interval at AST level
    if ((node.op === '+' || node.op === '-') && node.right.type === 'interval') {
      const dateVal = await evaluateExpr({ node: node.left, row, tables, functions, rowIndex, rows })
      return applyIntervalToDate(dateVal, node.right.value, node.right.unit, node.op)
    }
    if (node.op === '+' && node.left.type === 'interval') {
      const dateVal = await evaluateExpr({ node: node.right, row, tables, functions, rowIndex, rows })
      return applyIntervalToDate(dateVal, node.left.value, node.left.unit, '+')
    }

    const left = await evaluateExpr({ node: node.left, row, tables, functions, rowIndex, rows })

    // Short-circuit evaluation for AND and OR
    if (node.op === 'AND') {
      if (!left) return false
    }
    if (node.op === 'OR') {
      if (left) return true
    }

    const right = await evaluateExpr({ node: node.right, row, tables, functions, rowIndex, rows })
    return applyBinaryOp(node.op, left, right)
  }

  // Function calls
  if (node.type === 'function') {
    const funcName = node.name.toUpperCase()

    // Handle aggregate functions
    if (isAggregateFunc(funcName)) {
      if (!rows) {
        throw aggregateError({
          funcName,
          issue: 'requires GROUP BY or will act on the whole dataset',
        })
      }

      // Check for star argument (COUNT(*))
      if (node.args.length === 1 && node.args[0].type === 'identifier' && node.args[0].name === '*') {
        if (funcName === 'COUNT') {
          return rows.length
        }
        throw aggregateError({
          funcName,
          issue: '(*) is not supported, use a column name',
        })
      }

      const argNode = node.args[0]

      if (funcName === 'COUNT') {
        if (node.distinct) {
          const seen = new Set()
          for (const r of rows) {
            const v = await evaluateExpr({ node: argNode, row: r, tables, functions })
            if (v != null) seen.add(v)
          }
          return seen.size
        }
        let count = 0
        for (const r of rows) {
          const v = await evaluateExpr({ node: argNode, row: r, tables, functions })
          if (v != null) count++
        }
        return count
      }

      if (funcName === 'SUM' || funcName === 'AVG' || funcName === 'MIN' || funcName === 'MAX') {
        let sum = 0
        let count = 0
        /** @type {number | null} */
        let min = null
        /** @type {number | null} */
        let max = null

        for (const r of rows) {
          const raw = await evaluateExpr({ node: argNode, row: r, tables, functions })
          if (raw == null) continue
          const num = Number(raw)
          if (!Number.isFinite(num)) continue

          if (count === 0) {
            min = num
            max = num
          } else {
            if (min == null || num < min) min = num
            if (max == null || num > max) max = num
          }
          sum += num
          count++
        }

        if (funcName === 'SUM') return sum
        if (funcName === 'AVG') return count === 0 ? null : sum / count
        if (funcName === 'MIN') return min
        if (funcName === 'MAX') return max
      }

      if (funcName === 'JSON_ARRAYAGG') {
        /** @type {SqlPrimitive[]} */
        const values = []
        if (node.distinct) {
          const seen = new Set()
          for (const r of rows) {
            const v = await evaluateExpr({ node: argNode, row: r, tables, functions })
            const key = stringify(v)
            if (!seen.has(key)) {
              seen.add(key)
              values.push(v)
            }
          }
        } else {
          for (const r of rows) {
            const v = await evaluateExpr({ node: argNode, row: r, tables, functions })
            values.push(v)
          }
        }
        return values
      }
    }

    /** @type {SqlPrimitive[]} */
    const args = await Promise.all(node.args.map(arg => evaluateExpr({ node: arg, row, tables, functions, rowIndex, rows })))

    if (funcName === 'UPPER') {
      const val = args[0]
      if (val == null) return null
      return String(val).toUpperCase()
    }

    if (funcName === 'LOWER') {
      const val = args[0]
      if (val == null) return null
      return String(val).toLowerCase()
    }

    if (funcName === 'CONCAT') {
      // SQL CONCAT returns NULL if any argument is NULL
      if (args.some(a => a == null)) return null
      if (args.some(a => typeof a === 'object')) {
        throw argValueError({
          funcName: 'CONCAT',
          message: 'does not support object arguments',
          positionStart: node.positionStart,
          positionEnd: node.positionEnd,
          hint: 'Use CAST to convert objects to strings first.',
          rowNumber: rowIndex,
        })
      }
      return args.map(a => String(a)).join('')
    }

    if (funcName === 'LENGTH') {
      const val = args[0]
      if (val == null) return null
      return String(val).length
    }

    if (funcName === 'SUBSTRING' || funcName === 'SUBSTR') {
      const str = args[0]
      if (str == null) return null
      const strVal = String(str)
      const start = Number(args[1])
      if (!Number.isInteger(start) || start < 1) {
        throw argValueError({
          funcName,
          message: `start position must be a positive integer, got ${args[1]}`,
          positionStart: node.positionStart,
          positionEnd: node.positionEnd,
          hint: 'SQL uses 1-based indexing.',
          rowNumber: rowIndex,
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
            positionStart: node.positionStart,
            positionEnd: node.positionEnd,
            rowNumber: rowIndex,
          })
        }
        return strVal.substring(startIdx, startIdx + len)
      }
      return strVal.substring(startIdx)
    }

    if (funcName === 'TRIM') {
      const val = args[0]
      if (val == null) return null
      return String(val).trim()
    }

    if (funcName === 'REPLACE') {
      const str = args[0]
      const searchStr = args[1]
      const replaceStr = args[2]
      // SQL REPLACE returns NULL if any argument is NULL
      if (str == null || searchStr == null || replaceStr == null) return null
      return String(str).replaceAll(String(searchStr), String(replaceStr))
    }

    if (funcName === 'RANDOM' || funcName === 'RAND') {
      return Math.random()
    }

    if (funcName === 'CURRENT_DATE') {
      return new Date().toISOString().split('T')[0]
    }

    if (funcName === 'CURRENT_TIME') {
      return new Date().toISOString().split('T')[1].replace('Z', '')
    }

    if (funcName === 'CURRENT_TIMESTAMP') {
      return new Date().toISOString()
    }

    if (funcName === 'JSON_OBJECT') {
      if (args.length % 2 !== 0) {
        throw argValueError({
          funcName: 'JSON_OBJECT',
          message: 'requires an even number of arguments (key-value pairs)',
          positionStart: node.positionStart,
          positionEnd: node.positionEnd,
          rowNumber: rowIndex,
        })
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
            positionStart: node.positionStart,
            positionEnd: node.positionEnd,
            hint: 'All keys must be non-null values.',
            rowNumber: rowIndex,
          })
        }
        result[String(key)] = value
      }
      return result
    }

    if (funcName === 'JSON_VALUE' || funcName === 'JSON_QUERY') {
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
            positionStart: node.positionStart,
            positionEnd: node.positionEnd,
            hint: 'First argument must be valid JSON.',
            rowNumber: rowIndex,
          })
        }
      }
      if (typeof jsonArg !== 'object' || jsonArg instanceof Date) {
        throw argValueError({
          funcName,
          message: `first argument must be JSON string or object, got ${typeof jsonArg}`,
          positionStart: node.positionStart,
          positionEnd: node.positionEnd,
          rowNumber: rowIndex,
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

    if (isMathFunc(funcName)) {
      return evaluateMathFunc({ funcName, args })
    }

    // Check user-defined functions (case-insensitive lookup)
    if (functions) {
      const udfName = Object.keys(functions).find(k => k.toUpperCase() === funcName)
      if (udfName) {
        return await functions[udfName].apply(...args)
      }
    }

    throw unknownFunctionError({
      funcName,
      positionStart: node.positionStart,
      positionEnd: node.positionEnd,
    })
  }

  if (node.type === 'cast') {
    const val = await evaluateExpr({ node: node.expr, row, tables, functions, rowIndex, rows })
    if (val == null) return null
    const toType = node.toType.toUpperCase()
    if (toType === 'TEXT' || toType === 'STRING' || toType === 'VARCHAR') {
      if (typeof val === 'object') return stringify(val)
      return String(val)
    }
    // Can only cast primitives to other primitive types
    if (typeof val === 'object') {
      throw castError({
        toType: node.toType,
        positionStart: node.positionStart,
        positionEnd: node.positionEnd,
        fromType: 'object',
        rowNumber: rowIndex,
      })
    }
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
    throw castError({
      toType: node.toType,
      positionStart: node.positionStart,
      positionEnd: node.positionEnd,
      rowNumber: rowIndex,
    })
  }

  // IN and NOT IN with value lists
  if (node.type === 'in valuelist') {
    const exprVal = await evaluateExpr({ node: node.expr, row, tables, functions, rowIndex, rows })
    for (const valueNode of node.values) {
      const val = await evaluateExpr({ node: valueNode, row, tables, functions, rowIndex, rows })
      if (exprVal === val) return true
    }
    return false
  }
  // IN with subqueries
  if (node.type === 'in') {
    const exprVal = await evaluateExpr({ node: node.expr, row, tables, functions, rowIndex, rows })
    const results = executeSelect({ select: node.subquery, tables })
    for await (const resRow of results) {
      const value = await resRow.cells[resRow.columns[0]]()
      if (exprVal === value) return true
    }
    return false
  }

  // EXISTS and NOT EXISTS with subqueries
  if (node.type === 'exists') {
    const results = await executeSelect({ select: node.subquery, tables }).next()
    return results.done === false
  }
  if (node.type === 'not exists') {
    const results = await executeSelect({ select: node.subquery, tables }).next()
    return results.done === true
  }

  // CASE expressions
  if (node.type === 'case') {
    // For simple CASE: evaluate the case expression once
    const caseValue = node.caseExpr && await evaluateExpr({ node: node.caseExpr, row, tables, functions, rowIndex, rows })

    // Iterate through WHEN clauses
    for (const whenClause of node.whenClauses) {
      let conditionResult
      if (caseValue !== undefined) {
        // Simple CASE: compare caseValue with condition
        const whenValue = await evaluateExpr({ node: whenClause.condition, row, tables, functions, rowIndex, rows })
        conditionResult = caseValue === whenValue
      } else {
        // Searched CASE: evaluate condition as boolean
        conditionResult = await evaluateExpr({ node: whenClause.condition, row, tables, functions, rowIndex, rows })
      }

      if (conditionResult) {
        return evaluateExpr({ node: whenClause.result, row, tables, functions, rowIndex, rows })
      }
    }

    // No WHEN clause matched, return ELSE result or NULL
    if (node.elseResult) {
      return evaluateExpr({ node: node.elseResult, row, tables, functions, rowIndex, rows })
    }
    return null
  }

  // INTERVAL expressions should only appear as part of binary +/- operations
  // which are handled above. A standalone interval is an error.
  if (node.type === 'interval') {
    throw invalidContextError({
      item: 'INTERVAL',
      validContext: 'date arithmetic (+ or -)',
      positionStart: node.positionStart,
      positionEnd: node.positionEnd,
      rowNumber: rowIndex,
    })
  }

  throw new Error(`Unknown expression node type: ${node.type}. This is an internal error - the query may contain unsupported syntax.`)
}
