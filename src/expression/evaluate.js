import { executeSelect } from '../execute/execute.js'
import { stringify } from '../execute/utils.js'
import { columnNotFoundError, invalidContextError } from '../executionErrors.js'
import { unknownFunctionError } from '../parseErrors.js'
import { isAggregateFunc, isMathFunc, isRegexpFunc, isStringFunc } from '../validation.js'
import { aggregateError, argValueError, castError } from '../validationErrors.js'
import { derivedAlias } from './alias.js'
import { applyBinaryOp } from './binary.js'
import { applyIntervalToDate } from './date.js'
import { evaluateMathFunc } from './math.js'
import { evaluateRegexpFunc } from './regexp.js'
import { evaluateStringFunc } from './strings.js'

/**
 * @import { ExprNode, AsyncRow, ExecuteContext, SqlPrimitive } from '../types.js'
 */

/**
 * Evaluates an expression node against a row of data
 *
 * @param {Object} options
 * @param {ExprNode} options.node - The expression node to evaluate
 * @param {AsyncRow} options.row - The data row to evaluate against
 * @param {number} [options.rowIndex] - 1-based row index for error reporting
 * @param {AsyncRow[]} [options.rows] - Group of rows for aggregate functions (undefined if not in aggregate context)
 * @param {ExecuteContext} options.context - execution context (tables, functions, signal)
 * @returns {Promise<SqlPrimitive>} The result of the evaluation
 */
export async function evaluateExpr({ node, row, rowIndex, rows, context }) {
  if (node.type === 'literal') {
    return node.value
  }

  if (node.type === 'identifier') {
    // Try exact match first (handles both qualified and unqualified names)
    if (node.name in row.cells) {
      return row.cells[node.name]()
    }
    // For qualified names like 'users.id', also try just the column part
    if (node.name.includes('.')) {
      const colName = node.name.split('.').pop()
      if (colName && colName in row.cells) {
        return row.cells[colName]()
      }
    }
    // Unknown identifier
    throw columnNotFoundError({
      columnName: node.name,
      availableColumns: Object.keys(row.cells),
      positionStart: node.positionStart,
      positionEnd: node.positionEnd,
      rowIndex,
    })
  }

  // Scalar subquery - returns a single value
  if (node.type === 'subquery') {
    const gen = executeSelect({ select: node.subquery, context })
    const { value } = await gen.next() // Start the generator
    gen.return(undefined) // Stop further execution
    if (!value) return null
    return value.cells[value.columns[0]]()
  }

  // Unary operators
  if (node.type === 'unary') {
    const val = await evaluateExpr({ node: node.argument, row, rowIndex, rows, context })
    if (node.op === '-') {
      if (val == null) return null
      return -val
    }
    if (node.op === 'NOT') return !val
    if (node.op === 'IS NULL') return val == null
    if (node.op === 'IS NOT NULL') return val != null
  }

  // Binary operators
  if (node.type === 'binary') {
    // Handle date +/- interval
    if ((node.op === '+' || node.op === '-') && node.right.type === 'interval') {
      const dateVal = await evaluateExpr({ node: node.left, row, rowIndex, rows, context })
      return applyIntervalToDate(dateVal, node.right.value, node.right.unit, node.op)
    }
    if (node.op === '+' && node.left.type === 'interval') {
      const dateVal = await evaluateExpr({ node: node.right, row, rowIndex, rows, context })
      return applyIntervalToDate(dateVal, node.left.value, node.left.unit, '+')
    }

    const left = await evaluateExpr({ node: node.left, row, rowIndex, rows, context })

    // Short-circuit evaluation for AND and OR
    if (node.op === 'AND' && !left) return false
    if (node.op === 'OR' && left) return true

    const right = await evaluateExpr({ node: node.right, row, rowIndex, rows, context })
    return applyBinaryOp(node.op, left, right)
  }

  // Function calls
  if (node.type === 'function') {
    const funcName = node.name.toUpperCase()

    // Handle aggregate functions
    if (isAggregateFunc(funcName)) {
      if (!rows) {
        // Aggregate function used outside of aggregate context
        // This is only allowed if same aggregate was in the SELECT list
        const alias = derivedAlias(node)
        if (row.columns.includes(alias)) {
          return row.cells[alias]()
        } else {
          throw aggregateError({
            funcName,
            positionStart: node.positionStart,
            positionEnd: node.positionEnd,
          })
        }
      }

      // Apply FILTER clause if present
      let filteredRows = rows
      if (node.filter) {
        filteredRows = []
        for (const row of rows) {
          const passes = await evaluateExpr({ node: node.filter, row, context })
          if (passes) filteredRows.push(row)
        }
      }

      const argNode = node.args[0]
      if (funcName === 'COUNT') {
        // COUNT(*) special case
        if (argNode.type === 'star') {
          return filteredRows.length
        }

        if (node.distinct) {
          const seen = new Set()
          for (const row of filteredRows) {
            const v = await evaluateExpr({ node: argNode, row, context })
            if (v != null) seen.add(v)
          }
          return seen.size
        }
        let count = 0
        for (const row of filteredRows) {
          const v = await evaluateExpr({ node: argNode, row, context })
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

        for (const row of filteredRows) {
          const raw = await evaluateExpr({ node: argNode, row, context })
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

        if (funcName === 'SUM') return count === 0 ? null : sum
        if (funcName === 'AVG') return count === 0 ? null : sum / count
        if (funcName === 'MIN') return min
        if (funcName === 'MAX') return max
      }

      if (funcName === 'STDDEV_SAMP' || funcName === 'STDDEV_POP') {
        const values = []
        for (const row of filteredRows) {
          const raw = await evaluateExpr({ node: argNode, row, context })
          if (raw == null) continue
          const num = Number(raw)
          if (!Number.isFinite(num)) continue
          values.push(num)
        }
        const n = values.length
        if (n === 0) return null
        if (funcName === 'STDDEV_SAMP' && n === 1) return null

        const mean = values.reduce((a, b) => a + b, 0) / n
        const squaredDiffs = values.reduce((acc, val) => acc + (val - mean) ** 2, 0)
        const divisor = funcName === 'STDDEV_SAMP' ? n - 1 : n
        return Math.sqrt(squaredDiffs / divisor)
      }

      if (funcName === 'JSON_ARRAYAGG') {
        /** @type {SqlPrimitive[]} */
        const values = []
        if (node.distinct) {
          const seen = new Set()
          for (const row of filteredRows) {
            const v = await evaluateExpr({ node: argNode, row, context })
            const key = stringify(v)
            if (!seen.has(key)) {
              seen.add(key)
              values.push(v)
            }
          }
        } else {
          for (const row of filteredRows) {
            const v = await evaluateExpr({ node: argNode, row, context })
            values.push(v)
          }
        }
        return values
      }
    }

    /** @type {SqlPrimitive[]} */
    const args = await Promise.all(node.args.map(arg => evaluateExpr({ node: arg, row, rowIndex, rows, context })))

    if (isStringFunc(funcName)) {
      return evaluateStringFunc({
        funcName,
        args,
        positionStart: node.positionStart,
        positionEnd: node.positionEnd,
        rowIndex,
      })
    }

    if (isRegexpFunc(funcName)) {
      return evaluateRegexpFunc({
        funcName,
        args,
        positionStart: node.positionStart,
        positionEnd: node.positionEnd,
        rowIndex,
      })
    }

    if (isMathFunc(funcName)) {
      return evaluateMathFunc({ funcName, args })
    }

    if (funcName === 'COALESCE') {
      // Short-circuit: evaluate args one at a time, return first non-null
      for (const arg of node.args) {
        const val = await evaluateExpr({ node: arg, row, rowIndex, rows, context })
        if (val != null) return val
      }
      return null
    }

    if (funcName === 'NULLIF') {
      // NULLIF(a, b) returns null if a = b, otherwise returns a
      const val1 = await evaluateExpr({ node: node.args[0], row, rowIndex, rows, context })
      const val2 = await evaluateExpr({ node: node.args[1], row, rowIndex, rows, context })
      return val1 == val2 ? null : val1
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
          rowIndex,
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
            rowIndex,
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
            rowIndex,
          })
        }
      }
      if (typeof jsonArg !== 'object' || jsonArg instanceof Date) {
        throw argValueError({
          funcName,
          message: `first argument must be JSON string or object, got ${typeof jsonArg}`,
          positionStart: node.positionStart,
          positionEnd: node.positionEnd,
          rowIndex,
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

    // Check user-defined functions (case-insensitive lookup)
    const { functions } = context
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
    const val = await evaluateExpr({ node: node.expr, row, rowIndex, rows, context })
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
        rowIndex,
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
      rowIndex,
    })
  }

  // IN and NOT IN with value lists
  if (node.type === 'in valuelist') {
    const exprVal = await evaluateExpr({ node: node.expr, row, rowIndex, rows, context })
    for (const valueNode of node.values) {
      const val = await evaluateExpr({ node: valueNode, row, rowIndex, rows, context })
      if (exprVal == val) return true
    }
    return false
  }
  // IN with subqueries
  if (node.type === 'in') {
    const exprVal = await evaluateExpr({ node: node.expr, row, rowIndex, rows, context })
    const results = executeSelect({ select: node.subquery, context })
    for await (const resRow of results) {
      const value = await resRow.cells[resRow.columns[0]]()
      if (exprVal == value) return true
    }
    return false
  }

  // EXISTS and NOT EXISTS with subqueries
  if (node.type === 'exists') {
    const results = await executeSelect({ select: node.subquery, context }).next()
    return results.done === false
  }
  if (node.type === 'not exists') {
    const results = await executeSelect({ select: node.subquery, context }).next()
    return results.done === true
  }

  // CASE expressions
  if (node.type === 'case') {
    // For simple CASE: evaluate the case expression once
    const caseValue = node.caseExpr && await evaluateExpr({ node: node.caseExpr, row, rowIndex, rows, context })

    // Iterate through WHEN clauses
    for (const whenClause of node.whenClauses) {
      let conditionResult
      if (caseValue !== undefined) {
        // Simple CASE: compare caseValue with condition
        const whenValue = await evaluateExpr({ node: whenClause.condition, row, rowIndex, rows, context })
        conditionResult = caseValue == whenValue
      } else {
        // Searched CASE: evaluate condition as boolean
        conditionResult = await evaluateExpr({ node: whenClause.condition, row, rowIndex, rows, context })
      }

      if (conditionResult) {
        return evaluateExpr({ node: whenClause.result, row, rowIndex, rows, context })
      }
    }

    // No WHEN clause matched, return ELSE result or NULL
    if (node.elseResult) {
      return evaluateExpr({ node: node.elseResult, row, rowIndex, rows, context })
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
      rowIndex,
    })
  }

  throw new Error(`Unknown expression node type: ${node.type}. This is an internal error - the query may contain unsupported syntax.`)
}
