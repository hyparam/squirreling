import { executeStatement } from '../execute/execute.js'
import { isPlainObject, keyify, stringify } from '../execute/utils.js'
import { ArgValueError, ExecutionError } from '../validation/executionErrors.js'
import { isAggregateFunc, isMathFunc, isRegexpFunc, isSpatialFunc, isStringFunc } from '../validation/functions.js'
import { UnknownFunctionError } from '../validation/parseErrors.js'
import { ColumnNotFoundError } from '../validation/tables.js'
import { derivedAlias } from './alias.js'
import { applyBinaryOp } from './binary.js'
import { applyIntervalToDate, dateTrunc, extractField } from './date.js'
import { evaluateMathFunc } from './math.js'
import { evaluateRegexpFunc } from './regexp.js'
import { evaluateSpatialFunc } from '../spatial/spatial.js'
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
    // Try qualified name first (e.g. 'users.id')
    if (node.prefix) {
      const qualified = node.prefix + '.' + node.name
      if (qualified in row.cells) {
        return row.cells[qualified]()
      }
      const prefix = node.prefix + '.'
      const prefixedColumns = row.columns.filter(col => col.startsWith(prefix))
      if (prefixedColumns.length === 1) {
        const value = await row.cells[prefixedColumns[0]]()
        if (isPlainObject(value) && Object.prototype.hasOwnProperty.call(value, node.name)) {
          return value[node.name]
        }
      }
      // Struct dot access where the prefix is itself a column name (bare or
      // table-qualified), e.g. `item.name` reading field `name` from a struct
      // column `item` (often introduced via UNNEST AS tc(item)).
      const suffix = '.' + node.prefix
      const baseColumns = row.columns.filter(col => col === node.prefix || col.endsWith(suffix))
      if (baseColumns.length === 1) {
        const value = await row.cells[baseColumns[0]]()
        if (isPlainObject(value) && Object.prototype.hasOwnProperty.call(value, node.name)) {
          return value[node.name]
        }
      }
      // Check outer row for correlated subquery references
      if (context.outerRow && context.outerAliases?.has(node.prefix) && node.name in context.outerRow.cells) {
        return context.outerRow.cells[node.name]()
      }
      // Fall back to just the column part
      if (node.name in row.cells) {
        return row.cells[node.name]()
      }
    } else {
      // Try exact match first
      if (node.name in row.cells) {
        return row.cells[node.name]()
      }
      // For unqualified names, search for a matching prefixed column (e.g. 'id' to 'a.id')
      const suffix = '.' + node.name
      const match = row.columns.find(col => col.endsWith(suffix))
      if (match) {
        return row.cells[match]()
      }
    }
    // Unknown identifier
    throw new ColumnNotFoundError({
      missingColumn: node.prefix ? node.prefix + '.' + node.name : node.name,
      availableColumns: row.columns,
      rowIndex,
      ...node,
    })
  }

  // Scalar subquery - returns a single value
  if (node.type === 'subquery') {
    const outerScope = context.scope
    const subContext = outerScope
      ? { ...context, outerRow: row, outerAliases: new Set(outerScope) }
      : context
    const gen = executeStatement({ query: node.subquery, context: subContext, outerScope }).rows()
    const { value } = await gen.next()
    gen.return(undefined)
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
    const funcName = node.funcName.toUpperCase()

    // Reuse a previously cached evaluation of this expression, written back
    // as a synthetic cell (e.g. by executeSort). Cached cells are not added to
    // row.columns, so checking that the alias is NOT a real column guards
    // against false positives where a table column happens to share a name
    // with the expression's derived alias.
    if (!rows) {
      const alias = derivedAlias(node)
      if (alias in row.cells && !row.columns.includes(alias)) {
        return row.cells[alias]()
      }
    }

    // Handle aggregate functions
    if (isAggregateFunc(funcName)) {
      if (!rows) {
        // Aggregate function used outside of aggregate context
        // This is only allowed if same aggregate was in the SELECT list
        const alias = derivedAlias(node)
        if (row.columns.includes(alias)) {
          return row.cells[alias]()
        } else {
          throw new ExecutionError({
            message: `Aggregate function ${funcName} is not available in this context`,
            ...node,
          })
        }
      }

      // Apply FILTER clause if present
      let filteredRows = rows
      if (node.filter) {
        const filterNode = node.filter
        const passes = await Promise.all(rows.map(row =>
          evaluateExpr({ node: filterNode, row, context })
        ))
        filteredRows = rows.filter((_, i) => passes[i])
      }

      const argNode = node.args[0]
      if (funcName === 'COUNT') {
        // COUNT(*) special case
        if (argNode.type === 'star') {
          return filteredRows.length
        }

        const values = await Promise.all(filteredRows.map(row =>
          evaluateExpr({ node: argNode, row, context })
        ))
        if (node.distinct) {
          const seen = new Set()
          for (const v of values) {
            if (v != null) seen.add(keyify(v))
          }
          return seen.size
        }
        let count = 0
        for (const v of values) {
          if (v != null) count++
        }
        return count
      }

      if (funcName === 'SUM' || funcName === 'AVG' || funcName === 'MIN' || funcName === 'MAX') {
        const rawValues = await Promise.all(filteredRows.map(row =>
          evaluateExpr({ node: argNode, row, context })
        ))
        let sum = 0
        let count = 0
        /** @type {SqlPrimitive} */
        let min = null
        /** @type {SqlPrimitive} */
        let max = null

        for (const raw of rawValues) {
          if (raw == null) continue
          if (min === null || raw < min) min = raw
          if (max === null || raw > max) max = raw
          const num = Number(raw)
          if (!Number.isFinite(num)) continue
          sum += num
          count++
        }

        if (funcName === 'SUM') return count === 0 ? null : sum
        if (funcName === 'AVG') return count === 0 ? null : sum / count
        if (funcName === 'MIN') return min
        if (funcName === 'MAX') return max
      }

      if (funcName === 'STDDEV_SAMP' || funcName === 'STDDEV_POP') {
        const rawValues = await Promise.all(filteredRows.map(row =>
          evaluateExpr({ node: argNode, row, context })
        ))
        let sum = 0
        /** @type {number[]} */
        const values = []
        for (const raw of rawValues) {
          if (raw == null) continue
          const num = Number(raw)
          if (!Number.isFinite(num)) continue
          values.push(num)
          sum += num
        }
        const n = values.length
        if (n === 0) return null
        if (funcName === 'STDDEV_SAMP' && n === 1) return null

        const mean = sum / n
        const squaredDiffs = values.reduce((acc, val) => acc + (val - mean) ** 2, 0)
        const divisor = funcName === 'STDDEV_SAMP' ? n - 1 : n
        return Math.sqrt(squaredDiffs / divisor)
      }

      if (funcName === 'MEDIAN' || funcName === 'PERCENTILE_CONT' || funcName === 'APPROX_QUANTILE') {
        let fraction
        let valueNode
        if (funcName === 'MEDIAN') {
          fraction = 0.5
          valueNode = argNode
        } else if (funcName === 'PERCENTILE_CONT') {
          fraction = Number(await evaluateExpr({ node: node.args[0], row: filteredRows[0] ?? { columns: [], cells: {} }, context }))
          valueNode = node.args[1]
        } else {
          // APPROX_QUANTILE: (expression, fraction)
          fraction = Number(await evaluateExpr({ node: node.args[1], row: filteredRows[0] ?? { columns: [], cells: {} }, context }))
          valueNode = argNode
        }
        if (!Number.isFinite(fraction) || fraction < 0 || fraction > 1) {
          throw new ExecutionError({
            message: `${funcName}: fraction must be between 0 and 1, got ${fraction}`,
            ...node,
          })
        }
        const rawValues = await Promise.all(filteredRows.map(row =>
          evaluateExpr({ node: valueNode, row, context })
        ))
        /** @type {number[]} */
        const values = []
        for (const raw of rawValues) {
          if (raw == null) continue
          const num = Number(raw)
          if (!Number.isFinite(num)) continue
          values.push(num)
        }
        if (values.length === 0) return null
        values.sort((a, b) => a - b)
        const pos = fraction * (values.length - 1)
        const lower = Math.floor(pos)
        const upper = Math.ceil(pos)
        if (lower === upper) return values[lower]
        return values[lower] + (values[upper] - values[lower]) * (pos - lower)
      }

      if (funcName === 'JSON_ARRAYAGG' || funcName === 'ARRAY_AGG') {
        if (node.distinct) {
          /** @type {SqlPrimitive[]} */
          const values = []
          const seen = new Set()
          for (const row of filteredRows) {
            const v = await evaluateExpr({ node: argNode, row, context })
            const key = keyify(v)
            if (!seen.has(key)) {
              seen.add(key)
              values.push(v)
            }
          }
          return values
        } else {
          return await Promise.all(filteredRows.map(row =>
            evaluateExpr({ node: argNode, row, context })
          ))
        }
      }

      if (funcName === 'STRING_AGG') {
        const separatorNode = node.args[1]
        const separator = String(await evaluateExpr({ node: separatorNode, row: filteredRows[0] ?? { columns: [], cells: {} }, context }))
        /** @type {string[]} */
        const values = []
        if (node.distinct) {
          const seen = new Set()
          for (const row of filteredRows) {
            const v = await evaluateExpr({ node: argNode, row, context })
            if (v == null) continue
            const str = String(v)
            const key = keyify(str)
            if (!seen.has(key)) {
              seen.add(key)
              values.push(str)
            }
          }
        } else {
          for (const row of filteredRows) {
            const v = await evaluateExpr({ node: argNode, row, context })
            if (v != null) values.push(String(v))
          }
        }
        return values.length === 0 ? null : values.join(separator)
      }
    }

    /** @type {SqlPrimitive[]} */
    const args = node.args.length === 1
      ? [await evaluateExpr({ node: node.args[0], row, rowIndex, rows, context })]
      : await Promise.all(node.args.map(arg => evaluateExpr({ node: arg, row, rowIndex, rows, context })))

    if (isStringFunc(funcName)) {
      return evaluateStringFunc({ funcName, node, args, rowIndex })
    }

    if (isRegexpFunc(funcName)) {
      return evaluateRegexpFunc({ funcName, node, args, rowIndex })
    }

    if (isMathFunc(funcName)) {
      return evaluateMathFunc({ funcName, args })
    }

    if (isSpatialFunc(funcName)) {
      return evaluateSpatialFunc({ funcName, args })
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
      const val2 = evaluateExpr({ node: node.args[1], row, rowIndex, rows, context })
      const val1 = await evaluateExpr({ node: node.args[0], row, rowIndex, rows, context })
      return val1 == await val2 ? null : val1
    }

    if (funcName === 'GREATEST' || funcName === 'LEAST') {
      // Skip nulls; return null if all inputs are null
      const isGreatest = funcName === 'GREATEST'
      /** @type {SqlPrimitive} */
      let best = null
      for (const arg of args) {
        if (arg == null) continue
        if (best == null || (isGreatest ? arg > best : arg < best)) {
          best = arg
        }
      }
      return best
    }

    if (funcName === 'DATE_TRUNC') {
      return dateTrunc(args[0], args[1])
    }

    if (funcName === 'EXTRACT' || funcName === 'DATE_PART') {
      return extractField(args[0], args[1])
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
        throw new ArgValueError({
          ...node,
          message: 'requires an even number of arguments (key-value pairs)',
          rowIndex,
        })
      }
      /** @type {Record<string, SqlPrimitive>} */
      const result = {}
      for (let i = 0; i < args.length; i += 2) {
        const key = args[i]
        const value = args[i + 1]
        if (key == null) {
          throw new ArgValueError({
            ...node,
            message: 'key cannot be null',
            hint: 'All keys must be non-null values.',
            rowIndex,
          })
        }
        result[String(key)] = value
      }
      return result
    }

    if (funcName === 'JSON_VALID') {
      const value = args[0]
      if (value == null) return null
      if (typeof value !== 'string') return false
      try {
        JSON.parse(value)
        return true
      } catch {
        return false
      }
    }

    if (funcName === 'JSON_TYPE') {
      let value = args[0]
      if (value == null) return null
      if (typeof value === 'string') {
        try {
          value = JSON.parse(value)
        } catch {
          throw new ArgValueError({
            ...node,
            message: 'invalid JSON string',
            hint: 'Argument must be valid JSON.',
            rowIndex,
          })
        }
      }
      if (value === null) return 'null'
      if (Array.isArray(value)) return 'array'
      if (value instanceof Date) return 'string'
      if (typeof value === 'bigint') return 'number'
      return typeof value
    }

    if (funcName === 'JSON_ARRAY_LENGTH') {
      let arr = args[0]
      if (arr == null) return null
      if (typeof arr === 'string') {
        try {
          arr = JSON.parse(arr)
        } catch {
          throw new ArgValueError({
            ...node,
            message: 'invalid JSON string',
            hint: 'Argument must be valid JSON.',
            rowIndex,
          })
        }
      }
      if (!Array.isArray(arr)) return null
      return arr.length
    }

    if (funcName === 'ARRAY_LENGTH' || funcName === 'CARDINALITY') {
      const arr = args[0]
      if (!Array.isArray(arr)) return null
      if (funcName === 'ARRAY_LENGTH' && args.length === 2) {
        const dim = args[1]
        if (typeof dim !== 'number' && typeof dim !== 'bigint') return null
        const d = Number(dim)
        if (!Number.isInteger(d) || d < 1) return null
        let level = arr
        for (let i = 1; i < d; i++) {
          if (!Array.isArray(level) || level.length === 0) return null
          const first = level[0]
          if (!Array.isArray(first)) return null
          for (const item of level) {
            if (!Array.isArray(item) || item.length !== first.length) return null
          }
          level = first
        }
        return level.length
      }
      return arr.length
    }

    if (funcName === 'ARRAY_POSITION') {
      const [arr, target] = args
      if (!Array.isArray(arr)) return null
      const index = arr.indexOf(target)
      return index === -1 ? null : index + 1
    }

    if (funcName === 'ARRAY_SORT') {
      const arr = args[0]
      if (!Array.isArray(arr)) return null
      return [...arr].sort((a, b) => {
        if (a == null && b == null) return 0
        if (a == null) return 1
        if (b == null) return -1
        if (a < b) return -1
        if (a > b) return 1
        return 0
      })
    }

    if (funcName === 'JSON_VALUE' || funcName === 'JSON_QUERY' || funcName === 'JSON_EXTRACT') {
      let jsonArg = args[0]
      const pathArg = args[1]
      if (jsonArg == null || pathArg == null) return null

      // Parse JSON if string, otherwise use directly
      if (typeof jsonArg === 'string') {
        try {
          jsonArg = JSON.parse(jsonArg)
        } catch {
          throw new ArgValueError({
            ...node,
            message: 'invalid JSON string',
            hint: 'First argument must be valid JSON.',
            rowIndex,
          })
        }
      }
      if (typeof jsonArg !== 'object' || jsonArg instanceof Date) {
        throw new ArgValueError({
          ...node,
          message: `first argument must be JSON string or object, got ${typeof jsonArg}`,
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

    throw new UnknownFunctionError(node)
  }

  if (node.type === 'cast') {
    const val = await evaluateExpr({ node: node.expr, row, rowIndex, rows, context })
    if (val == null) return null
    const { toType } = node
    if (toType === 'TEXT' || toType === 'STRING' || toType === 'VARCHAR') {
      if (typeof val === 'object') return stringify(val)
      return String(val)
    }
    // Can only cast primitives to other primitive types
    if (typeof val === 'object') {
      throw new ExecutionError({ message: `Cannot CAST object to ${toType}`, rowIndex, ...node })
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
    const subResult = executeStatement({ query: node.subquery, context })
    for await (const resRow of subResult.rows()) {
      const value = await resRow.cells[resRow.columns[0]]()
      if (exprVal == value) return true
    }
    return false
  }

  // EXISTS and NOT EXISTS with subqueries
  if (node.type === 'exists') {
    const results = await executeStatement({ query: node.subquery, context }).rows().next()
    return results.done === false
  }
  if (node.type === 'not exists') {
    const results = await executeStatement({ query: node.subquery, context }).rows().next()
    return results.done === true
  }

  // CASE expressions
  if (node.type === 'case') {
    // For simple CASE: evaluate the case expression once
    const caseValue = node.caseExpr && await evaluateExpr({ node: node.caseExpr, row, rowIndex, rows, context })

    // Iterate through WHEN clauses
    for (const whenClause of node.whenClauses) {
      const whenValue = await evaluateExpr({ node: whenClause.condition, row, rowIndex, rows, context })
      // compare caseValue with condition or evaluate as boolean
      if (caseValue !== undefined ? caseValue == whenValue : whenValue) {
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
    throw new ExecutionError({
      message: 'INTERVAL can only be used with date arithmetic (+ or -)',
      rowIndex,
      ...node,
    })
  }

  throw new Error(`Unknown expression node type: ${node.type}. This is an internal error - the query may contain unsupported syntax.`)
}
