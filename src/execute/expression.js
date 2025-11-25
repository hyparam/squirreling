
/**
 * Evaluates an expression node against a row of data
 *
 * @import { ExprNode, RowSource, SqlPrimitive } from '../types.js'
 * @param {ExprNode} node - The expression node to evaluate
 * @param {RowSource} row - The data row to evaluate against
 * @returns {SqlPrimitive} The result of the evaluation
 */
export function evaluateExpr(node, row) {
  if (node.type === 'literal') {
    return node.value
  }

  if (node.type === 'identifier') {
    return row.getCell(node.name)
  }

  // Unary operators
  if (node.type === 'unary') {
    if (node.op === 'NOT') {
      return !evaluateExpr(node.argument, row)
    }
    if (node.op === 'IS NULL') {
      return evaluateExpr(node.argument, row) == null
    }
    if (node.op === 'IS NOT NULL') {
      return evaluateExpr(node.argument, row) != null
    }
    if (node.op === '-') {
      const val = evaluateExpr(node.argument, row)
      if (val == null) return null
      return -Number(val)
    }
  }

  // Binary operators
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
    const expr = evaluateExpr(node.expr, row)
    const lower = evaluateExpr(node.lower, row)
    const upper = evaluateExpr(node.upper, row)

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
    const args = node.args.map(arg => evaluateExpr(arg, row))

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
    const val = evaluateExpr(node.expr, row)
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
    const exprVal = evaluateExpr(node.expr, row)
    for (const valueNode of node.values) {
      const val = evaluateExpr(valueNode, row)
      if (exprVal === val) return true
    }
    return false
  }
  if (node.type === 'not in valuelist') {
    const exprVal = evaluateExpr(node.expr, row)
    for (const valueNode of node.values) {
      const val = evaluateExpr(valueNode, row)
      if (exprVal === val) return false
    }
    return true
  }

  // IN and NOT IN with subqueries
  if (node.type === 'in') {
    throw new Error('WHERE IN with subqueries is not yet supported.')
  }
  if (node.type === 'not in') {
    throw new Error('WHERE NOT IN with subqueries is not yet supported.')
  }

  // EXISTS and NOT EXISTS with subqueries
  if (node.type === 'exists') {
    throw new Error('WHERE EXISTS with subqueries is not yet supported.')
  }
  if (node.type === 'not exists') {
    throw new Error('WHERE NOT EXISTS with subqueries is not yet supported.')
  }

  // CASE expressions
  if (node.type === 'case') {
    // For simple CASE: evaluate the case expression once
    const caseValue = node.caseExpr ? evaluateExpr(node.caseExpr, row) : undefined

    // Iterate through WHEN clauses
    for (const whenClause of node.whenClauses) {
      let conditionResult
      if (caseValue !== undefined) {
        // Simple CASE: compare caseValue with condition
        const whenValue = evaluateExpr(whenClause.condition, row)
        conditionResult = caseValue === whenValue
      } else {
        // Searched CASE: evaluate condition as boolean
        conditionResult = evaluateExpr(whenClause.condition, row)
      }

      if (conditionResult) {
        return evaluateExpr(whenClause.result, row)
      }
    }

    // No WHEN clause matched, return ELSE result or NULL
    if (node.elseResult) {
      return evaluateExpr(node.elseResult, row)
    }
    return null
  }

  throw new Error('Unknown expression node type ' + node.type)
}
