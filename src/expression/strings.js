import { argValueError } from '../validation/executionErrors.js'

/**
 * @import { FunctionNode, SqlPrimitive, StringFunc } from '../types.js'
 */

/**
 * Evaluate a string function
 *
 * @param {Object} options
 * @param {StringFunc} options.funcName
 * @param {FunctionNode} options.node
 * @param {SqlPrimitive[]} options.args - Function arguments
 * @param {number} options.rowIndex - Row index for error reporting
 * @returns {SqlPrimitive}
 */
export function evaluateStringFunc({ funcName, node, args, rowIndex }) {
  if (funcName === 'CONCAT') {
    // Returns NULL if any argument is NULL
    if (args.some(a => a == null)) return null
    if (args.some(a => typeof a === 'object')) {
      throw argValueError({
        ...node,
        message: 'does not support object arguments',
        hint: 'Use CAST to convert objects to strings first.',
        rowIndex,
      })
    }
    return args.map(a => String(a)).join('')
  }

  // String first arg
  const [val] = args
  if (val == null) return null
  const str = String(val)

  if (funcName === 'UPPER') {
    return str.toUpperCase()
  }

  if (funcName === 'LOWER') {
    return str.toLowerCase()
  }

  if (funcName === 'LENGTH') {
    return str.length
  }

  if (funcName === 'SUBSTRING' || funcName === 'SUBSTR') {
    const start = Number(args[1])
    if (!Number.isInteger(start) || start < 1) {
      throw argValueError({
        ...node,
        message: `start position must be a positive integer, got ${args[1]}`,
        hint: 'SQL uses 1-based indexing.',
        rowIndex,
      })
    }
    // SQL uses 1-based indexing
    const startIdx = start - 1
    if (args.length === 3) {
      const len = Number(args[2])
      if (!Number.isInteger(len) || len < 0) {
        throw argValueError({
          ...node,
          message: `length must be a non-negative integer, got ${args[2]}`,
          hint: 'SQL uses 1-based indexing.',
          rowIndex,
        })
      }
      return str.substring(startIdx, startIdx + len)
    }
    return str.substring(startIdx)
  }

  if (funcName === 'TRIM') {
    return str.trim()
  }

  if (funcName === 'REPLACE') {
    const searchStr = args[1]
    const replaceStr = args[2]
    // SQL REPLACE returns NULL if any argument is NULL
    if (searchStr == null || replaceStr == null) return null
    return str.replaceAll(String(searchStr), String(replaceStr))
  }

  if (funcName === 'LEFT') {
    const n = args[1]
    if (n == null) return null
    const len = Number(n)
    if (!Number.isInteger(len) || len < 0) {
      throw argValueError({
        ...node,
        message: `length must be a non-negative integer, got ${n}`,
        hint: 'SQL uses 1-based indexing.',
        rowIndex,
      })
    }
    return str.substring(0, len)
  }

  if (funcName === 'RIGHT') {
    const n = args[1]
    if (n == null) return null
    const len = Number(n)
    if (!Number.isInteger(len) || len < 0) {
      throw argValueError({
        ...node,
        message: `length must be a non-negative integer, got ${n}`,
        hint: 'SQL uses 1-based indexing.',
        rowIndex,
      })
    }
    if (len >= str.length) return str
    return str.substring(str.length - len)
  }

  if (funcName === 'INSTR' || funcName === 'POSITION' || funcName === 'STRPOS') {
    const search = args[1]
    if (search == null) return null
    // INSTR returns 1-based position, 0 if not found
    return str.indexOf(String(search)) + 1
  }
}
