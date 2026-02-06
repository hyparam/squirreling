/**
 * @import { SqlPrimitive, StringFunc } from '../types.js'
 */

import { argValueError } from '../validationErrors.js'

/**
 * Evaluate a string function
 *
 * @param {Object} options
 * @param {StringFunc} options.funcName - Uppercase function name
 * @param {SqlPrimitive[]} options.args - Function arguments
 * @param {number} [options.positionStart] - Start position for error reporting
 * @param {number} [options.positionEnd] - End position for error reporting
 * @param {number} [options.rowIndex] - Row index for error reporting
 * @returns {SqlPrimitive}
 */
export function evaluateStringFunc({ funcName, args, positionStart, positionEnd, rowIndex }) {
  if (funcName === 'CONCAT') {
    // Returns NULL if any argument is NULL
    if (args.some(a => a == null)) return null
    if (args.some(a => typeof a === 'object')) {
      throw argValueError({
        funcName: 'CONCAT',
        message: 'does not support object arguments',
        positionStart,
        positionEnd,
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
        funcName,
        message: `start position must be a positive integer, got ${args[1]}`,
        positionStart,
        positionEnd,
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
          funcName,
          message: `length must be a non-negative integer, got ${args[2]}`,
          positionStart,
          positionEnd,
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
        funcName,
        message: `length must be a non-negative integer, got ${n}`,
        positionStart,
        positionEnd,
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
        funcName,
        message: `length must be a non-negative integer, got ${n}`,
        positionStart,
        positionEnd,
        rowIndex,
      })
    }
    if (len >= str.length) return str
    return str.substring(str.length - len)
  }

  if (funcName === 'INSTR') {
    const search = args[1]
    if (search == null) return null
    // INSTR returns 1-based position, 0 if not found
    return str.indexOf(String(search)) + 1
  }
}
