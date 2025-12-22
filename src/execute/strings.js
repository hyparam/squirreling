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
 * @returns {SqlPrimitive} Result
 */
export function evaluateStringFunc({ funcName, args, positionStart, positionEnd, rowIndex }) {
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
        positionStart,
        positionEnd,
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
        positionStart,
        positionEnd,
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
          positionStart,
          positionEnd,
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

  if (funcName === 'LEFT') {
    const str = args[0]
    const n = args[1]
    if (str == null || n == null) return null
    const len = Number(n)
    if (!Number.isInteger(len) || len < 0) {
      throw argValueError({
        funcName,
        message: `length must be a non-negative integer, got ${n}`,
        positionStart,
        positionEnd,
        rowNumber: rowIndex,
      })
    }
    return String(str).substring(0, len)
  }

  if (funcName === 'RIGHT') {
    const str = args[0]
    const n = args[1]
    if (str == null || n == null) return null
    const len = Number(n)
    if (!Number.isInteger(len) || len < 0) {
      throw argValueError({
        funcName,
        message: `length must be a non-negative integer, got ${n}`,
        positionStart,
        positionEnd,
        rowNumber: rowIndex,
      })
    }
    const strVal = String(str)
    if (len >= strVal.length) return strVal
    return strVal.substring(strVal.length - len)
  }

  if (funcName === 'INSTR') {
    const str = args[0]
    const search = args[1]
    if (str == null || search == null) return null
    // INSTR returns 1-based position, 0 if not found
    const pos = String(str).indexOf(String(search))
    return pos === -1 ? 0 : pos + 1
  }
}
