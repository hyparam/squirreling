import { argValueError } from '../validationErrors.js'

/**
 * @import { SqlPrimitive } from '../types.js'
 */

/**
 * Evaluate a regexp function
 *
 * @param {Object} options
 * @param {string} options.funcName - Uppercase function name
 * @param {SqlPrimitive[]} options.args - Function arguments
 * @param {number} [options.positionStart] - Start position in SQL string for error reporting
 * @param {number} [options.positionEnd] - End position in SQL string for error reporting
 * @param {number} [options.rowIndex] - Row number for error reporting
 * @returns {SqlPrimitive} Result
 */
export function evaluateRegexpFunc({ funcName, args, positionStart, positionEnd, rowIndex }) {
  if (funcName === 'REGEXP_SUBSTR') {
    const str = args[0]
    const pattern = args[1]
    if (str == null || pattern == null) return null
    const strVal = String(str)
    const patternStr = String(pattern)

    // Default position is 1 (1-based)
    let position = 1
    if (args.length >= 3 && args[2] != null) {
      position = Number(args[2])
      if (!Number.isInteger(position) || position < 1) {
        throw argValueError({
          funcName,
          message: `position must be a positive integer, got ${args[2]}`,
          positionStart,
          positionEnd,
          hint: 'SQL uses 1-based indexing.',
          rowNumber: rowIndex,
        })
      }
    }

    // Default occurrence is 1
    let occurrence = 1
    if (args.length >= 4 && args[3] != null) {
      occurrence = Number(args[3])
      if (!Number.isInteger(occurrence) || occurrence < 1) {
        throw argValueError({
          funcName,
          message: `occurrence must be a positive integer, got ${args[3]}`,
          positionStart,
          positionEnd,
          rowNumber: rowIndex,
        })
      }
    }

    // Create regex
    let regex
    try {
      regex = new RegExp(patternStr, 'g')
    } catch (/** @type {any} */ error) {
      throw argValueError({
        funcName,
        message: `invalid regex pattern: ${error.message}`,
        positionStart,
        positionEnd,
        rowNumber: rowIndex,
      })
    }

    // Search from position (convert to 0-based)
    const searchStr = strVal.substring(position - 1)

    // Find the nth occurrence
    let match
    let count = 0
    while ((match = regex.exec(searchStr)) !== null) {
      count++
      if (count === occurrence) {
        return match[0]
      }
    }

    return null
  }

  if (funcName === 'REGEXP_REPLACE') {
    const str = args[0]
    const pattern = args[1]
    const replacement = args[2]
    if (str == null || pattern == null || replacement == null) return null
    const strVal = String(str)
    const patternStr = String(pattern)
    const replacementStr = String(replacement)

    // Default position is 1 (1-based)
    let position = 1
    if (args.length >= 4 && args[3] != null) {
      position = Number(args[3])
      if (!Number.isInteger(position) || position < 1) {
        throw argValueError({
          funcName,
          message: `position must be a positive integer, got ${args[3]}`,
          positionStart,
          positionEnd,
          hint: 'SQL uses 1-based indexing.',
          rowNumber: rowIndex,
        })
      }
    }

    // Default occurrence is 0 (replace all)
    let occurrence = 0
    if (args.length >= 5 && args[4] != null) {
      occurrence = Number(args[4])
      if (!Number.isInteger(occurrence) || occurrence < 0) {
        throw argValueError({
          funcName,
          message: `occurrence must be a non-negative integer, got ${args[4]}`,
          positionStart,
          positionEnd,
          hint: 'Use 0 to replace all occurrences.',
          rowNumber: rowIndex,
        })
      }
    }

    // Create regex
    let regex
    try {
      regex = new RegExp(patternStr, 'g')
    } catch (/** @type {any} */ error) {
      throw argValueError({
        funcName,
        message: `invalid regex pattern: ${error.message}`,
        positionStart,
        positionEnd,
        rowNumber: rowIndex,
      })
    }

    // If position > 1, preserve the prefix
    const prefix = strVal.substring(0, position - 1)
    const searchStr = strVal.substring(position - 1)

    if (occurrence === 0) {
      // Replace all occurrences
      return prefix + searchStr.replace(regex, replacementStr)
    }

    // Replace only the nth occurrence
    let count = 0
    const result = searchStr.replace(regex, (match) => {
      count++
      return count === occurrence ? replacementStr : match
    })
    return prefix + result
  }
}
