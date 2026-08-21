import { ArgValueError } from '../validation/executionErrors.js'

/**
 * @import { FunctionNode, RegExpFunction, SqlPrimitive } from '../types.js'
 */

/**
 * Evaluate a regexp function
 *
 * @param {Object} options
 * @param {RegExpFunction} options.funcName
 * @param {FunctionNode} options.node
 * @param {SqlPrimitive[]} options.args - Function arguments
 * @param {number} [options.rowIndex] - Row index for error reporting
 * @returns {SqlPrimitive}
 */
export function evaluateRegexpFunc({ funcName, node, args, rowIndex }) {
  if (funcName === 'REGEXP_SUBSTR' || funcName === 'REGEXP_EXTRACT') {
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
        throw new ArgValueError({
          ...node,
          message: `position must be a positive integer, got ${args[2]}`,
          hint: 'SQL uses 1-based indexing.',
          rowIndex,
        })
      }
    }

    // Default occurrence is 1
    let occurrence = 1
    if (args.length >= 4 && args[3] != null) {
      occurrence = Number(args[3])
      if (!Number.isInteger(occurrence) || occurrence < 1) {
        throw new ArgValueError({
          ...node,
          message: `occurrence must be a positive integer, got ${args[3]}`,
          hint: 'SQL uses 1-based indexing.',
          rowIndex,
        })
      }
    }

    // Create regex
    let regex
    try {
      regex = new RegExp(patternStr, 'g')
    } catch (/** @type {any} */ error) {
      throw new ArgValueError({
        ...node,
        message: `invalid regex pattern: ${error.message}`,
        rowIndex,
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

  if (funcName === 'REGEXP_MATCHES' || funcName === 'REGEXP_LIKE') {
    return evaluateRegexpLike({ node, args, rowIndex })
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
        throw new ArgValueError({
          ...node,
          message: `position must be a positive integer, got ${args[3]}`,
          hint: 'SQL uses 1-based indexing.',
          rowIndex,
        })
      }
    }

    // Default occurrence is 0 (replace all)
    let occurrence = 0
    if (args.length >= 5 && args[4] != null) {
      occurrence = Number(args[4])
      if (!Number.isInteger(occurrence) || occurrence < 0) {
        throw new ArgValueError({
          ...node,
          message: `occurrence must be a non-negative integer, got ${args[4]}`,
          hint: 'Use 0 to replace all occurrences.',
          rowIndex,
        })
      }
    }

    // Create regex
    let regex
    try {
      regex = new RegExp(patternStr, 'g')
    } catch (/** @type {any} */ error) {
      throw new ArgValueError({
        ...node,
        message: `invalid regex pattern: ${error.message}`,
        rowIndex,
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

  throw new Error(`Unsupported regexp function: ${funcName}`)
}

/**
 * Evaluates REGEXP_LIKE/REGEXP_MATCHES with an optional single-pattern cache.
 * Batch kernels use the cache for literal patterns so the RegExp is compiled
 * once, while scalar and dynamic-pattern evaluation retain row-local behavior.
 * Compilation stays lazy so a null string still returns null without validating
 * an otherwise invalid pattern, matching the scalar evaluator.
 *
 * @param {Object} options
 * @param {FunctionNode} options.node
 * @param {SqlPrimitive[]} options.args
 * @param {number} [options.rowIndex]
 * @param {{ pattern?: string, regex?: RegExp }} [options.cache]
 * @returns {SqlPrimitive}
 */
export function evaluateRegexpLike({ node, args, rowIndex, cache }) {
  const string = args[0]
  const pattern = args[1]
  if (string == null || pattern == null) return null
  const patternString = String(pattern)
  let regex = cache?.pattern === patternString ? cache.regex : undefined
  if (!regex) {
    try {
      regex = new RegExp(patternString)
    } catch (/** @type {any} */ error) {
      throw new ArgValueError({
        ...node,
        message: `invalid regex pattern: ${error.message}`,
        rowIndex,
      })
    }
    if (cache) {
      cache.pattern = patternString
      cache.regex = regex
    }
  }
  return regex.test(String(string))
}
