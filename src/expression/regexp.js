import { ArgValueError } from '../validation/executionErrors.js'

/**
 * @import { FunctionNode, RegExpFunction, SqlPrimitive } from '../types.js'
 */

/**
 * Only memoize inputs at least this long. Below the floor, re-running the
 * regex costs about as much as the Map bookkeeping, and short-string columns
 * tend toward distinct values, where a memo can only miss.
 */
const MEMO_MIN_INPUT_LENGTH = 1024

/**
 * Stop inserting memo entries once the retained results reach this budget
 * (UTF-16 code units, two bytes each). Entries already inserted keep serving
 * hits, so a distinct-heavy column costs at most this much extra memory
 * instead of growing with the row count, and past the budget evaluation
 * degrades to the previous per-row behavior. Sized to hold a realistic
 * dictionary-encoded long-string column whole (a measured production day:
 * 3,445 distinct values averaging ~90KB); at 32MB that same day capped out
 * mid-scan and the recomputing tail gave back most of the win.
 */
const MEMO_BYTE_BUDGET = 256 * 1024 * 1024

/**
 * @typedef {Object} RegexpNodeCache
 * @property {string} [pattern] - pattern source of the cached compiled regex
 * @property {RegExp} [regex] - compiled regex reused while the pattern repeats
 * @property {boolean} [memoizable] - every argument after the input is a literal
 * @property {Map<string, SqlPrimitive>} [memo] - result per distinct input string
 * @property {number} [memoBytes] - retained result bytes, counted against the budget
 */

/**
 * Evaluation state scoped per execution context, then per AST node. Keyed on
 * the context object (a WeakMap, so state dies with the execution) to keep one
 * query's results out of every other query: a module-global memo would retain
 * one caller's data across callers for the life of the process.
 *
 * @type {WeakMap<object, WeakMap<FunctionNode, RegexpNodeCache>>}
 */
const executionCaches = new WeakMap()

/**
 * The per-node cache for this execution, or undefined when no context is
 * threaded through (evaluation then stays row-local, the previous behavior).
 *
 * @param {object | undefined} context
 * @param {FunctionNode} node
 * @returns {RegexpNodeCache | undefined}
 */
function resolveNodeCache(context, node) {
  if (!context) return undefined
  let byNode = executionCaches.get(context)
  if (!byNode) {
    byNode = new WeakMap()
    executionCaches.set(context, byNode)
  }
  let cache = byNode.get(node)
  if (!cache) {
    cache = {}
    byNode.set(node, cache)
  }
  return cache
}

/**
 * Compile a pattern with the 'g' flag, reusing the node's cached RegExp while
 * the pattern repeats. lastIndex is reset on reuse: a 'g' regex is stateful,
 * and a reused instance would otherwise resume the next row's exec() scan
 * wherever the previous row's search stopped.
 *
 * @param {Object} options
 * @param {RegexpNodeCache} [options.cache]
 * @param {string} options.patternStr
 * @param {FunctionNode} options.node
 * @param {number} [options.rowIndex]
 * @returns {RegExp}
 */
function compileGlobalRegex({ cache, patternStr, node, rowIndex }) {
  let regex = cache?.pattern === patternStr ? cache.regex : undefined
  if (regex) {
    regex.lastIndex = 0
    return regex
  }
  try {
    regex = new RegExp(patternStr, 'g')
  } catch (/** @type {any} */ error) {
    throw new ArgValueError({
      ...node,
      message: `invalid regex pattern: ${error.message}`,
      rowIndex,
    })
  }
  if (cache) {
    cache.pattern = patternStr
    cache.regex = regex
  }
  return regex
}

/**
 * The result memo for this node and input, or undefined when memoization does
 * not apply: no cache, an argument after the input is not a literal (the
 * result then depends on more than the input string), or the input is below
 * the length floor.
 *
 * Memoization pays off when a long-string column holds few distinct values,
 * which is exactly how dictionary-encoded columnar data arrives: thousands of
 * rows sharing a handful of large strings. Without the memo, a string-producing
 * function allocates a fresh full-length result per row (gigabytes of churn on
 * data whose storage was megabytes); with it, work and allocation scale with
 * distinct values instead of rows.
 *
 * @param {RegexpNodeCache | undefined} cache
 * @param {FunctionNode} node
 * @param {string} strVal
 * @returns {Map<string, SqlPrimitive> | undefined}
 */
function resolveMemo(cache, node, strVal) {
  if (!cache || strVal.length < MEMO_MIN_INPUT_LENGTH) return undefined
  cache.memoizable ??= node.args.every((arg, index) => index === 0 || arg.type === 'literal')
  if (!cache.memoizable) return undefined
  cache.memo ??= new Map()
  return cache.memo
}

/**
 * Record a computed result and return the stored value. String results are
 * flattened first: a substring result in V8 is a view pinning its parent
 * buffer, and a memo must not turn a 100-character result into a retained
 * copy of a 100KB input. Insertion stops at the byte budget; entries already
 * stored keep serving hits.
 *
 * @param {RegexpNodeCache} cache
 * @param {Map<string, SqlPrimitive>} memo
 * @param {string} key
 * @param {SqlPrimitive} value
 * @returns {SqlPrimitive}
 */
function memoInsert(cache, memo, key, value) {
  const bytes = typeof value === 'string' ? value.length * 2 : 16
  const memoBytes = cache.memoBytes ?? 0
  if (memoBytes + bytes > MEMO_BYTE_BUDGET) return value
  cache.memoBytes = memoBytes + bytes
  // concatenation forces a flat copy; slice(1) drops the pad character
  const flattened = typeof value === 'string' ? (' ' + value).slice(1) : value
  memo.set(key, flattened)
  return flattened
}

/**
 * Evaluate a regexp function
 *
 * @param {Object} options
 * @param {RegExpFunction} options.funcName
 * @param {FunctionNode} options.node
 * @param {SqlPrimitive[]} options.args - Function arguments
 * @param {number} [options.rowIndex] - Row index for error reporting
 * @param {object} [options.context] - execution context; enables per-node
 *   regex reuse and result memoization scoped to this execution
 * @returns {SqlPrimitive}
 */
export function evaluateRegexpFunc({ funcName, node, args, rowIndex, context }) {
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

    const cache = resolveNodeCache(context, node)
    const memo = resolveMemo(cache, node, strVal)
    if (memo?.has(strVal)) return memo.get(strVal) ?? null

    // Create regex
    const regex = compileGlobalRegex({ cache, patternStr, node, rowIndex })

    // Search from position (convert to 0-based)
    const searchStr = strVal.substring(position - 1)

    // Find the nth occurrence
    let match
    let count = 0
    /** @type {SqlPrimitive} */
    let result = null
    while ((match = regex.exec(searchStr)) !== null) {
      count++
      if (count === occurrence) {
        result = match[0]
        break
      }
    }

    return cache && memo ? memoInsert(cache, memo, strVal, result) : result
  }

  if (funcName === 'REGEXP_MATCHES' || funcName === 'REGEXP_LIKE') {
    return evaluateRegexpLike({ node, args, rowIndex, cache: resolveNodeCache(context, node) })
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

    const cache = resolveNodeCache(context, node)
    const memo = resolveMemo(cache, node, strVal)
    if (memo?.has(strVal)) return memo.get(strVal) ?? null

    // Create regex
    const regex = compileGlobalRegex({ cache, patternStr, node, rowIndex })

    // If position > 1, preserve the prefix
    const prefix = strVal.substring(0, position - 1)
    const searchStr = strVal.substring(position - 1)

    /** @type {string} */
    let result
    if (occurrence === 0) {
      // Replace all occurrences
      result = prefix + searchStr.replace(regex, replacementStr)
    } else {
      // Replace only the nth occurrence
      let count = 0
      result = prefix + searchStr.replace(regex, (match) => {
        count++
        return count === occurrence ? replacementStr : match
      })
    }

    return cache && memo ? memoInsert(cache, memo, strVal, result) : result
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
