import { stringify } from '../execute/utils.js'
import { ArgValueError, ExecutionError } from '../validation/executionErrors.js'
import { toDate } from './date.js'

/**
 * @import { CastNode, FunctionNode, SqlPrimitive } from '../types.js'
 */

/**
 * Applies a scalar CAST without requiring a row evaluator.
 *
 * @param {CastNode} node
 * @param {SqlPrimitive} value
 * @param {number} [rowIndex]
 * @returns {SqlPrimitive}
 */
export function applyCast(node, value, rowIndex) {
  if (value == null) return null
  const { toType } = node
  if (toType === 'TEXT' || toType === 'STRING' || toType === 'VARCHAR') {
    return typeof value === 'object' ? stringify(value) : String(value)
  }
  if (typeof value === 'object' && !(value instanceof Date)) {
    if (node.tryCast) return null
    throw new ExecutionError({ message: `Cannot CAST object to ${toType}`, rowIndex, ...node })
  }
  if (toType === 'INTEGER' || toType === 'INT') {
    const number = Number(value)
    return isNaN(number) ? null : Math.trunc(number)
  }
  if (toType === 'BIGINT') {
    if (typeof value === 'bigint') return value
    const number = Number(value)
    return isFinite(number) ? BigInt(Math.trunc(number)) : null
  }
  if (toType === 'FLOAT' || toType === 'REAL' || toType === 'DOUBLE') {
    const number = Number(value)
    return isNaN(number) ? null : number
  }
  if (toType === 'BOOLEAN' || toType === 'BOOL') return Boolean(value)
  if (typeof value === 'number' || typeof value === 'bigint') {
    const date = new Date(Number(value))
    return isNaN(date.getTime()) ? null : date
  }
  return toDate(value)
}

/**
 * Applies the JSON extraction functions shared by row and batch evaluation.
 *
 * @param {object} options
 * @param {string} options.funcName
 * @param {FunctionNode} options.node
 * @param {SqlPrimitive[]} options.args
 * @param {number} [options.rowIndex]
 * @returns {SqlPrimitive}
 */
export function evaluateJsonExtract({ funcName, node, args, rowIndex }) {
  let json = args[0]
  const pathArg = args[1]
  if (json == null || pathArg == null) return null

  if (typeof json === 'string') {
    try {
      json = JSON.parse(json)
    } catch {
      throw new ArgValueError({
        ...node,
        message: 'invalid JSON string',
        hint: 'First argument must be valid JSON.',
        rowIndex,
      })
    }
  }
  if (typeof json !== 'object' || json instanceof Date) {
    throw new ArgValueError({
      ...node,
      message: `first argument must be JSON string or object, got ${typeof json}`,
      rowIndex,
    })
  }

  const path = String(pathArg)
  const normalized = path.startsWith('$') ? path.slice(1) : path
  /** @type {any} */
  let current = json
  const segments = normalized.match(/\.?([^.[]+)|\[(\d+)\]/g) || []
  for (const segment of segments) {
    if (current == null) return null
    if (segment.startsWith('[')) {
      if (!Array.isArray(current)) return null
      current = current[parseInt(segment.slice(1, -1), 10)]
    } else {
      if (typeof current !== 'object' || Array.isArray(current)) return null
      const key = segment.startsWith('.') ? segment.slice(1) : segment
      current = current[key]
    }
  }

  if (current == null) return null
  if (funcName === 'JSON_EXTRACT_STRING') {
    return typeof current === 'object' ? JSON.stringify(current) : String(current)
  }
  return current
}
