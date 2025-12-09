/**
 * @import { SqlPrimitive } from '../types.js'
 */
import { argCountError } from '../errors.js'

/**
 * Evaluate a math function
 *
 * @param {Object} options
 * @param {string} options.funcName - Uppercase function name
 * @param {SqlPrimitive[]} options.args - Function arguments
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @returns {SqlPrimitive} Result
 */
export function evaluateMathFunc({ funcName, args, positionStart, positionEnd }) {
  if (funcName === 'FLOOR') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'FLOOR',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.floor(Number(val))
  }

  if (funcName === 'CEIL' || funcName === 'CEILING') {
    if (args.length !== 1) {
      throw argCountError({
        funcName,
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.ceil(Number(val))
  }

  if (funcName === 'ABS') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'ABS',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.abs(Number(val))
  }

  if (funcName === 'MOD') {
    if (args.length !== 2) {
      throw argCountError({
        funcName: 'MOD',
        expected: 2,
        received: args.length,
        positionStart,
        positionEnd,
      })
    }
    const dividend = args[0]
    const divisor = args[1]
    if (dividend == null || divisor == null) return null
    return Number(dividend) % Number(divisor)
  }

  if (funcName === 'EXP') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'EXP',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.exp(Number(val))
  }

  if (funcName === 'LN') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'LN',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.log(Number(val))
  }

  if (funcName === 'LOG10') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'LOG10',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.log10(Number(val))
  }

  if (funcName === 'POWER') {
    if (args.length !== 2) {
      throw argCountError({
        funcName: 'POWER',
        expected: 2,
        received: args.length,
        positionStart,
        positionEnd,
      })
    }
    const base = args[0]
    const exponent = args[1]
    if (base == null || exponent == null) return null
    return Number(base) ** Number(exponent)
  }

  if (funcName === 'SQRT') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'SQRT',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.sqrt(Number(val))
  }

  return undefined
}
