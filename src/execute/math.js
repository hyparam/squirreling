/**
 * @import { MathFunc, SqlPrimitive } from '../types.js'
 */
import { argCountError } from '../validationErrors.js'

/**
 * Evaluate a math function
 *
 * @param {Object} options
 * @param {MathFunc} options.funcName - Uppercase function name
 * @param {SqlPrimitive[]} options.args - Function arguments
 * @param {number} options.positionStart - Start position in query
 * @param {number} options.positionEnd - End position in query
 * @param {number} [options.rowNumber] - 1-based row number for error reporting
 * @returns {SqlPrimitive} Result
 */
export function evaluateMathFunc({ funcName, args, positionStart, positionEnd, rowNumber }) {
  if (funcName === 'FLOOR') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'FLOOR',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
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
        rowNumber,
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
        rowNumber,
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
        rowNumber,
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
        rowNumber,
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
        rowNumber,
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
        rowNumber,
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
        rowNumber,
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
        rowNumber,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.sqrt(Number(val))
  }

  if (funcName === 'SIN') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'SIN',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.sin(Number(val))
  }

  if (funcName === 'COS') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'COS',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.cos(Number(val))
  }

  if (funcName === 'TAN') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'TAN',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.tan(Number(val))
  }

  if (funcName === 'COT') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'COT',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    const val = args[0]
    if (val == null) return null
    return 1 / Math.tan(Number(val))
  }

  if (funcName === 'ASIN') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'ASIN',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.asin(Number(val))
  }

  if (funcName === 'ACOS') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'ACOS',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.acos(Number(val))
  }

  if (funcName === 'ATAN') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'ATAN',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Math.atan(Number(val))
  }

  if (funcName === 'ATAN2') {
    if (args.length !== 2) {
      throw argCountError({
        funcName: 'ATAN2',
        expected: 2,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    const y = args[0]
    const x = args[1]
    if (y == null || x == null) return null
    return Math.atan2(Number(y), Number(x))
  }

  if (funcName === 'DEGREES') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'DEGREES',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Number(val) * 180 / Math.PI
  }

  if (funcName === 'RADIANS') {
    if (args.length !== 1) {
      throw argCountError({
        funcName: 'RADIANS',
        expected: 1,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    const val = args[0]
    if (val == null) return null
    return Number(val) * Math.PI / 180
  }

  if (funcName === 'PI') {
    if (args.length !== 0) {
      throw argCountError({
        funcName: 'PI',
        expected: 0,
        received: args.length,
        positionStart,
        positionEnd,
        rowNumber,
      })
    }
    return Math.PI
  }

  return null
}
