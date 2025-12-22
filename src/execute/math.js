/**
 * @import { MathFunc, SqlPrimitive } from '../types.js'
 */

/**
 * Evaluate a math function
 *
 * @param {Object} options
 * @param {MathFunc} options.funcName
 * @param {SqlPrimitive[]} options.args
 * @returns {SqlPrimitive}
 */
export function evaluateMathFunc({ funcName, args }) {
  if (funcName === 'FLOOR') {
    const val = args[0]
    if (val == null) return null
    return Math.floor(Number(val))
  }

  if (funcName === 'CEIL' || funcName === 'CEILING') {
    const val = args[0]
    if (val == null) return null
    return Math.ceil(Number(val))
  }

  if (funcName === 'ABS') {
    const val = args[0]
    if (val == null) return null
    return Math.abs(Number(val))
  }

  if (funcName === 'MOD') {
    const dividend = args[0]
    const divisor = args[1]
    if (dividend == null || divisor == null) return null
    return Number(dividend) % Number(divisor)
  }

  if (funcName === 'EXP') {
    const val = args[0]
    if (val == null) return null
    return Math.exp(Number(val))
  }

  if (funcName === 'LN') {
    const val = args[0]
    if (val == null) return null
    return Math.log(Number(val))
  }

  if (funcName === 'LOG10') {
    const val = args[0]
    if (val == null) return null
    return Math.log10(Number(val))
  }

  if (funcName === 'POWER') {
    const base = args[0]
    const exponent = args[1]
    if (base == null || exponent == null) return null
    return Number(base) ** Number(exponent)
  }

  if (funcName === 'SQRT') {
    const val = args[0]
    if (val == null) return null
    return Math.sqrt(Number(val))
  }

  if (funcName === 'SIN') {
    const val = args[0]
    if (val == null) return null
    return Math.sin(Number(val))
  }

  if (funcName === 'COS') {
    const val = args[0]
    if (val == null) return null
    return Math.cos(Number(val))
  }

  if (funcName === 'TAN') {
    const val = args[0]
    if (val == null) return null
    return Math.tan(Number(val))
  }

  if (funcName === 'COT') {
    const val = args[0]
    if (val == null) return null
    return 1 / Math.tan(Number(val))
  }

  if (funcName === 'ASIN') {
    const val = args[0]
    if (val == null) return null
    return Math.asin(Number(val))
  }

  if (funcName === 'ACOS') {
    const val = args[0]
    if (val == null) return null
    return Math.acos(Number(val))
  }

  if (funcName === 'ATAN') {
    if (args.length === 1) {
      const val = args[0]
      if (val == null) return null
      return Math.atan(Number(val))
    } else {
      const y = args[0]
      const x = args[1]
      if (y == null || x == null) return null
      return Math.atan2(Number(y), Number(x))
    }
  }

  if (funcName === 'ATAN2') {
    const y = args[0]
    const x = args[1]
    if (y == null || x == null) return null
    return Math.atan2(Number(y), Number(x))
  }

  if (funcName === 'DEGREES') {
    const val = args[0]
    if (val == null) return null
    return Number(val) * 180 / Math.PI
  }

  if (funcName === 'RADIANS') {
    const val = args[0]
    if (val == null) return null
    return Number(val) * Math.PI / 180
  }

  if (funcName === 'PI') {
    return Math.PI
  }

  if (funcName === 'RAND' || funcName === 'RANDOM') {
    return Math.random()
  }
}
