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
  // No args
  if (funcName === 'PI') {
    return Math.PI
  }

  if (funcName === 'RAND' || funcName === 'RANDOM') {
    return Math.random()
  }

  // Two args
  if (funcName === 'MOD') {
    const [dividend, divisor] = args
    if (dividend == null || divisor == null) return null
    return Number(dividend) % Number(divisor)
  }

  if (funcName === 'POWER') {
    const [base, exponent] = args
    if (base == null || exponent == null) return null
    return Number(base) ** Number(exponent)
  }

  if (funcName === 'ATAN2') {
    const [y, x] = args
    if (y == null || x == null) return null
    return Math.atan2(Number(y), Number(x))
  }

  // One arg
  const [val] = args
  if (val == null) return null

  if (funcName === 'ATAN') {
    if (args.length === 1) {
      return Math.atan(Number(val))
    } else {
      const [y, x] = args
      if (y == null || x == null) return null
      return Math.atan2(Number(y), Number(x))
    }
  }

  if (funcName === 'ROUND') {
    const decimals = args[1] ?? 0
    const multiplier = 10 ** Number(decimals)
    return Math.round(Number(val) * multiplier) / multiplier
  }

  if (funcName === 'FLOOR') {
    return Math.floor(Number(val))
  }

  if (funcName === 'CEIL' || funcName === 'CEILING') {
    return Math.ceil(Number(val))
  }

  if (funcName === 'ABS') {
    return Math.abs(Number(val))
  }

  if (funcName === 'SIGN') {
    return Math.sign(Number(val))
  }

  if (funcName === 'EXP') {
    return Math.exp(Number(val))
  }

  if (funcName === 'LN') {
    return Math.log(Number(val))
  }

  if (funcName === 'LOG10') {
    return Math.log10(Number(val))
  }

  if (funcName === 'SQRT') {
    return Math.sqrt(Number(val))
  }

  if (funcName === 'SIN') {
    return Math.sin(Number(val))
  }

  if (funcName === 'COS') {
    return Math.cos(Number(val))
  }

  if (funcName === 'TAN') {
    return Math.tan(Number(val))
  }

  if (funcName === 'COT') {
    return 1 / Math.tan(Number(val))
  }

  if (funcName === 'ASIN') {
    return Math.asin(Number(val))
  }

  if (funcName === 'ACOS') {
    return Math.acos(Number(val))
  }

  if (funcName === 'DEGREES') {
    return Number(val) * 180 / Math.PI
  }

  if (funcName === 'RADIANS') {
    return Number(val) * Math.PI / 180
  }
}
