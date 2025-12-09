/**
 * @import { SqlPrimitive } from '../types.js'
 */
import { argCountError } from '../errors.js'

/**
 * Evaluate a math function
 * @param {string} funcName - Uppercase function name
 * @param {SqlPrimitive[]} args - Function arguments
 * @returns {SqlPrimitive} Result
 */
export function evaluateMathFunc(funcName, args) {
  if (funcName === 'FLOOR') {
    if (args.length !== 1) throw argCountError('FLOOR', 1, args.length)
    const val = args[0]
    if (val == null) return null
    return Math.floor(Number(val))
  }

  if (funcName === 'CEIL' || funcName === 'CEILING') {
    if (args.length !== 1) throw argCountError(funcName, 1, args.length)
    const val = args[0]
    if (val == null) return null
    return Math.ceil(Number(val))
  }

  if (funcName === 'ABS') {
    if (args.length !== 1) throw argCountError('ABS', 1, args.length)
    const val = args[0]
    if (val == null) return null
    return Math.abs(Number(val))
  }

  if (funcName === 'MOD') {
    if (args.length !== 2) throw argCountError('MOD', 2, args.length)
    const dividend = args[0]
    const divisor = args[1]
    if (dividend == null || divisor == null) return null
    return Number(dividend) % Number(divisor)
  }

  if (funcName === 'EXP') {
    if (args.length !== 1) throw argCountError('EXP', 1, args.length)
    const val = args[0]
    if (val == null) return null
    return Math.exp(Number(val))
  }

  if (funcName === 'LN') {
    if (args.length !== 1) throw argCountError('LN', 1, args.length)
    const val = args[0]
    if (val == null) return null
    return Math.log(Number(val))
  }

  if (funcName === 'LOG10') {
    if (args.length !== 1) throw argCountError('LOG10', 1, args.length)
    const val = args[0]
    if (val == null) return null
    return Math.log10(Number(val))
  }

  if (funcName === 'POWER') {
    if (args.length !== 2) throw argCountError('POWER', 2, args.length)
    const base = args[0]
    const exponent = args[1]
    if (base == null || exponent == null) return null
    return Number(base) ** Number(exponent)
  }

  if (funcName === 'SQRT') {
    if (args.length !== 1) throw argCountError('SQRT', 1, args.length)
    const val = args[0]
    if (val == null) return null
    return Math.sqrt(Number(val))
  }

  return undefined
}
