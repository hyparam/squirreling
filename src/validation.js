
/**
 * @import {AggregateFunc, BinaryOp, ComparisonOp, IntervalUnit, MathFunc, StringFunc, UserDefinedFunction} from './types.js'
 * @param {string} name
 * @returns {name is AggregateFunc}
 */
export function isAggregateFunc(name) {
  return ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'JSON_ARRAYAGG'].includes(name)
}

/**
 * @param {string} name
 * @returns {boolean}
 */
export function isRegexpFunc(name) {
  return ['REGEXP_SUBSTR', 'REGEXP_REPLACE'].includes(name)
}

/**
 * @param {string} name
 * @returns {name is MathFunc}
 */
export function isMathFunc(name) {
  return [
    'FLOOR', 'CEIL', 'CEILING', 'ABS', 'MOD',
    'EXP', 'LN', 'LOG10', 'POWER', 'SQRT',
    'SIN', 'COS', 'TAN', 'COT', 'ASIN', 'ACOS', 'ATAN', 'ATAN2',
    'DEGREES', 'RADIANS', 'PI',
  ].includes(name)
}

/**
 * @param {string} name
 * @returns {name is IntervalUnit}
 */
export function isIntervalUnit(name) {
  return ['DAY', 'MONTH', 'YEAR', 'HOUR', 'MINUTE', 'SECOND'].includes(name)
}

/**
 * @param {string} name
 * @returns {name is StringFunc}
 */
export function isStringFunc(name) {
  return [
    'UPPER',
    'LOWER',
    'CONCAT',
    'LENGTH',
    'SUBSTRING',
    'SUBSTR',
    'TRIM',
    'REPLACE',
    'LEFT',
    'RIGHT',
    'INSTR',
    'REGEXP_SUBSTR',
    'REGEXP_REPLACE',
    'RANDOM',
    'RAND',
    'JSON_VALUE',
    'JSON_QUERY',
    'JSON_OBJECT',
    'CURRENT_DATE',
    'CURRENT_TIME',
    'CURRENT_TIMESTAMP',
    'COALESCE',
  ].includes(name)
}

/**
 * @param {string} op
 * @returns {op is BinaryOp}
 */
export function isBinaryOp(op) {
  return ['AND', 'OR', 'LIKE', '=', '!=', '<>', '<', '>', '<=', '>='].includes(op)
}

/**
 * Function argument count specifications.
 * min: minimum number of arguments
 * max: maximum number of arguments
 * @type {Record<string, {min: number, max?: number}>}
 */
export const FUNCTION_ARG_COUNTS = {
  // String functions
  UPPER: { min: 1, max: 1 },
  LOWER: { min: 1, max: 1 },
  LENGTH: { min: 1, max: 1 },
  TRIM: { min: 1, max: 1 },
  REPLACE: { min: 3, max: 3 },
  SUBSTRING: { min: 2, max: 3 },
  SUBSTR: { min: 2, max: 3 },
  CONCAT: { min: 1 },
  LEFT: { min: 2, max: 2 },
  RIGHT: { min: 2, max: 2 },
  INSTR: { min: 2, max: 2 },
  REGEXP_SUBSTR: { min: 2, max: 4 },
  REGEXP_REPLACE: { min: 3, max: 5 },

  // Date/time functions
  RANDOM: { min: 0, max: 0 },
  RAND: { min: 0, max: 0 },
  CURRENT_DATE: { min: 0, max: 0 },
  CURRENT_TIME: { min: 0, max: 0 },
  CURRENT_TIMESTAMP: { min: 0, max: 0 },

  // Math functions
  FLOOR: { min: 1, max: 1 },
  CEIL: { min: 1, max: 1 },
  CEILING: { min: 1, max: 1 },
  ABS: { min: 1, max: 1 },
  MOD: { min: 2, max: 2 },
  EXP: { min: 1, max: 1 },
  LN: { min: 1, max: 1 },
  LOG10: { min: 1, max: 1 },
  POWER: { min: 2, max: 2 },
  SQRT: { min: 1, max: 1 },
  SIN: { min: 1, max: 1 },
  COS: { min: 1, max: 1 },
  TAN: { min: 1, max: 1 },
  COT: { min: 1, max: 1 },
  ASIN: { min: 1, max: 1 },
  ACOS: { min: 1, max: 1 },
  ATAN: { min: 1, max: 2 },
  ATAN2: { min: 2, max: 2 },
  DEGREES: { min: 1, max: 1 },
  RADIANS: { min: 1, max: 1 },
  PI: { min: 0, max: 0 },

  // JSON functions
  JSON_VALUE: { min: 2, max: 2 },
  JSON_QUERY: { min: 2, max: 2 },
  JSON_OBJECT: { min: 0 },
  JSON_ARRAYAGG: { min: 1, max: 1 },

  // Conditional functions
  COALESCE: { min: 1 },

  // Aggregate functions
  COUNT: { min: 1, max: 1 },
  SUM: { min: 1, max: 1 },
  AVG: { min: 1, max: 1 },
  MIN: { min: 1, max: 1 },
  MAX: { min: 1, max: 1 },
}

/**
 * Format expected argument count for error messages.
 * @param {number} min
 * @param {number | undefined} max
 * @returns {string | number}
 */
function formatExpected(min, max) {
  if (max == null) return `at least ${min}`
  if (min === max) return min
  return `${min} or ${max}`
}

/**
 * Validates function argument count.
 * @param {string} funcName - The function name (uppercase)
 * @param {number} argCount - Number of arguments provided
 * @param {Record<string, UserDefinedFunction>} [functions] - User-defined functions
 * @returns {{ valid: boolean, expected: string | number }}
 */
export function validateFunctionArgCount(funcName, argCount, functions) {
  // Check built-in functions
  let spec = FUNCTION_ARG_COUNTS[funcName]

  // Check user-defined functions (case-insensitive)
  if (!spec && functions) {
    const udfName = Object.keys(functions).find(k => k.toUpperCase() === funcName)
    if (udfName) {
      spec = functions[udfName].arguments
    }
  }

  if (!spec) return { valid: true, expected: 0 }

  const { min, max } = spec

  if (argCount < min) {
    return { valid: false, expected: formatExpected(min, max) }
  }
  if (max != null && argCount > max) {
    return { valid: false, expected: formatExpected(min, max) }
  }

  return { valid: true, expected: formatExpected(min, max) }
}

/**
 * Checks if a function is known (either built-in or user-defined).
 * @param {string} funcName - The function name (uppercase)
 * @param {Record<string, UserDefinedFunction>} [functions] - User-defined functions
 * @returns {boolean}
 */
export function isKnownFunction(funcName, functions) {
  // Check built-in functions
  if (isAggregateFunc(funcName) || isMathFunc(funcName) || isStringFunc(funcName)) {
    return true
  }

  // Special case: CAST is not in any function list but is a built-in
  if (funcName === 'CAST') {
    return true
  }

  // Check user-defined functions (case-insensitive)
  if (functions) {
    return Object.keys(functions).some(k => k.toUpperCase() === funcName)
  }

  return false
}

// Keywords that cannot be used as implicit aliases after a column
export const RESERVED_AFTER_COLUMN = new Set([
  'FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
])

// Keywords that cannot be used as table aliases
export const RESERVED_AFTER_TABLE = new Set([
  'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'JOIN', 'INNER',
  'LEFT', 'RIGHT', 'FULL', 'CROSS', 'ON',
])
