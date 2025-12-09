
/**
 * @import {AggregateFunc, BinaryOp, ComparisonOp, IntervalUnit, MathFunc, StringFunc} from './types.js'
 * @param {string} name
 * @returns {name is AggregateFunc}
 */
export function isAggregateFunc(name) {
  return ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'JSON_ARRAYAGG'].includes(name)
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
    'RANDOM',
    'RAND',
    'JSON_VALUE',
    'JSON_QUERY',
    'JSON_OBJECT',
    'CURRENT_DATE',
    'CURRENT_TIME',
    'CURRENT_TIMESTAMP',
  ].includes(name)
}

/**
 * @param {string} op
 * @returns {op is BinaryOp}
 */
export function isBinaryOp(op) {
  return ['AND', 'OR', 'LIKE', '=', '!=', '<>', '<', '>', '<=', '>='].includes(op)
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
