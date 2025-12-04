
/**
 * @import {AggregateFunc, BinaryOp, StringFunc} from './types.js'
 * @param {string} name
 * @returns {name is AggregateFunc}
 */
export function isAggregateFunc(name) {
  return ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(name)
}

/**
 * @param {string} name
 * @returns {name is StringFunc}
 */
export function isStringFunc(name) {
  return ['UPPER', 'LOWER', 'CONCAT', 'LENGTH', 'SUBSTRING', 'SUBSTR', 'TRIM', 'REPLACE', 'RANDOM', 'RAND'].includes(name)
}

/**
 * @param {string} op
 * @returns {op is BinaryOp}
 */
export function isBinaryOp(op) {
  return ['=', '!=', '<>', '<', '>', '<=', '>='].includes(op)
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
