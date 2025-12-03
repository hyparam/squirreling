
/**
 * @import {AggregateFunc, StringFunc} from './types.js'
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
