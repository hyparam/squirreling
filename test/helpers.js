import { parseSql } from '../src/parse/parse.js'

/**
 * @import { SelectStatement, UserDefinedFunction, WithStatement } from '../src/types.js'
 */

/**
 * Parse a SQL query and assert it is a SELECT statement.
 * @param {string} query - The SQL query to parse
 * @returns {SelectStatement}
 */
export function parseSelect(query) {
  const stmt = parseSql({ query })
  if (stmt.type !== 'select') throw new Error('expected select')
  return stmt
}

/**
 * Parse a SQL query and assert it is a WITH statement.
 * @param {{ query: string, functions?: Record<string, UserDefinedFunction> }} options
 * @returns {WithStatement}
 */
export function parseWith(options) {
  const stmt = parseSql(options)
  if (stmt.type !== 'with') throw new Error('expected with')
  return stmt
}

/**
 * Wraps an AsyncBuffer to count the number of fetches made
 *
 * @import {AsyncBuffer} from 'hyparquet'
 * @param {AsyncBuffer} asyncBuffer
 * @returns {AsyncBuffer & {fetches: number, bytes: number}}
 */
export function countingBuffer(asyncBuffer) {
  return {
    ...asyncBuffer,
    fetches: 0,
    bytes: 0,
    slice(start, end) {
      this.fetches++
      this.bytes += (end ?? asyncBuffer.byteLength) - start
      return asyncBuffer.slice(start, end)
    },
  }
}
