import { sqlEquals } from './utils.js'

/**
 * @import { AsyncRow, SqlPrimitive } from '../types.js'
 * @typedef {{ groups: Map<string, Map<SqlPrimitive, JoinIndex>>, rows: AsyncRow[] }} JoinIndex
 */

/**
 * Tuple index with exact primitive lookups and SQL equality for other types.
 * Keeping each column separate avoids serialized tuple collisions.
 * @returns {JoinIndex}
 */
export function joinIndex() {
  return { groups: new Map(), rows: [] }
}

/**
 * @param {JoinIndex} node
 * @param {SqlPrimitive[]} keys
 * @param {AsyncRow} row
 * @returns {void}
 */
export function addJoinRow(node, keys, row) {
  for (const value of keys) {
    const type = value instanceof Date ? 'date' : typeof value
    const key = value instanceof Date ? value.getTime() : value
    let group = node.groups.get(type)
    if (!group) node.groups.set(type, group = new Map())
    let child = group.get(key)
    if (!child) group.set(key, child = joinIndex())
    node = child
  }
  node.rows.push(row)
}

/**
 * @param {JoinIndex} node
 * @param {SqlPrimitive[]} keys
 * @param {number} [index]
 * @returns {Generator<AsyncRow>}
 * @yields {AsyncRow}
 */
export function* findJoinRows(node, keys, index = 0) {
  if (index === keys.length) {
    yield* node.rows
    return
  }
  const value = keys[index]
  const type = value instanceof Date ? 'date' : typeof value
  const key = value instanceof Date ? value.getTime() : value
  if (type !== 'object' && !Number.isNaN(key)) {
    const child = node.groups.get(type)?.get(key)
    if (child) yield* findJoinRows(child, keys, index + 1)
  }
  for (const [candidateType, group] of node.groups) {
    if (candidateType === type && type !== 'object') continue
    for (const [candidate, child] of group) {
      if (sqlEquals(value, candidateType === 'date' ? new Date(Number(candidate)) : candidate)) yield* findJoinRows(child, keys, index + 1)
    }
  }
}
