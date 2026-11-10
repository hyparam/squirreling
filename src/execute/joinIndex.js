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
  for (const key of keys) {
    const type = typeof key
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
  const type = typeof value
  if (type !== 'object' && !Number.isNaN(value)) {
    const child = node.groups.get(type)?.get(value)
    if (child) yield* findJoinRows(child, keys, index + 1)
  }
  for (const [candidateType, group] of node.groups) {
    if (candidateType === type && type !== 'object') continue
    for (const [candidate, child] of group) {
      if (sqlEquals(value, candidate)) yield* findJoinRows(child, keys, index + 1)
    }
  }
}
