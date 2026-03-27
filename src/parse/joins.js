import { expectNoAggregate } from '../validation/aggregates.js'
import { parseExpression } from './expression.js'
import { parseTableAlias } from './parse.js'
import { current, expect, match } from './state.js'

/**
 * @import { ExprNode, JoinClause, JoinType, ParserState } from '../types.js'
 */

/**
 * @param {ParserState} state
 * @returns {JoinClause[]}
 */
export function parseJoins(state) {
  /** @type {JoinClause[]} */
  const joins = []

  while (true) {
    const tok = current(state)

    // Check for join keywords
    /** @type {JoinType} */
    let joinType = 'INNER'

    if (match(state, 'keyword', 'INNER')) {
      joinType = 'INNER'
    } else if (match(state, 'keyword', 'LEFT')) {
      match(state, 'keyword', 'OUTER') // LEFT OUTER JOIN
      joinType = 'LEFT'
    } else if (match(state, 'keyword', 'RIGHT')) {
      match(state, 'keyword', 'OUTER') // RIGHT OUTER JOIN
      joinType = 'RIGHT'
    } else if (match(state, 'keyword', 'FULL')) {
      match(state, 'keyword', 'OUTER') // FULL OUTER JOIN
      joinType = 'FULL'
    } else if (match(state, 'keyword', 'POSITIONAL')) {
      joinType = 'POSITIONAL'
    } else if (!match(state, 'keyword', 'JOIN')) {
      // Not a join keyword, stop parsing joins
      break
    }

    // If we consumed a type keyword, expect JOIN next
    if (tok.value !== 'JOIN') {
      expect(state, 'keyword', 'JOIN')
    }

    // Parse table name and optional alias
    const tableTok = expect(state, 'identifier')
    const tableName = tableTok.value
    const tableAlias = parseTableAlias(state)

    // Parse ON condition (not for POSITIONAL joins)
    /** @type {ExprNode | undefined} */
    let condition
    if (joinType !== 'POSITIONAL') {
      expect(state, 'keyword', 'ON')
      condition = parseExpression(state)
      expectNoAggregate(condition, 'JOIN ON')
    }

    joins.push({
      joinType,
      table: tableName,
      alias: tableAlias,
      on: condition,
      positionStart: tableTok.positionStart,
      positionEnd: tableTok.positionEnd,
    })
  }

  return joins
}
