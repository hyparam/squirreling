import { parseExpression } from './expression.js'
import { parseTableAlias } from './parse.js'
import { consume, current, expect, expectIdentifier, match } from './state.js'
import { expectNoAggregate } from '../validation.js'

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

    if (tok.type === 'keyword') {
      if (tok.value === 'INNER') {
        consume(state)
        joinType = 'INNER'
      } else if (tok.value === 'LEFT') {
        consume(state)
        match(state, 'keyword', 'OUTER') // LEFT OUTER JOIN
        joinType = 'LEFT'
      } else if (tok.value === 'RIGHT') {
        consume(state)
        match(state, 'keyword', 'OUTER') // RIGHT OUTER JOIN
        joinType = 'RIGHT'
      } else if (tok.value === 'FULL') {
        consume(state)
        match(state, 'keyword', 'OUTER') // FULL OUTER JOIN
        joinType = 'FULL'
      } else if (tok.value === 'POSITIONAL') {
        consume(state)
        joinType = 'POSITIONAL'
      } else if (tok.value === 'JOIN') {
        // Just JOIN (defaults to INNER)
        consume(state)
      } else {
        // Not a join keyword, stop parsing joins
        break
      }

      // If we consumed a join type keyword (INNER/LEFT/RIGHT/FULL), expect JOIN
      if (tok.value !== 'JOIN') {
        expect(state, 'keyword', 'JOIN')
      }
    } else {
      // No more joins
      break
    }

    // Parse table name and optional alias
    const tableName = expectIdentifier(state).value
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
    })
  }

  return joins
}
