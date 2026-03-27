import { isBinaryOp } from '../validation/functions.js'
import { syntaxError } from '../validation/parseErrors.js'
import { parseAdditive, parseExpression, parseSubquery } from './expression.js'
import { consume, current, expect, match, peekToken } from './state.js'

/**
 * @import { ExprNode, ParserState } from '../types.js'
 */

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
export function parseComparison(state) {
  const left = parseAdditive(state)

  // IS [NOT] NULL
  if (match(state, 'keyword', 'IS')) {
    const op = match(state, 'keyword', 'NOT') ? 'IS NOT NULL' : 'IS NULL'
    expect(state, 'keyword', 'NULL')
    return {
      type: 'unary',
      op,
      argument: left,
      positionStart: left.positionStart,
      positionEnd: state.lastPos,
    }
  }

  // Binary operators
  const opTok = current(state)
  if (match(state, 'keyword', 'NOT')) {
    // NOT LIKE
    if (match(state, 'keyword', 'LIKE')) {
      const right = parseAdditive(state)
      return {
        type: 'unary',
        op: 'NOT',
        argument: {
          type: 'binary',
          op: 'LIKE',
          left,
          right,
          positionStart: left.positionStart,
          positionEnd: right.positionEnd,
        },
        positionStart: opTok.positionStart,
        positionEnd: right.positionEnd,
      }
    }

    // NOT BETWEEN - convert to range comparison
    if (match(state, 'keyword', 'BETWEEN')) {
      const lower = parseAdditive(state)
      expect(state, 'keyword', 'AND')
      const upper = parseAdditive(state)
      // NOT BETWEEN -> expr < lower OR expr > upper
      return {
        type: 'binary',
        op: 'OR',
        left: { type: 'binary', op: '<', left, right: lower, positionStart: left.positionStart, positionEnd: lower.positionEnd },
        right: { type: 'binary', op: '>', left, right: upper, positionStart: left.positionStart, positionEnd: upper.positionEnd },
        positionStart: opTok.positionStart,
        positionEnd: upper.positionEnd,
      }
    }

    // NOT IN
    if (match(state, 'keyword', 'IN')) {
      const node = parseIn(state, left)
      return {
        type: 'unary',
        op: 'NOT',
        argument: node,
        positionStart: opTok.positionStart,
        positionEnd: node.positionEnd,
      }
    }

    const found = current(state)
    throw syntaxError({
      expected: 'LIKE, BETWEEN, or IN',
      received: found.type === 'eof' ? 'end of query' : `"${found.originalValue ?? found.value}"`,
      after: 'NOT',
      ...found,
    })
  }

  // LIKE
  if (match(state, 'keyword', 'LIKE')) {
    const right = parseAdditive(state)
    return {
      type: 'binary',
      op: 'LIKE',
      left,
      right,
      positionStart: left.positionStart,
      positionEnd: right.positionEnd,
    }
  }

  // BETWEEN - convert to range comparison
  if (match(state, 'keyword', 'BETWEEN')) {
    const lower = parseAdditive(state)
    expect(state, 'keyword', 'AND')
    const upper = parseAdditive(state)
    // BETWEEN -> expr >= lower AND expr <= upper
    return {
      type: 'binary',
      op: 'AND',
      left: { type: 'binary', op: '>=', left, right: lower, positionStart: left.positionStart, positionEnd: lower.positionEnd },
      right: { type: 'binary', op: '<=', left, right: upper, positionStart: left.positionStart, positionEnd: upper.positionEnd },
      positionStart: left.positionStart,
      positionEnd: upper.positionEnd,
    }
  }

  // IN
  if (match(state, 'keyword', 'IN')) {
    return parseIn(state, left)
  }

  if (opTok.type === 'operator' && isBinaryOp(opTok.value)) {
    consume(state)
    const right = parseAdditive(state)
    return {
      type: 'binary',
      op: opTok.value,
      left,
      right,
      positionStart: left.positionStart,
      positionEnd: right.positionEnd,
    }
  }

  return left
}

/**
 * Parses an IN expression (subquery or value list).
 *
 * @param {ParserState} state
 * @param {ExprNode} left
 * @returns {ExprNode}
 */
function parseIn(state, left) {
  // Subquery
  if (peekToken(state, 0).type === 'paren' && peekToken(state, 1).value === 'SELECT') {
    const subquery = parseSubquery(state)
    return {
      type: 'in',
      expr: left,
      subquery,
      positionStart: left.positionStart,
      positionEnd: state.lastPos,
    }
  }
  // Value list
  expect(state, 'paren', '(')
  /** @type {ExprNode[]} */
  const values = []
  while (true) {
    values.push(parseExpression(state))
    if (!match(state, 'comma')) break
  }
  expect(state, 'paren', ')')
  return {
    type: 'in valuelist',
    expr: left,
    values,
    positionStart: left.positionStart,
    positionEnd: state.lastPos,
  }
}
