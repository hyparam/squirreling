import { isBinaryOp } from '../validation/functions.js'
import { SyntaxError } from '../validation/parseErrors.js'
import { parsePrimary } from './primary.js'
import { parseStatement } from './parse.js'
import { consume, current, expect, match } from './state.js'

/**
 * @import { ExprNode, ParserState } from '../types.js'
 */

// Precedence (lowest to highest):
// OR, AND, NOT, Comparison, Additive, Multiplicative, Primary

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
export function parseExpression(state) {
  let left = parseAnd(state)
  while (match(state, 'keyword', 'OR')) {
    const right = parseAnd(state)
    left = {
      type: 'binary',
      op: 'OR',
      left,
      right,
      positionStart: left.positionStart,
      positionEnd: right.positionEnd,
    }
  }
  return left
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseAnd(state) {
  let left = parseNot(state)
  while (match(state, 'keyword', 'AND')) {
    const right = parseNot(state)
    left = {
      type: 'binary',
      op: 'AND',
      left,
      right,
      positionStart: left.positionStart,
      positionEnd: right.positionEnd,
    }
  }
  return left
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseNot(state) {
  const tok = current(state)
  if (match(state, 'keyword', 'NOT')) {
    const { positionStart } = tok
    // Check for NOT EXISTS
    if (match(state, 'keyword', 'EXISTS')) {
      expect(state, 'paren', '(')
      const subquery = parseStatement(state)
      expect(state, 'paren', ')')
      return {
        type: 'not exists',
        subquery,
        positionStart,
        positionEnd: state.lastPos,
      }
    }
    const argument = parseNot(state)
    return {
      type: 'unary',
      op: 'NOT',
      argument,
      positionStart,
      positionEnd: argument.positionEnd,
    }
  }
  return parseComparison(state)
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseComparison(state) {
  const left = parseAdditive(state)
  const { positionStart } = left

  // IS [NOT] NULL
  if (match(state, 'keyword', 'IS')) {
    const op = match(state, 'keyword', 'NOT') ? 'IS NOT NULL' : 'IS NULL'
    expect(state, 'keyword', 'NULL')
    return {
      type: 'unary',
      op,
      argument: left,
      positionStart,
      positionEnd: state.lastPos,
    }
  }

  // Binary operators
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
          positionStart,
          positionEnd: right.positionEnd,
        },
        positionStart,
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
        left: { type: 'binary', op: '<', left, right: lower, positionStart, positionEnd: lower.positionEnd },
        right: { type: 'binary', op: '>', left, right: upper, positionStart, positionEnd: upper.positionEnd },
        positionStart,
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
        positionStart,
        positionEnd: node.positionEnd,
      }
    }

    const found = current(state)
    throw new SyntaxError({
      expected: 'LIKE, BETWEEN, or IN',
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
      positionStart,
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
      left: { type: 'binary', op: '>=', left, right: lower, positionStart, positionEnd: lower.positionEnd },
      right: { type: 'binary', op: '<=', left, right: upper, positionStart, positionEnd: upper.positionEnd },
      positionStart,
      positionEnd: upper.positionEnd,
    }
  }

  // IN
  if (match(state, 'keyword', 'IN')) {
    return parseIn(state, left)
  }

  const opTok = current(state)
  if (opTok.type === 'operator' && isBinaryOp(opTok.value)) {
    consume(state)
    const right = parseAdditive(state)
    return {
      type: 'binary',
      op: opTok.value,
      left,
      right,
      positionStart,
      positionEnd: right.positionEnd,
    }
  }

  return left
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseAdditive(state) {
  let left = parseMultiplicative(state)
  while (true) {
    const tok = current(state)
    if (tok.type === 'operator' && (tok.value === '+' || tok.value === '-')) {
      consume(state)
      const right = parseMultiplicative(state)
      // Recursive left-associative binary operator
      left = {
        type: 'binary',
        op: tok.value,
        left,
        right,
        positionStart: left.positionStart,
        positionEnd: right.positionEnd,
      }
    } else {
      break
    }
  }
  return left
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseMultiplicative(state) {
  let left = parsePrimary(state)
  while (true) {
    const tok = current(state)
    if (tok.type === 'operator' && (tok.value === '*' || tok.value === '/' || tok.value === '%')) {
      consume(state)
      const right = parsePrimary(state)
      // Recursively build left-associative tree for multiplicative operators
      left = {
        type: 'binary',
        op: tok.value,
        left,
        right,
        positionStart: left.positionStart,
        positionEnd: right.positionEnd,
      }
    } else {
      break
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
  expect(state, 'paren', '(')
  // Subquery
  const next = current(state)
  if (next.type === 'keyword' && next.value === 'SELECT') {
    const subquery = parseStatement(state)
    expect(state, 'paren', ')')
    return {
      type: 'in',
      expr: left,
      subquery,
      positionStart: left.positionStart,
      positionEnd: state.lastPos,
    }
  }
  // Value list
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
