import { isBinaryOp } from '../validation.js'
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
  const tok = current(state)

  // IS [NOT] NULL
  if (tok.type === 'keyword' && tok.value === 'IS') {
    consume(state)
    const notToken = current(state)
    if (notToken.type === 'keyword' && notToken.value === 'NOT') {
      consume(state)
      expect(state, 'keyword', 'NULL')
      return {
        type: 'unary',
        op: 'IS NOT NULL',
        argument: left,
      }
    }
    expect(state, 'keyword', 'NULL')
    return {
      type: 'unary',
      op: 'IS NULL',
      argument: left,
    }
  }

  // [NOT] LIKE
  if (tok.type === 'keyword' && tok.value === 'NOT') {
    const nextTok = peekToken(state, 1)
    if (nextTok.type === 'keyword' && nextTok.value === 'LIKE') {
      consume(state) // NOT
      consume(state) // LIKE
      const right = parseAdditive(state)
      return {
        type: 'unary',
        op: 'NOT',
        argument: {
          type: 'binary',
          op: 'LIKE',
          left,
          right,
        },
      }
    }
  }

  if (tok.type === 'keyword' && tok.value === 'LIKE') {
    consume(state)
    const right = parseAdditive(state)
    return {
      type: 'binary',
      op: 'LIKE',
      left,
      right,
    }
  }

  // [NOT] BETWEEN - convert to range comparison
  if (tok.type === 'keyword' && tok.value === 'NOT') {
    const nextTok = peekToken(state, 1)
    if (nextTok.type === 'keyword' && nextTok.value === 'BETWEEN') {
      consume(state) // NOT
      consume(state) // BETWEEN
      const lower = parseAdditive(state)
      expect(state, 'keyword', 'AND')
      const upper = parseAdditive(state)
      // NOT BETWEEN -> expr < lower OR expr > upper
      return {
        type: 'binary',
        op: 'OR',
        left: { type: 'binary', op: '<', left, right: lower },
        right: { type: 'binary', op: '>', left, right: upper },
      }
    }
  }

  if (tok.type === 'keyword' && tok.value === 'BETWEEN') {
    consume(state)
    const lower = parseAdditive(state)
    expect(state, 'keyword', 'AND')
    const upper = parseAdditive(state)
    // BETWEEN -> expr >= lower AND expr <= upper
    return {
      type: 'binary',
      op: 'AND',
      left: { type: 'binary', op: '>=', left, right: lower },
      right: { type: 'binary', op: '<=', left, right: upper },
    }
  }

  // [NOT] IN
  if (tok.type === 'keyword' && tok.value === 'NOT') {
    const nextTok = peekToken(state, 1)
    if (nextTok.type === 'keyword' && nextTok.value === 'IN') {
      consume(state) // NOT
      consume(state) // IN

      // Check if it's a subquery or a list of values by peeking ahead
      // parseSubquery expects to consume the opening paren itself
      const parenTok = current(state)
      if (parenTok.type !== 'paren' || parenTok.value !== '(') {
        throw new Error('Expected ( after IN')
      }
      const peekTok = peekToken(state, 1)
      if (peekTok.type === 'keyword' && peekTok.value === 'SELECT') {
        // Subquery - let parseSubquery handle the parens
        const subquery = parseSubquery(state)
        return {
          type: 'unary',
          op: 'NOT',
          argument: {
            type: 'in',
            expr: left,
            subquery,
          },
        }
      } else {
        // Parse list of values - we handle the parens
        consume(state) // '('
        /** @type {ExprNode[]} */
        const values = []
        while (true) {
          values.push(parseExpression(state))
          if (!match(state, 'comma')) break
        }
        expect(state, 'paren', ')')
        return {
          type: 'unary',
          op: 'NOT',
          argument: {
            type: 'in valuelist',
            expr: left,
            values,
          },
        }
      }
    }
  }

  if (tok.type === 'keyword' && tok.value === 'IN') {
    consume(state) // IN

    // Check if it's a subquery or a list of values by peeking ahead
    // parseSubquery expects to consume the opening paren itself
    const parenTok = current(state)
    if (parenTok.type !== 'paren' || parenTok.value !== '(') {
      throw new Error('Expected ( after IN')
    }
    const peekTok = peekToken(state, 1)
    if (peekTok.type === 'keyword' && peekTok.value === 'SELECT') {
      // Subquery - let parseSubquery handle the parens
      const subquery = parseSubquery(state)
      return {
        type: 'in',
        expr: left,
        subquery,
      }
    } else {
      // Parse list of values - we handle the parens
      consume(state) // '('
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
      }
    }
  }

  if (tok.type === 'operator' && isBinaryOp(tok.value)) {
    consume(state)
    const right = parseAdditive(state)
    return {
      type: 'binary',
      op: tok.value,
      left,
      right,
    }
  }

  return left
}
