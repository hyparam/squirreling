import { isBinaryOp } from '../validation.js'
import { parseExpression, parsePrimary } from './expression.js'

/**
 * @import { ExprCursor, ExprNode } from '../types.js'
 */

/**
 * @param {ExprCursor} c
 * @returns {ExprNode}
 */
export function parseComparison(c) {
  const left = parsePrimary(c)
  const tok = c.current()

  // IS [NOT] NULL
  if (tok.type === 'keyword' && tok.value === 'IS') {
    c.consume()
    const notToken = c.current()
    if (notToken.type === 'keyword' && notToken.value === 'NOT') {
      c.consume()
      c.expect('keyword', 'NULL')
      return {
        type: 'unary',
        op: 'IS NOT NULL',
        argument: left,
      }
    }
    c.expect('keyword', 'NULL')
    return {
      type: 'unary',
      op: 'IS NULL',
      argument: left,
    }
  }

  // [NOT] LIKE
  if (tok.type === 'keyword' && tok.value === 'NOT') {
    const nextTok = c.peek(1)
    if (nextTok.type === 'keyword' && nextTok.value === 'LIKE') {
      c.consume() // NOT
      c.consume() // LIKE
      const right = parsePrimary(c)
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
    c.consume()
    const right = parsePrimary(c)
    return {
      type: 'binary',
      op: 'LIKE',
      left,
      right,
    }
  }

  // [NOT] BETWEEN - convert to range comparison
  if (tok.type === 'keyword' && tok.value === 'NOT') {
    const nextTok = c.peek(1)
    if (nextTok.type === 'keyword' && nextTok.value === 'BETWEEN') {
      c.consume() // NOT
      c.consume() // BETWEEN
      const lower = parsePrimary(c)
      c.expect('keyword', 'AND')
      const upper = parsePrimary(c)
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
    c.consume()
    const lower = parsePrimary(c)
    c.expect('keyword', 'AND')
    const upper = parsePrimary(c)
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
    const nextTok = c.peek(1)
    if (nextTok.type === 'keyword' && nextTok.value === 'IN') {
      c.consume() // NOT
      c.consume() // IN

      // Check if it's a subquery or a list of values by peeking ahead
      // parseSubquery expects to consume the opening paren itself
      const parenTok = c.current()
      if (parenTok.type !== 'paren' || parenTok.value !== '(') {
        throw new Error('Expected ( after IN')
      }
      const peekTok = c.peek(1)
      if (peekTok.type === 'keyword' && peekTok.value === 'SELECT') {
        // Subquery - let parseSubquery handle the parens
        const subquery = c.parseSubquery()
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
        c.consume() // '('
        /** @type {ExprNode[]} */
        const values = []
        while (true) {
          values.push(parseExpression(c))
          if (!c.match('comma')) break
        }
        c.expect('paren', ')')
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
    c.consume() // IN

    // Check if it's a subquery or a list of values by peeking ahead
    // parseSubquery expects to consume the opening paren itself
    const parenTok = c.current()
    if (parenTok.type !== 'paren' || parenTok.value !== '(') {
      throw new Error('Expected ( after IN')
    }
    const peekTok = c.peek(1)
    if (peekTok.type === 'keyword' && peekTok.value === 'SELECT') {
      // Subquery - let parseSubquery handle the parens
      const subquery = c.parseSubquery()
      return {
        type: 'in',
        expr: left,
        subquery,
      }
    } else {
      // Parse list of values - we handle the parens
      c.consume() // '('
      /** @type {ExprNode[]} */
      const values = []
      while (true) {
        values.push(parseExpression(c))
        if (!c.match('comma')) break
      }
      c.expect('paren', ')')
      return {
        type: 'in valuelist',
        expr: left,
        values,
      }
    }
  }

  if (tok.type === 'operator' && isBinaryOp(tok.value)) {
    c.consume()
    const right = parsePrimary(c)
    return {
      type: 'binary',
      op: tok.value,
      left,
      right,
    }
  }

  return left
}
