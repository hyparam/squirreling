/**
 * @import { ExprCursor, ExprNode, BinaryOp } from '../types.js'
 */

/**
 * @param {ExprCursor} c
 * @returns {ExprNode}
 */
export function parseExpression(c) {
  return parseOr(c)
}

/**
 * Exposed so SELECT list parsing can reuse the same notion of "primary"
 * for function arguments, etc.
 *
 * @param {ExprCursor} c
 * @returns {ExprNode}
 */
export function parsePrimary(c) {
  const tok = c.current()

  if (tok.type === 'paren' && tok.value === '(') {
    c.consume()
    const expr = parseExpression(c)
    c.expect('paren', ')')
    return expr
  }

  if (tok.type === 'identifier') {
    const next = c.peek(1)

    // function call
    if (next.type === 'paren' && next.value === '(') {
      const funcName = tok.value
      // TODO: validate function name
      c.consume() // function name
      c.consume() // '('

      /** @type {ExprNode[]} */
      const args = []

      if (c.current().type !== 'paren' || c.current().value !== ')') {
        while (true) {
          const arg = parseExpression(c)
          args.push(arg)
          if (!c.match('comma')) break
        }
      }

      c.expect('paren', ')')

      return {
        type: 'function',
        name: funcName,
        args,
      }
    }

    c.consume()
    let name = tok.value

    // table.column
    if (c.current().type === 'dot') {
      c.consume()
      const columnTok = c.expectIdentifier()
      name = name + '.' + columnTok.value
    }

    return {
      type: 'identifier',
      name,
    }
  }

  if (tok.type === 'number') {
    c.consume()
    return {
      type: 'literal',
      value: tok.numericValue ?? null,
    }
  }

  if (tok.type === 'string') {
    c.consume()
    return {
      type: 'literal',
      value: tok.value,
    }
  }

  if (tok.type === 'keyword') {
    if (tok.value === 'TRUE') {
      c.consume()
      return { type: 'literal', value: true }
    }
    if (tok.value === 'FALSE') {
      c.consume()
      return { type: 'literal', value: false }
    }
    if (tok.value === 'NULL') {
      c.consume()
      return { type: 'literal', value: null }
    }
  }

  throw new Error(
    'Unexpected token in expression at position ' +
      tok.position +
      ': ' +
      tok.type +
      ' ' +
      tok.value
  )
}

/**
 * @param {ExprCursor} c
 * @returns {ExprNode}
 */
function parseOr(c) {
  let node = parseAnd(c)
  while (c.match('keyword', 'OR')) {
    const right = parseAnd(c)
    node = {
      type: 'binary',
      op: 'OR',
      left: node,
      right,
    }
  }
  return node
}

/**
 * @param {ExprCursor} c
 * @returns {ExprNode}
 */
function parseAnd(c) {
  let node = parseNot(c)
  while (c.match('keyword', 'AND')) {
    const right = parseNot(c)
    node = {
      type: 'binary',
      op: 'AND',
      left: node,
      right,
    }
  }
  return node
}

/**
 * @param {ExprCursor} c
 * @returns {ExprNode}
 */
function parseNot(c) {
  if (c.match('keyword', 'NOT')) {
    const argument = parseNot(c)
    return {
      type: 'unary',
      op: 'NOT',
      argument,
    }
  }
  return parseComparison(c)
}

/**
 * @param {ExprCursor} c
 * @returns {ExprNode}
 */
function parseComparison(c) {
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

  // LIKE
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

  if (tok.type === 'operator' && isComparisonOperator(tok.value)) {
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

/**
 * @param {string} op
 * @returns {op is BinaryOp}
 */
function isComparisonOperator(op) {
  return (
    op === '=' ||
    op === '!=' ||
    op === '<>' ||
    op === '<' ||
    op === '>' ||
    op === '<=' ||
    op === '>='
  )
}
