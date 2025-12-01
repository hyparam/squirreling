import { isAggregateFunc, isStringFunc } from '../validation.js'

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
 * @param {ExprCursor} c
 * @returns {ExprNode}
 */
function parsePrimary(c) {
  const tok = c.current()

  if (tok.type === 'paren' && tok.value === '(') {
    // Peek ahead to see if this is a scalar subquery
    const nextTok = c.peek(1)
    if (nextTok.type === 'keyword' && nextTok.value === 'SELECT') {
      // It's a scalar subquery
      const subquery = c.parseSubquery()
      return {
        type: 'subquery',
        subquery,
      }
    }
    // Regular grouped expression
    c.consume()
    const expr = parseExpression(c)
    c.expect('paren', ')')
    return expr
  }

  if (tok.type === 'identifier') {
    const next = c.peek(1)

    // CAST expression
    if (tok.value === 'CAST' && next.type === 'paren' && next.value === '(') {
      c.consume() // CAST
      c.consume() // '('
      const expr = parseExpression(c)
      c.expect('keyword', 'AS')
      const typeTok = c.expectIdentifier()
      c.expect('paren', ')')
      return {
        type: 'cast',
        expr,
        toType: typeTok.value,
      }
    }

    // function call
    if (next.type === 'paren' && next.value === '(') {
      const funcName = tok.value

      // validate function names
      if (!isStringFunc(funcName) && !isAggregateFunc(funcName)) {
        throw new Error(`Unknown function "${funcName}" at position ${tok.position}`)
      }

      c.consume() // function name
      c.consume() // '('

      /** @type {ExprNode[]} */
      const args = []

      if (c.current().type !== 'paren' || c.current().value !== ')') {
        while (true) {
          // Handle COUNT(*) - treat * as a special identifier
          if (c.current().type === 'operator' && c.current().value === '*') {
            c.consume()
            args.push({
              type: 'identifier',
              name: '*',
            })
          } else {
            const arg = parseExpression(c)
            args.push(arg)
          }
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
    if (tok.value === 'EXISTS') {
      c.consume() // EXISTS
      const subquery = c.parseSubquery()
      return {
        type: 'exists',
        subquery,
      }
    }
    if (tok.value === 'CASE') {
      c.consume() // CASE

      // Check if it's simple CASE (CASE expr WHEN ...) or searched CASE (CASE WHEN ...)
      /** @type {import('../types.js').ExprNode | undefined} */
      let caseExpr
      const nextTok = c.current()
      if (nextTok.type !== 'keyword' || nextTok.value !== 'WHEN') {
        // Simple CASE: parse the case expression
        caseExpr = parseExpression(c)
      }

      // Parse WHEN clauses
      /** @type {import('../types.js').WhenClause[]} */
      const whenClauses = []
      while (c.match('keyword', 'WHEN')) {
        const condition = parseExpression(c)
        c.expect('keyword', 'THEN')
        const result = parseExpression(c)
        whenClauses.push({ condition, result })
      }

      if (whenClauses.length === 0) {
        throw new Error('CASE expression must have at least one WHEN clause')
      }

      // Parse optional ELSE clause
      /** @type {import('../types.js').ExprNode | undefined} */
      let elseResult
      if (c.match('keyword', 'ELSE')) {
        elseResult = parseExpression(c)
      }

      c.expect('keyword', 'END')

      return {
        type: 'case',
        caseExpr,
        whenClauses,
        elseResult,
      }
    }
  }

  if (tok.type === 'operator' && tok.value === '-') {
    c.consume()
    const argument = parsePrimary(c)
    return {
      type: 'unary',
      op: '-',
      argument,
    }
  }

  const found = tok.type === 'eof' ? 'end of query' : `"${tok.originalValue ?? tok.value}"`
  throw new Error(`Expected expression but found ${found} at position ${tok.position}`)
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
    // Check for NOT EXISTS
    const nextTok = c.current()
    if (nextTok.type === 'keyword' && nextTok.value === 'EXISTS') {
      c.consume() // EXISTS
      if (!c.parseSubquery) {
        throw new Error('Subquery parsing not available in this context')
      }
      const subquery = c.parseSubquery()
      return {
        type: 'not exists',
        subquery,
      }
    }
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

  // [NOT] BETWEEN
  if (tok.type === 'keyword' && tok.value === 'NOT') {
    const nextTok = c.peek(1)
    if (nextTok.type === 'keyword' && nextTok.value === 'BETWEEN') {
      c.consume() // NOT
      c.consume() // BETWEEN
      const lower = parsePrimary(c)
      c.expect('keyword', 'AND')
      const upper = parsePrimary(c)
      return {
        type: 'not between',
        expr: left,
        lower,
        upper,
      }
    }
  }

  if (tok.type === 'keyword' && tok.value === 'BETWEEN') {
    c.consume()
    const lower = parsePrimary(c)
    c.expect('keyword', 'AND')
    const upper = parsePrimary(c)
    return {
      type: 'between',
      expr: left,
      lower,
      upper,
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
        if (!c.parseSubquery) {
          throw new Error('Subquery parsing not available in this context')
        }
        const subquery = c.parseSubquery()
        return {
          type: 'not in',
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
          type: 'not in valuelist',
          expr: left,
          values,
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
      if (!c.parseSubquery) {
        throw new Error('Subquery parsing not available in this context')
      }
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
