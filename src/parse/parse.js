/**
 * @import { AggregateColumn, AggregateArg, AggregateFunc, ExprCursor, ExprNode, FromSubquery, JoinClause, JoinType, OrderByItem, ParserState, SelectStatement, SelectColumn, StringFunc, Token, TokenType } from '../types.js'
 */

import { tokenize } from './tokenize.js'
import { parseExpression, parsePrimary } from './expression.js'
import { isAggregateFunc, isStringFunc } from '../validation.js'

// Keywords that cannot be used as implicit aliases after a column
const RESERVED_AFTER_COLUMN = new Set([
  'FROM',
  'WHERE',
  'GROUP',
  'HAVING',
  'ORDER',
  'LIMIT',
  'OFFSET',
])

/**
 * @param {string} query
 * @returns {SelectStatement}
 */
export function parseSql(query) {
  const tokens = tokenize(query)
  /** @type {ParserState} */
  const state = { tokens, pos: 0 }
  const select = parseSelectInternal(state)

  const tok = current(state)
  if (tok.type !== 'eof') {
    throw parseError(state, 'end of query')
  }

  return select
}

/**
 * @param {ParserState} state
 * @returns {Token}
 */
function current(state) {
  return state.tokens[state.pos]
}

/**
 * @param {ParserState} state
 * @param {number} offset
 * @returns {Token}
 */
function peekToken(state, offset) {
  const idx = state.pos + offset
  if (idx >= state.tokens.length) {
    return state.tokens[state.tokens.length - 1]
  }
  return state.tokens[idx]
}

/**
 * @param {ParserState} state
 * @returns {Token}
 */
function consume(state) {
  const tok = current(state)
  if (state.pos < state.tokens.length - 1) {
    state.pos += 1
  }
  return tok
}

/**
 * @param {ParserState} state
 * @param {TokenType} type
 * @param {string} [value]
 * @returns {boolean}
 */
function match(state, type, value) {
  const tok = current(state)
  if (tok.type !== type) return false
  if (typeof value === 'string' && tok.value !== value) return false
  consume(state)
  return true
}

/**
 * @param {ParserState} state
 * @param {TokenType} type
 * @param {string} value
 * @returns {Token}
 */
function expect(state, type, value) {
  const tok = current(state)
  if (tok.type !== type || tok.value !== value) {
    throw parseError(state, value)
  }
  consume(state)
  return tok
}

/**
 * @param {ParserState} state
 * @returns {Token}
 */
function expectIdentifier(state) {
  const tok = current(state)
  if (tok.type !== 'identifier') {
    throw parseError(state, 'identifier')
  }
  consume(state)
  return tok
}

/**
 * Creates an ExprCursor adapter for the ParserState.
 * @param {ParserState} state
 * @returns {ExprCursor}
 */
function createExprCursor(state) {
  return {
    current: () => current(state),
    peek: (offset) => peekToken(state, offset),
    consume: () => consume(state),
    match: (type, value) => match(state, type, value),
    expect: (type, value) => expect(state, type, value),
    expectIdentifier: () => expectIdentifier(state),
  }
}

/**
 * @param {ParserState} state
 * @returns {SelectColumn[]}
 */
function parseSelectList(state) {
  /** @type {SelectColumn[]} */
  const cols = []
  const tok = current(state)

  if (tok.type === 'operator' && tok.value === '*') {
    consume(state)
    cols.push({ kind: 'star' })
    return cols
  }

  while (true) {
    cols.push(parseSelectItem(state))
    if (!match(state, 'comma')) break
  }

  return cols
}

/**
 * @param {ParserState} state
 * @returns {SelectColumn}
 */
function parseSelectItem(state) {
  const tok = current(state)

  if (tok.type !== 'identifier' && tok.type !== 'operator') {
    throw parseError(state, 'column name or expression')
  }

  if (tok.type === 'identifier' && tok.value === 'CAST') {
    expectIdentifier(state) // consume CAST
    expect(state, 'paren', '(')
    const cursor = createExprCursor(state)
    const expr = parseExpression(cursor)
    expect(state, 'keyword', 'AS')
    const typeTok = expectIdentifier(state)
    expect(state, 'paren', ')')
    const alias = parseAs(state)
    return {
      kind: 'operation',
      expr: { type: 'cast', expr, toType: typeTok.value },
      alias,
    }
  }

  if (tok.type === 'operator') {
    // Handle SELECT expression AS alias
    const cursor = createExprCursor(state)
    const expr = parseExpression(cursor)
    const alias = parseAs(state)
    return { kind: 'operation', expr, alias }
  }

  const next = peekToken(state, 1)
  const upper = tok.value.toUpperCase()

  if (next.type === 'paren' && next.value === '(') {
    expectIdentifier(state) // consume function name
    if (isAggregateFunc(upper)) {
      return parseAggregateItem(state, upper)
    }
    if (isStringFunc(upper)) {
      return parseStringFunctionItem(state, upper)
    }
  }

  consume(state)
  let column = tok.value

  // Handle dot notation (table.column)
  if (current(state).type === 'dot') {
    consume(state) // consume the dot
    const columnTok = expectIdentifier(state)
    column += '.' + columnTok.value
  }

  const alias = parseAs(state)

  return { kind: 'column', column, alias }
}

/**
 * @param {ParserState} state
 * @param {AggregateFunc} func
 * @returns {AggregateColumn}
 */
function parseAggregateItem(state, func) {
  expect(state, 'paren', '(')

  /** @type {AggregateArg} */
  let arg

  const cur = current(state)
  if (cur.type === 'operator' && cur.value === '*') {
    consume(state)
    arg = { kind: 'star' }
  } else if (cur.type === 'identifier' && cur.value === 'CAST') {
    // Handle CAST inside aggregate: SUM(CAST(x AS type))
    expectIdentifier(state) // consume CAST
    expect(state, 'paren', '(')
    const cursor = createExprCursor(state)
    const expr = parseExpression(cursor)
    expect(state, 'keyword', 'AS')
    const typeTok = expectIdentifier(state)
    expect(state, 'paren', ')')
    arg = {
      kind: 'expression',
      expr: { type: 'cast', expr, toType: typeTok.value },
    }
  } else {
    const colTok = expectIdentifier(state)
    arg = {
      kind: 'expression',
      expr: { type: 'identifier', name: colTok.value },
    }
  }

  expect(state, 'paren', ')')

  const alias = parseAs(state)

  return { kind: 'aggregate', func, arg, alias }
}

/**
 * @param {ParserState} state
 * @param {StringFunc} func
 * @returns {SelectColumn}
 */
function parseStringFunctionItem(state, func) {
  expect(state, 'paren', '(')

  /** @type {ExprNode[]} */
  const args = []

  // Parse comma-separated arguments
  if (current(state).type !== 'paren' || current(state).value !== ')') {
    const cursor = createExprCursor(state)
    while (true) {
      const arg = parsePrimary(cursor)
      args.push(arg)
      if (!match(state, 'comma')) break
    }
  }

  expect(state, 'paren', ')')

  const alias = parseAs(state)

  return { kind: 'function', func, args, alias }
}

/**
 * @param {ParserState} state
 * @returns {string | undefined}
 */
function parseAs(state) {
  if (match(state, 'keyword', 'AS')) {
    // After AS, allow keywords as aliases (except reserved ones)
    const aliasTok = current(state)
    if (aliasTok.type === 'identifier') {
      consume(state)
      return aliasTok.value
    } else if (aliasTok.type === 'keyword' && !RESERVED_AFTER_COLUMN.has(aliasTok.value.toUpperCase())) {
      consume(state)
      // Use original case for keywords used as aliases
      return aliasTok.originalValue ?? aliasTok.value
    } else {
      throw parseError(state, 'alias')
    }
  } else {
    // Implicit alias SELECT UPPER(name) name_upper
    const maybeAlias = current(state)
    if (maybeAlias.type === 'identifier' && !RESERVED_AFTER_COLUMN.has(maybeAlias.value.toUpperCase())) {
      consume(state)
      return maybeAlias.value
    }
  }
}

/**
 * @param {ParserState} state
 * @returns {JoinClause[]}
 */
function parseJoins(state) {
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
        if (match(state, 'keyword', 'OUTER')) {
          // LEFT OUTER JOIN
        }
        joinType = 'LEFT'
      } else if (tok.value === 'RIGHT') {
        consume(state)
        if (match(state, 'keyword', 'OUTER')) {
          // RIGHT OUTER JOIN
        }
        joinType = 'RIGHT'
      } else if (tok.value === 'FULL') {
        consume(state)
        if (match(state, 'keyword', 'OUTER')) {
          // FULL OUTER JOIN
        }
        joinType = 'FULL'
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

    // Parse table name
    const tableTok = expectIdentifier(state)
    const tableName = tableTok.value

    // Parse ON condition
    expect(state, 'keyword', 'ON')
    const cursor = createExprCursor(state)
    const condition = parseExpression(cursor)

    joins.push({
      type: joinType,
      table: tableName,
      on: condition,
    })
  }

  return joins
}

/**
 * Parses a subquery in parentheses with an alias
 * @param {ParserState} state
 * @returns {FromSubquery}
 */
function parseSubquery(state) {
  expect(state, 'paren', '(')
  const query = parseSelectInternal(state)
  expect(state, 'paren', ')')
  expect(state, 'keyword', 'AS')
  const aliasTok = expectIdentifier(state)
  return {
    kind: 'subquery',
    query,
    alias: aliasTok.value,
  }
}

/**
 * @param {ParserState} state
 * @returns {SelectStatement}
 */
function parseSelectInternal(state) {
  expect(state, 'keyword', 'SELECT')

  let distinct = false
  if (match(state, 'keyword', 'DISTINCT')) {
    distinct = true
  }

  const columns = parseSelectList(state)

  expect(state, 'keyword', 'FROM')

  // Check if it's a subquery or table name
  let from
  const tok = current(state)
  if (tok.type === 'paren' && tok.value === '(') {
    // Subquery: SELECT * FROM (SELECT ...) AS alias
    from = parseSubquery(state)
  } else {
    // Simple table name: SELECT * FROM users
    from = expectIdentifier(state).value
  }

  // Parse JOIN clauses
  const joins = parseJoins(state)

  /** @type {ExprNode | undefined} */
  let where
  /** @type {ExprNode[]} */
  const groupBy = []
  /** @type {ExprNode | undefined} */
  let having
  /** @type {OrderByItem[]} */
  const orderBy = []
  /** @type {number | undefined} */
  let limit
  /** @type {number | undefined} */
  let offset

  const cursor = createExprCursor(state)

  if (match(state, 'keyword', 'WHERE')) {
    where = parseExpression(cursor)
  }

  if (match(state, 'keyword', 'GROUP')) {
    expect(state, 'keyword', 'BY')
    while (true) {
      const expr = parseExpression(cursor)
      groupBy.push(expr)
      if (!match(state, 'comma')) break
    }
  }

  if (match(state, 'keyword', 'HAVING')) {
    having = parseExpression(cursor)
  }

  if (match(state, 'keyword', 'ORDER')) {
    expect(state, 'keyword', 'BY')
    while (true) {
      const expr = parseExpression(cursor)
      /** @type {'ASC' | 'DESC'} */
      let direction = 'ASC'
      if (match(state, 'keyword', 'ASC')) {
        direction = 'ASC'
      } else if (match(state, 'keyword', 'DESC')) {
        direction = 'DESC'
      }
      orderBy.push({
        expr,
        direction,
      })
      if (!match(state, 'comma')) break
    }
  }

  if (match(state, 'keyword', 'LIMIT')) {
    const tok = current(state)
    if (tok.type !== 'number') {
      throw parseError(state, 'numeric LIMIT')
    }
    consume(state)
    const n = parseInt(tok.value, 10)
    if (!Number.isFinite(n)) {
      throw parseError(state, 'valid LIMIT value')
    }
    limit = n

    if (match(state, 'keyword', 'OFFSET')) {
      const oTok = current(state)
      if (oTok.type !== 'number') {
        throw parseError(state, 'numeric OFFSET')
      }
      consume(state)
      const off = parseInt(oTok.value, 10)
      if (!Number.isFinite(off)) {
        throw parseError(state, 'valid OFFSET value')
      }
      offset = off
    }
  } else if (match(state, 'keyword', 'OFFSET')) {
    const oTok = current(state)
    if (oTok.type !== 'number') {
      throw parseError(state, 'numeric OFFSET')
    }
    consume(state)
    const off = parseInt(oTok.value, 10)
    if (!Number.isFinite(off)) {
      throw parseError(state, 'valid OFFSET value')
    }
    offset = off
  }

  // optional trailing semicolon
  if (current(state).type === 'semicolon') {
    consume(state)
  }

  return {
    distinct,
    columns,
    from,
    joins,
    where,
    groupBy,
    having,
    orderBy,
    limit,
    offset,
  }
}

/**
 * Helper function to create consistent parser error messages.
 * @param {ParserState} state
 * @param {string} expected - Description of what was expected
 * @returns {Error}
 */
function parseError(state, expected) {
  const tok = current(state)
  const prevToken = state.tokens[state.pos - 1]
  const after = prevToken ? ` after "${prevToken.originalValue ?? prevToken.value}"` : ''
  const found = tok.type === 'eof' ? 'end of query' : `"${tok.originalValue ?? tok.value}"`
  return new Error(`Expected ${expected}${after} but found ${found} at position ${tok.position}`)
}
