/**
 * @import { AggregateColumn, AggregateArg, AggregateFunc, BinaryOp, ExprNode, SelectAst, SelectColumn, ParserState, Token, TokenType, OrderByItem, JoinClause, JoinType } from './types.js'
 */

import { tokenize } from './tokenize.js'
import { parseExpression, parsePrimary } from './expression.js'

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

/** @type {Set<AggregateFunc>} */
const AGG_FUNCS = new Set([
  'COUNT',
  'SUM',
  'AVG',
  'MIN',
  'MAX',
])

const STRING_FUNCS = new Set([
  'UPPER',
  'LOWER',
  'CONCAT',
  'LENGTH',
  'SUBSTRING',
  'TRIM',
])

/**
 * @param {ParserState} state
 * @returns {Token}
 */
function current(state) {
  return state.tokens[state.pos]
}

/**
 * @param {ParserState} state
 * @param {number} [offset=0]
 * @returns {Token}
 */
function peekToken(state, offset = 0) {
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
 * @param {string} keywordUpper
 * @returns {boolean}
 */
function matchKeyword(state, keywordUpper) {
  const tok = current(state)
  if (tok.type === 'keyword' && tok.value === keywordUpper) {
    consume(state)
    return true
  }
  return false
}

/**
 * @param {ParserState} state
 * @param {TokenType} type
 * @param {string} [value]
 * @returns {Token}
 */
function expect(state, type, value) {
  const tok = current(state)
  if (tok.type !== type) {
    throw new Error('Expected ' + type + ' but found ' + tok.type + ' at position ' + tok.position)
  }
  if (typeof value === 'string' && tok.value !== value) {
    throw new Error('Expected ' + value + ' but found ' + tok.value + ' at position ' + tok.position)
  }
  consume(state)
  return tok
}

/**
 * @param {ParserState} state
 * @param {string} keywordUpper
 * @returns {Token}
 */
function expectKeyword(state, keywordUpper) {
  const tok = current(state)
  if (tok.type !== 'keyword' || tok.value !== keywordUpper) {
    throw new Error('Expected keyword ' + keywordUpper + ' at position ' + tok.position)
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
    throw new Error('Expected identifier at position ' + tok.position)
  }
  consume(state)
  return tok
}

/**
 * Creates an ExprCursor adapter for the ParserState.
 * @param {ParserState} state
 * @returns {import('./types.js').ExprCursor}
 */
function createExprCursor(state) {
  return {
    current: () => current(state),
    peek: (offset) => peekToken(state, offset),
    consume: () => consume(state),
    match: (type, value) => match(state, type, value),
    matchKeyword: (keywordUpper) => matchKeyword(state, keywordUpper),
    expect: (type, value) => expect(state, type, value),
    expectKeyword: (keywordUpper) => expectKeyword(state, keywordUpper),
    expectIdentifier: () => expectIdentifier(state),
  }
}

// --- parsing ---

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
    const col = parseSelectItem(state)
    cols.push(col)
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
  if (tok.type !== 'identifier') {
    throw new Error('Expected column name or function at position ' + tok.position)
  }

  const next = peekToken(state, 1)
  const upper = tok.value.toUpperCase()

  if (next.type === 'paren' && next.value === '(') {
    if (AGG_FUNCS.has(/** @type {AggregateFunc} */ (upper))) {
      return parseAggregateItem(state)
    }
    if (STRING_FUNCS.has(upper)) {
      return parseStringFunctionItem(state)
    }
  }

  consume(state)
  let columnName = tok.value

  // Handle dot notation (table.column)
  if (current(state).type === 'dot') {
    consume(state) // consume the dot
    const columnTok = expectIdentifier(state)
    columnName = columnName + '.' + columnTok.value
  }

  /** @type {string | undefined} */
  let alias
  if (matchKeyword(state, 'AS')) {
    // After AS, allow keywords as aliases (except reserved ones)
    const aliasTok = current(state)
    if (aliasTok.type === 'identifier') {
      consume(state)
      alias = aliasTok.value
    } else if (aliasTok.type === 'keyword' && !RESERVED_AFTER_COLUMN.has(aliasTok.value.toUpperCase())) {
      consume(state)
      // Use original case for keywords used as aliases
      alias = aliasTok.originalValue || aliasTok.value
    } else {
      throw new Error('Expected alias after AS at position ' + aliasTok.position)
    }
  } else {
    const maybeAlias = current(state)
    if (
      maybeAlias.type === 'identifier' &&
      !RESERVED_AFTER_COLUMN.has(maybeAlias.value.toUpperCase())
    ) {
      consume(state)
      alias = maybeAlias.value
    }
  }

  return {
    kind: 'column',
    column: columnName,
    alias,
  }
}

/**
 * @param {ParserState} state
 * @returns {AggregateColumn}
 */
function parseAggregateItem(state) {
  const funcTok = expectIdentifier(state)
  const funcUpper = /** @type {AggregateFunc} */ (funcTok.value.toUpperCase())
  expect(state, 'paren', '(')

  /** @type {AggregateArg} */
  let arg

  const cur = current(state)
  if (cur.type === 'operator' && cur.value === '*') {
    consume(state)
    arg = { kind: 'star' }
  } else {
    const colTok = expectIdentifier(state)
    arg = {
      kind: 'column',
      column: colTok.value,
    }
  }

  expect(state, 'paren', ')')

  /** @type {string | undefined} */
  let alias
  if (matchKeyword(state, 'AS')) {
    // After AS, allow keywords as aliases (except reserved ones)
    const aliasTok = current(state)
    if (aliasTok.type === 'identifier') {
      consume(state)
      alias = aliasTok.value
    } else if (aliasTok.type === 'keyword' && !RESERVED_AFTER_COLUMN.has(aliasTok.value.toUpperCase())) {
      consume(state)
      // Use original case for keywords used as aliases
      alias = aliasTok.originalValue || aliasTok.value
    } else {
      throw new Error('Expected alias after AS at position ' + aliasTok.position)
    }
  } else {
    const maybeAlias = current(state)
    if (
      maybeAlias.type === 'identifier' &&
      !RESERVED_AFTER_COLUMN.has(maybeAlias.value.toUpperCase())
    ) {
      consume(state)
      alias = maybeAlias.value
    }
  }

  return {
    kind: 'aggregate',
    func: funcUpper,
    arg,
    alias,
  }
}

/**
 * @param {ParserState} state
 * @returns {SelectColumn}
 */
function parseStringFunctionItem(state) {
  const funcTok = expectIdentifier(state)
  const funcUpper = funcTok.value.toUpperCase()
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

  /** @type {string | undefined} */
  let alias
  if (matchKeyword(state, 'AS')) {
    // After AS, allow keywords as aliases (except reserved ones)
    const aliasTok = current(state)
    if (aliasTok.type === 'identifier') {
      consume(state)
      alias = aliasTok.value
    } else if (aliasTok.type === 'keyword' && !RESERVED_AFTER_COLUMN.has(aliasTok.value.toUpperCase())) {
      consume(state)
      // Use original case for keywords used as aliases
      alias = aliasTok.originalValue || aliasTok.value
    } else {
      throw new Error('Expected alias after AS at position ' + aliasTok.position)
    }
  } else {
    const maybeAlias = current(state)
    if (
      maybeAlias.type === 'identifier' &&
      !RESERVED_AFTER_COLUMN.has(maybeAlias.value.toUpperCase())
    ) {
      consume(state)
      alias = maybeAlias.value
    }
  }

  return {
    kind: 'function',
    func: funcUpper,
    args,
    alias,
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
        if (matchKeyword(state, 'OUTER')) {
          // LEFT OUTER JOIN
        }
        joinType = 'LEFT'
      } else if (tok.value === 'RIGHT') {
        consume(state)
        if (matchKeyword(state, 'OUTER')) {
          // RIGHT OUTER JOIN
        }
        joinType = 'RIGHT'
      } else if (tok.value === 'FULL') {
        consume(state)
        if (matchKeyword(state, 'OUTER')) {
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
        expectKeyword(state, 'JOIN')
      }
    } else {
      // No more joins
      break
    }

    // Parse table name
    const tableTok = expectIdentifier(state)
    const tableName = tableTok.value

    // Parse ON condition
    expectKeyword(state, 'ON')
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
 * @param {ParserState} state
 * @returns {SelectAst}
 */
function parseSelectInternal(state) {
  expectKeyword(state, 'SELECT')

  let distinct = false
  if (matchKeyword(state, 'DISTINCT')) {
    distinct = true
  }

  const columns = parseSelectList(state)

  expectKeyword(state, 'FROM')
  const fromTok = expectIdentifier(state)
  const fromName = fromTok.value

  // Parse JOIN clauses
  const joins = parseJoins(state)

  /** @type {ExprNode | undefined} */
  let where
  /** @type {ExprNode[]} */
  const groupBy = []
  /** @type {OrderByItem[]} */
  const orderBy = []
  /** @type {number | undefined} */
  let limit
  /** @type {number | undefined} */
  let offset

  const cursor = createExprCursor(state)

  if (matchKeyword(state, 'WHERE')) {
    where = parseExpression(cursor)
  }

  if (matchKeyword(state, 'GROUP')) {
    expectKeyword(state, 'BY')
    while (true) {
      const expr = parseExpression(cursor)
      groupBy.push(expr)
      if (!match(state, 'comma')) break
    }
  }

  if (matchKeyword(state, 'ORDER')) {
    expectKeyword(state, 'BY')
    while (true) {
      const expr = parseExpression(cursor)
      /** @type {'ASC' | 'DESC'} */
      let direction = 'ASC'
      if (matchKeyword(state, 'ASC')) {
        direction = 'ASC'
      } else if (matchKeyword(state, 'DESC')) {
        direction = 'DESC'
      }
      orderBy.push({
        expr,
        direction,
      })
      if (!match(state, 'comma')) break
    }
  }

  if (matchKeyword(state, 'LIMIT')) {
    const tok = current(state)
    if (tok.type !== 'number') {
      throw new Error('Expected numeric LIMIT at position ' + tok.position)
    }
    consume(state)
    const n = parseInt(tok.value, 10)
    if (!Number.isFinite(n)) {
      throw new Error('Invalid LIMIT value at position ' + tok.position)
    }
    limit = n

    if (matchKeyword(state, 'OFFSET')) {
      const oTok = current(state)
      if (oTok.type !== 'number') {
        throw new Error('Expected numeric OFFSET at position ' + oTok.position)
      }
      consume(state)
      const off = parseInt(oTok.value, 10)
      if (!Number.isFinite(off)) {
        throw new Error('Invalid OFFSET value at position ' + oTok.position)
      }
      offset = off
    }
  } else if (matchKeyword(state, 'OFFSET')) {
    const oTok = current(state)
    if (oTok.type !== 'number') {
      throw new Error('Expected numeric OFFSET at position ' + oTok.position)
    }
    consume(state)
    const off = parseInt(oTok.value, 10)
    if (!Number.isFinite(off)) {
      throw new Error('Invalid OFFSET value at position ' + oTok.position)
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
    from: fromName,
    joins,
    where,
    groupBy,
    orderBy,
    limit,
    offset,
  }
}

/**
 * @param {string} sql
 * @returns {SelectAst}
 */
export function parseSql(sql) {
  const tokens = tokenize(sql)
  /** @type {ParserState} */
  const state = { tokens, pos: 0 }
  const ast = parseSelectInternal(state)

  const tok = current(state)
  if (tok.type !== 'eof') {
    throw new Error('Unexpected tokens after end of query at position ' + tok.position)
  }

  return ast
}
