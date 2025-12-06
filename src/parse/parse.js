import { tokenize } from './tokenize.js'
import { parseExpression } from './expression.js'
import { RESERVED_AFTER_COLUMN, RESERVED_AFTER_TABLE, isAggregateFunc } from '../validation.js'
import { consume, current, expect, expectIdentifier, match, parseError, peekToken } from './state.js'
import { parseJoins } from './joins.js'

/**
 * @import { AggregateColumn, AggregateArg, AggregateFunc, ExprNode, FromSubquery, FromTable, OrderByItem, ParserState, SelectStatement, SelectColumn } from '../types.js'
 */

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
 * @returns {SelectColumn[]}
 */
function parseSelectList(state) {
  /** @type {SelectColumn[]} */
  const cols = []
  const tok = current(state)

  // Check for qualified asterisk (table.*)
  if (tok.type === 'identifier') {
    const next = peekToken(state, 1)
    const nextNext = peekToken(state, 2)
    if (next.type === 'dot' && nextNext.type === 'operator' && nextNext.value === '*') {
      const tableTok = consume(state) // consume table name
      consume(state) // consume dot
      consume(state) // consume asterisk
      cols.push({ kind: 'star', table: tableTok.value })
      return cols
    }
  }

  // Check for unqualified asterisk (*)
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

// Keywords that can start a valid expression in SELECT
const EXPRESSION_START_KEYWORDS = new Set([
  'CASE', 'TRUE', 'FALSE', 'NULL', 'EXISTS', 'NOT', 'INTERVAL',
])

/**
 * @param {ParserState} state
 * @returns {SelectColumn}
 */
function parseSelectItem(state) {
  const tok = current(state)

  if (tok.type === 'keyword' && !EXPRESSION_START_KEYWORDS.has(tok.value) || tok.type === 'eof') {
    throw parseError(state, 'column name or expression')
  }

  const next = peekToken(state, 1)
  if (next.type === 'paren' && next.value === '(') {
    const upper = tok.value.toUpperCase()
    if (isAggregateFunc(upper)) {
      expectIdentifier(state) // consume function name
      return parseAggregateItem(state, upper)
    }
  }

  // Delegate to expression parser
  const expr = parseExpression(state)
  const alias = parseAs(state)
  return { kind: 'derived', expr, alias }
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
  } else {
    /** @type {'all' | 'distinct'} */
    let quantifier = 'all'
    if (cur.type === 'keyword' && cur.value === 'ALL') {
      consume(state) // consume ALL
    } else if (cur.type === 'keyword' && cur.value === 'DISTINCT') {
      consume(state)
      quantifier = 'distinct'
    }

    const expr = parseExpression(state)
    arg = {
      kind: 'expression',
      expr,
      quantifier,
    }
  }

  expect(state, 'paren', ')')

  const alias = parseAs(state)

  return { kind: 'aggregate', func, arg, alias }
}

/**
 * Parses an optional table alias (e.g., "FROM users u" or "FROM users AS u")
 * @param {ParserState} state
 * @returns {string | undefined}
 */
export function parseTableAlias(state) {
  // Check for explicit AS keyword
  if (match(state, 'keyword', 'AS')) {
    const aliasTok = expectIdentifier(state)
    return aliasTok.value
  }
  // Check for implicit alias (identifier not in reserved list)
  const maybeAlias = current(state)
  if (maybeAlias.type === 'identifier' && !RESERVED_AFTER_TABLE.has(maybeAlias.value.toUpperCase())) {
    consume(state)
    return maybeAlias.value
  }
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
 * Parses a subquery in parentheses with an alias
 * @param {ParserState} state
 * @returns {FromSubquery}
 */
function parseFromSubquery(state) {
  expect(state, 'paren', '(')
  const query = parseSelectInternal(state)
  expect(state, 'paren', ')')
  expect(state, 'keyword', 'AS')
  const alias = expectIdentifier(state).value
  return { kind: 'subquery', query, alias }
}

/**
 * @param {ParserState} state
 * @returns {SelectStatement}
 */
export function parseSelectInternal(state) {
  expect(state, 'keyword', 'SELECT')

  let distinct = false
  if (match(state, 'keyword', 'DISTINCT')) {
    distinct = true
  }

  const columns = parseSelectList(state)

  expect(state, 'keyword', 'FROM')

  // Check if it's a subquery or table name
  /** @type {FromTable | FromSubquery} */
  let from
  const tok = current(state)
  if (tok.type === 'paren' && tok.value === '(') {
    // Subquery: SELECT * FROM (SELECT ...) AS alias
    from = parseFromSubquery(state)
  } else {
    // Simple table name: SELECT * FROM users
    const table = expectIdentifier(state).value
    const alias = parseTableAlias(state)
    from = { kind: 'table', table, alias }
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

  if (match(state, 'keyword', 'WHERE')) {
    where = parseExpression(state)
  }

  if (match(state, 'keyword', 'GROUP')) {
    expect(state, 'keyword', 'BY')
    while (true) {
      const expr = parseExpression(state)
      groupBy.push(expr)
      if (!match(state, 'comma')) break
    }
  }

  if (match(state, 'keyword', 'HAVING')) {
    having = parseExpression(state)
  }

  if (match(state, 'keyword', 'ORDER')) {
    expect(state, 'keyword', 'BY')
    while (true) {
      const expr = parseExpression(state)
      /** @type {'ASC' | 'DESC'} */
      let direction = 'ASC'
      if (match(state, 'keyword', 'ASC')) {
        direction = 'ASC'
      } else if (match(state, 'keyword', 'DESC')) {
        direction = 'DESC'
      }
      /** @type {'FIRST' | 'LAST' | undefined} */
      let nulls
      if (match(state, 'keyword', 'NULLS')) {
        const tok = current(state)
        if (tok.type === 'identifier' && tok.value.toUpperCase() === 'FIRST') {
          consume(state)
          nulls = 'FIRST'
        } else if (tok.type === 'identifier' && tok.value.toUpperCase() === 'LAST') {
          consume(state)
          nulls = 'LAST'
        } else {
          throw parseError(state, 'FIRST or LAST after NULLS')
        }
      }
      orderBy.push({
        expr,
        direction,
        nulls,
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
