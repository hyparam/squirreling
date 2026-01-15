import { parseExpression } from './expression.js'
import { tokenizeSql } from './tokenize.js'
import { consume, current, expect, expectIdentifier, match, parseError, peekToken } from './state.js'
import { parseJoins } from './joins.js'
import { duplicateCTEError } from '../parseErrors.js'
import { RESERVED_AFTER_COLUMN, RESERVED_AFTER_TABLE, isKnownFunction } from '../validation.js'

/**
 * @import { CTEDefinition, ExprNode, FromSubquery, FromTable, OrderByItem, ParseSqlOptions, ParserState, SelectStatement, SelectColumn, WithClause } from '../types.js'
 */

/**
 * Parses a WITH clause containing one or more CTEs
 * @param {ParserState} state
 * @returns {WithClause}
 */
function parseWithClause(state) {
  /** @type {CTEDefinition[]} */
  const ctes = []
  /** @type {Set<string>} */
  const seenNames = new Set()

  while (true) {
    // Parse CTE name
    const nameTok = expectIdentifier(state)
    const name = nameTok.value
    const nameLower = name.toLowerCase()

    // Check for duplicate CTE names
    if (seenNames.has(nameLower)) {
      throw duplicateCTEError({
        cteName: name,
        positionStart: nameTok.positionStart,
        positionEnd: nameTok.positionEnd,
      })
    }
    seenNames.add(nameLower)

    // Expect AS keyword
    expect(state, 'keyword', 'AS')

    // Expect opening parenthesis
    expect(state, 'paren', '(')

    // Parse the CTE's SELECT statement
    const query = parseSelectInternal(state)

    // Expect closing parenthesis
    expect(state, 'paren', ')')

    ctes.push({ name, query })

    // Check for comma (more CTEs) or end of WITH clause
    if (!match(state, 'comma')) {
      break
    }
  }

  return { ctes }
}

/**
 * @param {ParseSqlOptions} options
 * @returns {SelectStatement}
 */
export function parseSql({ query, functions }) {
  const tokens = tokenizeSql(query)
  /** @type {ParserState} */
  const state = { tokens, pos: 0, functions }

  // Check for WITH clause
  /** @type {WithClause | undefined} */
  let withClause
  if (match(state, 'keyword', 'WITH')) {
    withClause = parseWithClause(state)
  }

  const select = parseSelectInternal(state)

  // Attach WITH clause to the select statement
  if (withClause) {
    select.with = withClause
  }

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

  while (true) {
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
        if (!match(state, 'comma')) break
        continue
      }
    }

    // Check for unqualified asterisk (*)
    if (tok.type === 'operator' && tok.value === '*') {
      consume(state)
      cols.push({ kind: 'star' })
      if (!match(state, 'comma')) break
      continue
    }

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

  // Check if keyword followed by ( is a known function (e.g., LEFT, RIGHT)
  const isKeywordFunction = tok.type === 'keyword' &&
    peekToken(state, 1).type === 'paren' &&
    peekToken(state, 1).value === '(' &&
    isKnownFunction(tok.value, state.functions)

  if (tok.type === 'keyword' && !EXPRESSION_START_KEYWORDS.has(tok.value) && !isKeywordFunction || tok.type === 'eof') {
    throw parseError(state, 'column name or expression')
  }

  // Delegate to expression parser (handles all expressions including aggregates)
  const expr = parseExpression(state)
  const alias = parseAs(state)
  return { kind: 'derived', expr, alias }
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
  const alias = parseTableAlias(state)
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
