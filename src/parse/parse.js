import { expectNoAggregate, findAggregate } from '../validation/aggregates.js'
import { RESERVED_AFTER_COLUMN, RESERVED_AFTER_TABLE } from '../validation/keywords.js'
import { ParseError } from '../validation/parseErrors.js'
import { parseExpression } from './expression.js'
import { parseJoins } from './joins.js'
import { consume, current, expect, match, parseError, peekToken } from './state.js'
import { tokenizeSql } from './tokenize.js'

/**
 * @import { CTEDefinition, ExprNode, FromSubquery, FromTable, OrderByItem, ParseSqlOptions, ParserState, SelectColumn, SelectStatement, SetOperationStatement, SetOperator, Statement } from '../types.js'
 */

const MAX_PARSE_CACHE = 64
/** @type {Map<string, Statement>} */
const parseCache = new Map()

/**
 * @param {ParseSqlOptions} options
 * @returns {Statement}
 */
export function parseSql({ query, functions }) {
  // Cache only for simple queries without custom functions
  if (!functions) {
    const cached = parseCache.get(query)
    if (cached) {
      // LRU touch
      parseCache.delete(query)
      parseCache.set(query, cached)
      return cached
    }
  }

  const tokens = tokenizeSql(query)
  /** @type {ParserState} */
  const state = { tokens, pos: 0, lastPos: 0, functions }

  // Parse optional WITH clause
  const stmt = parseStatement(state)

  const tok = current(state)
  if (tok.type !== 'eof') {
    throw parseError(state, 'end of query')
  }

  if (!functions) {
    parseCache.set(query, stmt)
    if (parseCache.size > MAX_PARSE_CACHE) {
      const oldest = parseCache.keys().next().value
      if (oldest) parseCache.delete(oldest)
    }
  }

  return stmt
}

/**
 * Parses a WITH clause containing one or more CTEs, or a SELECT with optional set operations.
 *
 * @param {ParserState} state
 * @returns {Statement}
 */
export function parseStatement(state) {
  const positionStart = state.lastPos
  if (match(state, 'keyword', 'WITH')) {
    /** @type {CTEDefinition[]} */
    const ctes = []
    /** @type {Set<string>} */
    const seenNames = new Set()

    while (true) {
      // Parse CTE name
      const nameTok = expect(state, 'identifier')
      const name = nameTok.value
      const nameLower = name.toLowerCase()

      // Check for duplicate CTE names
      if (seenNames.has(nameLower)) {
        throw new ParseError({
          message: `CTE "${name}" is defined more than once at position ${positionStart}`,
          ...nameTok,
        })
      }
      seenNames.add(nameLower)

      // Expect AS statement
      expect(state, 'keyword', 'AS')
      expect(state, 'paren', '(')

      // Parse the CTE's SELECT statement
      const query = parseStatement(state)

      expect(state, 'paren', ')')

      ctes.push({ name, query, positionStart: nameTok.positionStart, positionEnd: state.lastPos })

      // Check for comma (more CTEs) or end of WITH clause
      if (!match(state, 'comma')) break
    }

    const query = parseSetOperations(state)

    return { type: 'with', ctes, query, positionStart, positionEnd: state.lastPos }
  } else {
    return parseSetOperations(state)
  }
}

/**
 * Checks for and parses UNION/INTERSECT/EXCEPT set operations after a SELECT.
 * Handles chaining (e.g., SELECT ... UNION SELECT ... EXCEPT SELECT ...).
 * ORDER BY and LIMIT/OFFSET on the last segment apply to the entire compound result.
 *
 * @param {ParserState} state
 * @returns {SelectStatement | SetOperationStatement}
 */
function parseSetOperations(state) {
  let left = parseIntersectOperations(state)

  while (true) {
    /** @type {SetOperator | undefined} */
    let operator
    if (match(state, 'keyword', 'UNION')) {
      operator = 'UNION'
    } else if (match(state, 'keyword', 'EXCEPT')) {
      operator = 'EXCEPT'
    }
    if (!operator) return left

    const all = !!match(state, 'keyword', 'ALL')
    const right = parseIntersectOperations(state)

    // ORDER BY / LIMIT / OFFSET after a set operation apply to the compound result.
    // If the right SELECT parsed them, lift them to the compound statement.
    left = {
      type: 'compound',
      operator,
      all,
      left,
      right,
      orderBy: right.orderBy,
      limit: right.limit,
      offset: right.offset,
      positionStart: left.positionStart,
      positionEnd: right.positionEnd,
    }

    // Clear lifted clauses from the right SELECT
    right.orderBy = []
    right.limit = undefined
    right.offset = undefined
  }
}

/**
 * Parses a left-associative INTERSECT chain, which binds tighter than UNION/EXCEPT.
 *
 * @param {ParserState} state
 * @returns {SelectStatement | SetOperationStatement}
 */
function parseIntersectOperations(state) {
  /** @type {SelectStatement | SetOperationStatement} */
  let left = parseSelect(state)

  while (match(state, 'keyword', 'INTERSECT')) {
    const all = !!match(state, 'keyword', 'ALL')
    const right = parseSelect(state)

    left = {
      type: 'compound',
      operator: 'INTERSECT',
      all,
      left,
      right,
      orderBy: right.orderBy,
      limit: right.limit,
      offset: right.offset,
      positionStart: left.positionStart,
      positionEnd: right.positionEnd,
    }

    right.orderBy = []
    right.limit = undefined
    right.offset = undefined
  }

  return left
}

/**
 * @param {ParserState} state
 * @returns {SelectStatement}
 */
function parseSelect(state) {
  const { positionStart } = current(state)
  /** @type {SelectColumn[]} */
  let columns
  let distinct = false

  // Support duckdb-style shorthand "FROM table"
  if (match(state, 'keyword', 'FROM')) {
    columns = [{ type: 'star', positionStart, positionEnd: positionStart }]
  } else {
    expect(state, 'keyword', 'SELECT')
    distinct = match(state, 'keyword', 'DISTINCT')
    columns = parseSelectList(state)
    expect(state, 'keyword', 'FROM')
  }

  // Check if it's a subquery or table name
  /** @type {FromTable | FromSubquery} */
  let from
  const fromTok = current(state)
  if (fromTok.type === 'paren' && fromTok.value === '(') {
    // Subquery: SELECT * FROM (SELECT ...) AS alias
    expect(state, 'paren', '(')
    const query = parseStatement(state)
    expect(state, 'paren', ')')
    const alias = parseTableAlias(state)
    from = {
      type: 'subquery',
      query,
      alias,
      positionStart: fromTok.positionStart,
      positionEnd: state.lastPos,
    }
  } else {
    // Simple table name: SELECT * FROM users
    expect(state, 'identifier')
    const alias = parseTableAlias(state)
    from = {
      type: 'table',
      table: fromTok.value,
      alias,
      positionStart: fromTok.positionStart,
      positionEnd: state.lastPos,
    }
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
    expectNoAggregate(where, 'WHERE')
  }

  if (match(state, 'keyword', 'GROUP')) {
    expect(state, 'keyword', 'BY')
    while (true) {
      const expr = parseExpression(state)
      expectNoAggregate(expr, 'GROUP BY')
      groupBy.push(expr)
      if (!match(state, 'comma')) break
    }
  }

  if (match(state, 'keyword', 'HAVING')) {
    having = parseExpression(state)
  }

  const hasAggregate = groupBy.length > 0 || columns.some(col =>
    col.type === 'derived' && findAggregate(col.expr)
  )

  if (match(state, 'keyword', 'ORDER')) {
    expect(state, 'keyword', 'BY')
    while (true) {
      const expr = parseExpression(state)
      if (!hasAggregate) {
        expectNoAggregate(expr, 'ORDER BY')
      }
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
        const tok = consume(state)
        const upper = tok.value.toUpperCase()
        if (tok.type === 'identifier' && upper === 'FIRST') {
          nulls = 'FIRST'
        } else if (tok.type === 'identifier' && upper === 'LAST') {
          nulls = 'LAST'
        } else {
          throw parseError(state, 'FIRST or LAST after NULLS')
        }
      }
      orderBy.push({
        expr,
        direction,
        nulls,
        positionStart,
        positionEnd: state.lastPos,

      })
      if (!match(state, 'comma')) break
    }
  }

  if (match(state, 'keyword', 'LIMIT')) {
    const tok = consume(state)
    if (tok.type !== 'number' || typeof tok.numericValue !== 'number') {
      throw parseError(state, 'positive integer LIMIT')
    }
    if (!Number.isInteger(tok.numericValue) || tok.numericValue < 0) {
      throw parseError(state, 'positive integer LIMIT value')
    }
    limit = tok.numericValue
  }

  if (match(state, 'keyword', 'OFFSET')) {
    const tok = consume(state)
    if (tok.type !== 'number' || typeof tok.numericValue !== 'number') {
      throw parseError(state, 'positive integer OFFSET value')
    }
    if (!Number.isInteger(tok.numericValue) || tok.numericValue < 0) {
      throw parseError(state, 'positive integer OFFSET value')
    }
    offset = tok.numericValue
  }

  // optional trailing semicolon
  match(state, 'semicolon')

  return {
    type: 'select',
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
    positionStart,
    positionEnd: state.lastPos,
  }
}

/**
 * @param {ParserState} state
 * @returns {SelectColumn[]}
 */
function parseSelectList(state) {
  /** @type {SelectColumn[]} */
  const cols = []

  while (true) {
    const { positionStart, type } = current(state)

    // Check for qualified asterisk (table.*)
    if (type === 'identifier') {
      const next = peekToken(state, 1)
      const nextNext = peekToken(state, 2)
      if (next.type === 'dot' && nextNext.type === 'operator' && nextNext.value === '*') {
        const table = consume(state).value
        consume(state) // consume dot
        consume(state) // consume asterisk
        cols.push({ type: 'star', table, positionStart, positionEnd: state.lastPos })
        if (!match(state, 'comma')) break
        continue
      }
    }

    // Check for unqualified asterisk (*)
    if (match(state, 'operator', '*')) {
      cols.push({ type: 'star', positionStart, positionEnd: state.lastPos })
      if (!match(state, 'comma')) break
      continue
    }

    // Parse derived column with optional alias
    const expr = parseExpression(state)
    const alias = parseAs(state)
    cols.push({ type: 'derived', expr, alias, positionStart, positionEnd: state.lastPos })

    if (!match(state, 'comma')) break
  }

  return cols
}

/**
 * Parses an optional table alias (e.g., "FROM users u" or "FROM users AS u")
 * @param {ParserState} state
 * @returns {string | undefined}
 */
export function parseTableAlias(state) {
  // Check for explicit AS keyword
  if (match(state, 'keyword', 'AS')) {
    const aliasTok = expect(state, 'identifier')
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
    } else if (aliasTok.type === 'keyword' && !RESERVED_AFTER_COLUMN.has(aliasTok.value)) {
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
