/**
 * @import { SelectAst, SelectColumn, AggregateColumn, AggregateArg, AggregateFunc, ExprNode, BinaryOp, OrderByItem, JoinClause, JoinType } from './types.d.ts'
 */

const KEYWORDS = new Set([
  'SELECT',
  'FROM',
  'WHERE',
  'AND',
  'OR',
  'NOT',
  'IS',
  'GROUP',
  'BY',
  'HAVING',
  'ORDER',
  'ASC',
  'DESC',
  'LIMIT',
  'OFFSET',
  'AS',
  'DISTINCT',
  'TRUE',
  'FALSE',
  'NULL',
  'LIKE',
  'IN',
  'BETWEEN',
  'CASE',
  'WHEN',
  'THEN',
  'ELSE',
  'END',
  'JOIN',
  'INNER',
  'LEFT',
  'RIGHT',
  'FULL',
  'OUTER',
  'ON',
])

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
 * @param {string} ch
 * @returns {boolean}
 */
function isWhitespace(ch) {
  return ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r'
}

/**
 * @param {string} ch
 * @returns {boolean}
 */
function isDigit(ch) {
  return ch >= '0' && ch <= '9'
}

/**
 * @param {string} ch
 * @returns {boolean}
 */
function isAlpha(ch) {
  return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch === '_' || ch === '$'
}

/**
 * @param {string} ch
 * @returns {boolean}
 */
function isAlphaNumeric(ch) {
  return isAlpha(ch) || isDigit(ch)
}

/**
 * @param {string} sql
 * @returns {import('./types.js').Token[]}
 */
export function tokenize(sql) {
  /** @type {import('./types.js').Token[]} */
  const tokens = []
  const { length } = sql
  let i = 0

  /**
   * @returns {string}
   */
  function peek() {
    if (i >= length) return ''
    return sql[i]
  }

  /**
   * @returns {string}
   */
  function nextChar() {
    if (i >= length) return ''
    const ch = sql[i]
    i += 1
    return ch
  }

  while (i < length) {
    const ch = peek()

    if (isWhitespace(ch)) {
      nextChar()
      continue
    }

    // line comment --
    if (ch === '-' && i + 1 < length && sql[i + 1] === '-') {
      while (i < length && sql[i] !== '\n') {
        i += 1
      }
      continue
    }

    // block comment /* ... */
    if (ch === '/' && i + 1 < length && sql[i + 1] === '*') {
      i += 2
      while (i < length) {
        if (sql[i] === '*' && i + 1 < length && sql[i + 1] === '/') {
          i += 2
          break
        }
        i += 1
      }
      continue
    }

    const pos = i

    // numbers
    if (isDigit(ch)) {
      let text = ''
      while (isDigit(peek())) {
        text += nextChar()
      }
      if (peek() === '.') {
        text += nextChar()
        while (isDigit(peek())) {
          text += nextChar()
        }
      }
      if (peek() === 'e' || peek() === 'E') {
        text += nextChar()
        if (peek() === '+' || peek() === '-') {
          text += nextChar()
        }
        while (isDigit(peek())) {
          text += nextChar()
        }
      }
      const num = parseFloat(text)
      tokens.push({
        type: 'number',
        value: text,
        position: pos,
        numericValue: num,
      })
      continue
    }

    // identifiers / keywords
    if (isAlpha(ch)) {
      let text = ''
      while (isAlphaNumeric(peek())) {
        text += nextChar()
      }
      const upper = text.toUpperCase()
      if (KEYWORDS.has(upper)) {
        tokens.push({
          type: 'keyword',
          value: upper,
          originalValue: text,
          position: pos,
        })
      } else {
        tokens.push({
          type: 'identifier',
          value: text,
          position: pos,
        })
      }
      continue
    }

    // string literals: single or double quotes
    if (ch === '\'' || ch === '"') {
      const quote = nextChar()
      let text = ''
      while (i < length) {
        const c = nextChar()
        if (c === quote) {
          if (peek() === quote) {
            text += quote
            nextChar()
            continue
          }
          break
        }
        if (c === '\\' && i < length) {
          const esc = nextChar()
          text += esc
        } else {
          text += c
        }
      }
      tokens.push({
        type: 'string',
        value: text,
        position: pos,
      })
      continue
    }

    // two-character operators
    if (ch === '<' || ch === '>' || ch === '!' || ch === '=') {
      let op = nextChar()
      if ((op === '<' || op === '>' || op === '!') && peek() === '=') {
        op += nextChar()
      } else if (op === '<' && peek() === '>') {
        op += nextChar()
      }
      tokens.push({
        type: 'operator',
        value: op,
        position: pos,
      })
      continue
    }

    // single-char operators
    if (ch === '*' || ch === '+' || ch === '-' || ch === '/' || ch === '%') {
      const op = nextChar()
      tokens.push({
        type: 'operator',
        value: op,
        position: pos,
      })
      continue
    }

    if (ch === ',') {
      nextChar()
      tokens.push({
        type: 'comma',
        value: ',',
        position: pos,
      })
      continue
    }

    if (ch === '.') {
      nextChar()
      tokens.push({
        type: 'dot',
        value: '.',
        position: pos,
      })
      continue
    }

    if (ch === '(' || ch === ')') {
      const p = nextChar()
      tokens.push({
        type: 'paren',
        value: p,
        position: pos,
      })
      continue
    }

    if (ch === ';') {
      nextChar()
      tokens.push({
        type: 'semicolon',
        value: ';',
        position: pos,
      })
      continue
    }

    throw new Error('Unexpected character at position ' + pos + ': ' + ch)
  }

  tokens.push({
    type: 'eof',
    value: '',
    position: length,
  })

  return tokens
}

/**
 * @param {import('./types.js').ParserState} state
 * @returns {import('./types.js').Token}
 */
function current(state) {
  return state.tokens[state.pos]
}

/**
 * @param {import('./types.js').ParserState} state
 * @param {number} [offset=0]
 * @returns {import('./types.js').Token}
 */
function peekToken(state, offset = 0) {
  const idx = state.pos + offset
  if (idx >= state.tokens.length) {
    return state.tokens[state.tokens.length - 1]
  }
  return state.tokens[idx]
}

/**
 * @param {import('./types.js').ParserState} state
 * @returns {import('./types.js').Token}
 */
function consume(state) {
  const tok = current(state)
  if (state.pos < state.tokens.length - 1) {
    state.pos += 1
  }
  return tok
}

/**
 * @param {import('./types.js').ParserState} state
 * @param {import('./types.js').TokenType} type
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
 * @param {import('./types.js').ParserState} state
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
 * @param {import('./types.js').ParserState} state
 * @param {import('./types.js').TokenType} type
 * @param {string} [value]
 * @returns {import('./types.js').Token}
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
 * @param {import('./types.js').ParserState} state
 * @param {string} keywordUpper
 * @returns {import('./types.js').Token}
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
 * @param {import('./types.js').ParserState} state
 * @returns {import('./types.js').Token}
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

// --- parsing ---

/**
 * @param {import('./types.js').ParserState} state
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
 * @param {import('./types.js').ParserState} state
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
 * @param {import('./types.js').ParserState} state
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
 * @param {import('./types.js').ParserState} state
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
    while (true) {
      const arg = parsePrimary(state)
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
 * @param {import('./types.js').ParserState} state
 * @returns {ExprNode}
 */
function parseExpression(state) {
  return parseOr(state)
}

/**
 * @param {import('./types.js').ParserState} state
 * @returns {ExprNode}
 */
function parseOr(state) {
  let node = parseAnd(state)
  while (matchKeyword(state, 'OR')) {
    const right = parseAnd(state)
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
 * @param {import('./types.js').ParserState} state
 * @returns {ExprNode}
 */
function parseAnd(state) {
  let node = parseNot(state)
  while (matchKeyword(state, 'AND')) {
    const right = parseNot(state)
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
 * @param {import('./types.js').ParserState} state
 * @returns {ExprNode}
 */
function parseNot(state) {
  if (matchKeyword(state, 'NOT')) {
    const argument = parseNot(state)
    return {
      type: 'unary',
      op: 'NOT',
      argument,
    }
  }
  return parseComparison(state)
}

/**
 * @param {import('./types.js').ParserState} state
 * @returns {ExprNode}
 */
function parseComparison(state) {
  const left = parsePrimary(state)
  const tok = current(state)

  // Handle IS NULL and IS NOT NULL
  if (tok.type === 'keyword' && tok.value === 'IS') {
    consume(state)
    const notToken = current(state)
    if (notToken.type === 'keyword' && notToken.value === 'NOT') {
      consume(state)
      expectKeyword(state, 'NULL')
      return {
        type: 'unary',
        op: 'IS NOT NULL',
        argument: left,
      }
    }
    expectKeyword(state, 'NULL')
    return {
      type: 'unary',
      op: 'IS NULL',
      argument: left,
    }
  }

  // Handle LIKE
  if (tok.type === 'keyword' && tok.value === 'LIKE') {
    consume(state)
    const right = parsePrimary(state)
    return {
      type: 'binary',
      op: 'LIKE',
      left,
      right,
    }
  }

  if (tok.type === 'operator' && isComparisonOperator(tok.value)) {
    consume(state)
    const right = parsePrimary(state)
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
 * @param {import('./types.js').ParserState} state
 * @returns {ExprNode}
 */
function parsePrimary(state) {
  const tok = current(state)

  if (tok.type === 'paren' && tok.value === '(') {
    consume(state)
    const expr = parseExpression(state)
    expect(state, 'paren', ')')
    return expr
  }

  if (tok.type === 'identifier') {
    const next = peekToken(state, 1)

    // Check if this is a function call
    if (next.type === 'paren' && next.value === '(') {
      const funcName = tok.value
      consume(state) // consume function name
      consume(state) // consume '('

      /** @type {ExprNode[]} */
      const args = []

      // Parse comma-separated arguments
      if (current(state).type !== 'paren' || current(state).value !== ')') {
        while (true) {
          const arg = parseExpression(state)
          args.push(arg)
          if (!match(state, 'comma')) break
        }
      }

      expect(state, 'paren', ')')

      return {
        type: 'function',
        name: funcName,
        args,
      }
    }

    consume(state)
    let name = tok.value

    // Handle dot notation (table.column)
    if (current(state).type === 'dot') {
      consume(state) // consume the dot
      const columnTok = expectIdentifier(state)
      name = name + '.' + columnTok.value
    }

    return {
      type: 'identifier',
      name,
    }
  }

  if (tok.type === 'number') {
    consume(state)
    return {
      type: 'literal',
      value: tok.numericValue ?? null,
    }
  }

  if (tok.type === 'string') {
    consume(state)
    return {
      type: 'literal',
      value: tok.value,
    }
  }

  if (tok.type === 'keyword') {
    if (tok.value === 'TRUE') {
      consume(state)
      return { type: 'literal', value: true }
    }
    if (tok.value === 'FALSE') {
      consume(state)
      return { type: 'literal', value: false }
    }
    if (tok.value === 'NULL') {
      consume(state)
      return { type: 'literal', value: null }
    }
  }

  throw new Error(
    'Unexpected token in expression at position ' + tok.position + ': ' + tok.type + ' ' + tok.value
  )
}

/**
 * @param {import('./types.js').ParserState} state
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
    const condition = parseExpression(state)

    joins.push({
      type: joinType,
      table: tableName,
      on: condition,
    })
  }

  return joins
}

/**
 * @param {import('./types.js').ParserState} state
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

  if (matchKeyword(state, 'WHERE')) {
    where = parseExpression(state)
  }

  if (matchKeyword(state, 'GROUP')) {
    expectKeyword(state, 'BY')
    while (true) {
      const expr = parseExpression(state)
      groupBy.push(expr)
      if (!match(state, 'comma')) break
    }
  }

  if (matchKeyword(state, 'ORDER')) {
    expectKeyword(state, 'BY')
    while (true) {
      const expr = parseExpression(state)
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
  /** @type {import('./types.js').ParserState} */
  const state = { tokens, pos: 0 }
  const ast = parseSelectInternal(state)

  const tok = current(state)
  if (tok.type !== 'eof') {
    throw new Error('Unexpected tokens after end of query at position ' + tok.position)
  }

  return ast
}
