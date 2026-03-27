import {
  invalidLiteralError,
  missingClauseError,
  syntaxError,
  unknownFunctionError,
} from '../validation/parseErrors.js'
import { RESERVED_KEYWORDS, isCastType, isExtractField, isIntervalUnit, isKnownFunction } from '../validation/functions.js'
import { parseComparison } from './comparison.js'
import { parseFunctionCall } from './functions.js'
import { parseSelectInternal } from './parse.js'
import { consume, current, expect, expectIdentifier, match, peekToken } from './state.js'

/**
 * @import { ExprNode, IntervalNode, ParserState, SelectStatement, WhenClause } from '../types.js'
 */

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
export function parseExpression(state) {
  return parseOr(state)
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
export function parsePrimary(state) {
  const tok = current(state)
  const { positionStart } = tok

  if (tok.type === 'paren' && tok.value === '(') {
    // Peek ahead to see if this is a scalar subquery
    const nextTok = peekToken(state, 1)
    if (nextTok.type === 'keyword' && nextTok.value === 'SELECT') {
      // It's a scalar subquery
      const subquery = parseSubquery(state)
      return {
        type: 'subquery',
        subquery,
        positionStart,
        positionEnd: state.lastPos,
      }
    }
    // Regular grouped expression
    consume(state)
    const expr = parseExpression(state)
    expect(state, 'paren', ')')
    return expr
  }

  if (tok.type === 'identifier') {
    const next = peekToken(state, 1)

    // CAST expression
    if (tok.value === 'CAST' && next.type === 'paren' && next.value === '(') {
      consume(state) // CAST
      consume(state) // '('
      const expr = parseExpression(state)
      expect(state, 'keyword', 'AS')
      const typeTok = expectIdentifier(state)
      const toType = typeTok.value.toUpperCase()
      if (!isCastType(toType)) {
        throw syntaxError({
          expected: 'cast type (STRING, INT, BIGINT, FLOAT, BOOL)',
          received: `"${typeTok.value}"`,
          after: 'AS',
          ...typeTok,
        })
      }
      expect(state, 'paren', ')')
      return {
        type: 'cast',
        expr,
        toType,
        positionStart,
        positionEnd: state.lastPos,
      }
    }

    // EXTRACT(field FROM expr)
    if (tok.value === 'EXTRACT' && next.type === 'paren' && next.value === '(') {
      consume(state) // EXTRACT
      consume(state) // '('
      const fieldTok = current(state)
      if (!isExtractField(fieldTok.value)) {
        throw syntaxError({
          expected: 'extract field (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DOW, EPOCH)',
          received: `"${fieldTok.value}"`,
          ...fieldTok,
        })
      }
      consume(state) // field
      expect(state, 'keyword', 'FROM')
      const expr = parseExpression(state)
      expect(state, 'paren', ')')
      return {
        type: 'function',
        funcName: 'EXTRACT',
        args: [
          { type: 'literal', value: fieldTok.value, positionStart: fieldTok.positionStart, positionEnd: fieldTok.positionEnd },
          expr,
        ],
        positionStart,
        positionEnd: state.lastPos,
      }
    }

    // function call
    if (next.type === 'paren' && next.value === '(') {
      const funcName = tok.value

      // Validate function existence early for better error messages
      if (!isKnownFunction(funcName.toUpperCase(), state.functions)) {
        throw unknownFunctionError({
          funcName,
          positionStart,
          positionEnd: tok.positionEnd,
        })
      }

      consume(state) // function name
      return parseFunctionCall(state, funcName, positionStart)
    }

    // Niladic datetime functions (no parentheses required per ANSI SQL)
    const niladicFuncs = ['CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP']
    if (niladicFuncs.includes(tok.value)) {
      consume(state)
      return {
        type: 'function',
        funcName: tok.value,
        args: [],
        positionStart,
        positionEnd: state.lastPos,
      }
    }

    consume(state)
    let name = tok.value

    // table.column
    if (match(state, 'dot')) {
      const columnTok = expectIdentifier(state)
      name = name + '.' + columnTok.value
    }

    return {
      type: 'identifier',
      name,
      positionStart,
      positionEnd: state.lastPos,
    }
  }

  if (tok.type === 'number') {
    consume(state)
    return {
      type: 'literal',
      value: tok.numericValue ?? null,
      positionStart,
      positionEnd: state.lastPos,
    }
  }

  if (tok.type === 'string') {
    consume(state)
    return {
      type: 'literal',
      value: tok.value,
      positionStart,
      positionEnd: state.lastPos,
    }
  }

  // Keywords that can be used as function names (e.g., LEFT, RIGHT)
  if (tok.type === 'keyword') {
    const next = peekToken(state, 1)
    if (next.type === 'paren' && next.value === '(' && isKnownFunction(tok.value, state.functions)) {
      consume(state) // function name
      return parseFunctionCall(state, tok.value, positionStart)
    }

    if (match(state, 'keyword', 'TRUE')) {
      return { type: 'literal', value: true, positionStart, positionEnd: state.lastPos }
    }
    if (match(state, 'keyword', 'FALSE')) {
      return { type: 'literal', value: false, positionStart, positionEnd: state.lastPos }
    }
    if (match(state, 'keyword', 'NULL')) {
      return { type: 'literal', value: null, positionStart, positionEnd: state.lastPos }
    }
    if (match(state, 'keyword', 'EXISTS')) {
      const subquery = parseSubquery(state)
      return {
        type: 'exists',
        subquery,
        positionStart,
        positionEnd: state.lastPos,
      }
    }
    if (match(state, 'keyword', 'CASE')) {
      // Check if it's simple CASE (CASE expr WHEN ...) or searched CASE (CASE WHEN ...)
      /** @type {ExprNode | undefined} */
      let caseExpr
      const nextTok = current(state)
      if (nextTok.type !== 'keyword' || nextTok.value !== 'WHEN') {
        // Simple CASE: parse the case expression
        caseExpr = parseExpression(state)
      }

      // Parse WHEN clauses
      /** @type {WhenClause[]} */
      const whenClauses = []
      while (match(state, 'keyword', 'WHEN')) {
        const condition = parseExpression(state)
        expect(state, 'keyword', 'THEN')
        const result = parseExpression(state)
        whenClauses.push({
          condition,
          result,
          positionStart: condition.positionStart,
          positionEnd: result.positionEnd,
        })
      }

      if (whenClauses.length === 0) {
        throw missingClauseError({
          missing: 'at least one WHEN clause',
          context: 'CASE expression',
          positionStart,
          positionEnd: state.lastPos,
        })
      }

      // Parse optional ELSE clause
      /** @type {ExprNode | undefined} */
      let elseResult
      if (match(state, 'keyword', 'ELSE')) {
        elseResult = parseExpression(state)
      }

      expect(state, 'keyword', 'END')

      return {
        type: 'case',
        caseExpr,
        whenClauses,
        elseResult,
        positionStart,
        positionEnd: state.lastPos,
      }
    }
    if (tok.value === 'INTERVAL') {
      return parseInterval(state)
    }

    // Non-reserved keywords can be used as identifiers (e.g. column aliases)
    if (!RESERVED_KEYWORDS.has(tok.value)) {
      consume(state)
      return {
        type: 'identifier',
        name: tok.originalValue ?? tok.value,
        positionStart,
        positionEnd: state.lastPos,
      }
    }
  }

  if (match(state, 'operator', '-')) {
    const argument = parsePrimary(state)
    return {
      type: 'unary',
      op: '-',
      argument,
      positionStart,
      positionEnd: argument.positionEnd,
    }
  }

  const found = tok.type === 'eof' ? 'end of query' : `"${tok.originalValue ?? tok.value}"`
  throw syntaxError({ expected: 'expression', received: found, ...tok })
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseOr(state) {
  let left = parseAnd(state)
  while (match(state, 'keyword', 'OR')) {
    const right = parseAnd(state)
    left = {
      type: 'binary',
      op: 'OR',
      left,
      right,
      positionStart: left.positionStart,
      positionEnd: right.positionEnd,
    }
  }
  return left
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseAnd(state) {
  let left = parseNot(state)
  while (match(state, 'keyword', 'AND')) {
    const right = parseNot(state)
    left = {
      type: 'binary',
      op: 'AND',
      left,
      right,
      positionStart: left.positionStart,
      positionEnd: right.positionEnd,
    }
  }
  return left
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseNot(state) {
  const tok = current(state)
  if (match(state, 'keyword', 'NOT')) {
    const { positionStart } = tok
    // Check for NOT EXISTS
    if (match(state, 'keyword', 'EXISTS')) {
      const subquery = parseSubquery(state)
      return {
        type: 'not exists',
        subquery,
        positionStart,
        positionEnd: state.lastPos,
      }
    }
    const argument = parseNot(state)
    return {
      type: 'unary',
      op: 'NOT',
      argument,
      positionStart,
      positionEnd: argument.positionEnd,
    }
  }
  return parseComparison(state)
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
export function parseAdditive(state) {
  let left = parseMultiplicative(state)
  while (true) {
    const tok = current(state)
    if (tok.type === 'operator' && (tok.value === '+' || tok.value === '-')) {
      consume(state)
      const right = parseMultiplicative(state)
      // Recursive left-associative binary operator
      left = {
        type: 'binary',
        op: tok.value,
        left,
        right,
        positionStart: left.positionStart,
        positionEnd: right.positionEnd,
      }
    } else {
      break
    }
  }
  return left
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseMultiplicative(state) {
  let left = parsePrimary(state)
  while (true) {
    const tok = current(state)
    if (tok.type === 'operator' && (tok.value === '*' || tok.value === '/' || tok.value === '%')) {
      consume(state)
      const right = parsePrimary(state)
      // Recursively build left-associative tree for multiplicative operators
      left = {
        type: 'binary',
        op: tok.value,
        left,
        right,
        positionStart: left.positionStart,
        positionEnd: right.positionEnd,
      }
    } else {
      break
    }
  }
  return left
}

/**
 * Creates an ExprCursor adapter for the ParserState.
 *
 * @param {ParserState} state
 * @returns {SelectStatement}
 */
export function parseSubquery(state) {
  expect(state, 'paren', '(')
  const query = parseSelectInternal(state)
  expect(state, 'paren', ')')
  return query
}

/**
 * @param {ParserState} state
 * @returns {IntervalNode}
 */
function parseInterval(state) {
  const { positionStart } = expect(state, 'keyword', 'INTERVAL')

  // Get value (number or quoted string)
  const valueTok = current(state)
  /** @type {number} */
  let value
  if (valueTok.type === 'number') {
    consume(state)
    value = Number(valueTok.numericValue)
  } else if (valueTok.type === 'string') {
    consume(state)
    const parsed = parseFloat(valueTok.value)
    if (isNaN(parsed)) {
      throw invalidLiteralError({ expected: 'interval value', ...valueTok })
    }
    value = parsed
  } else {
    throw syntaxError({ expected: 'interval value (number)', received: `"${valueTok.value}"`, ...valueTok })
  }

  // Get unit keyword
  const unitTok = current(state)
  if (unitTok.type !== 'keyword' || !isIntervalUnit(unitTok.value)) {
    throw invalidLiteralError({
      expected: 'interval unit',
      validValues: 'DAY, MONTH, YEAR, HOUR, MINUTE, SECOND',
      ...unitTok,
    })
  }
  consume(state)

  return { type: 'interval', value, unit: unitTok.value, positionStart, positionEnd: state.lastPos }
}
