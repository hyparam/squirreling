import {
  argCountParseError,
  invalidLiteralError,
  missingClauseError,
  syntaxError,
  unknownFunctionError,
} from '../parseErrors.js'
import { isAggregateFunc, isIntervalUnit, isMathFunc, isStringFunc, validateFunctionArgCount } from '../validation.js'
import { parseComparison } from './comparison.js'
import { parseSelectInternal } from './parse.js'
import { consume, current, expect, expectIdentifier, lastPosition, match, peekToken } from './state.js'

/**
 * @import { ExprNode, IntervalNode, ParserState, SelectStatement, WhenClause } from '../types.js'
 */

/**
 * @param {ParserState} state
 * @returns {IntervalNode}
 */
function parseInterval(state) {
  const { positionStart } = current(state)
  consume(state) // INTERVAL

  // Handle optional negative sign
  let sign = 1
  const signTok = current(state)
  if (signTok.type === 'operator' && signTok.value === '-') {
    consume(state)
    sign = -1
  }

  // Get value (number or quoted string)
  const valueTok = current(state)
  /** @type {number} */
  let value
  if (valueTok.type === 'number') {
    consume(state)
    value = sign * Number(valueTok.numericValue)
  } else if (valueTok.type === 'string') {
    consume(state)
    const parsed = parseFloat(valueTok.value)
    if (isNaN(parsed)) {
      throw invalidLiteralError({ type: 'interval value', value: valueTok.value, positionStart: valueTok.positionStart, positionEnd: valueTok.positionEnd })
    }
    value = sign * parsed
  } else {
    throw syntaxError({ expected: 'interval value (number)', received: `"${valueTok.value}"`, positionStart: valueTok.positionStart, positionEnd: valueTok.positionEnd })
  }

  // Get unit keyword
  const unitTok = current(state)
  if (unitTok.type !== 'keyword' || !isIntervalUnit(unitTok.value)) {
    throw invalidLiteralError({
      type: 'interval unit',
      value: unitTok.value,
      positionStart: unitTok.positionStart,
      positionEnd: unitTok.positionEnd,
      validValues: 'DAY, MONTH, YEAR, HOUR, MINUTE, SECOND',
    })
  }
  consume(state)

  return { type: 'interval', value, unit: unitTok.value, positionStart, positionEnd: lastPosition(state) }
}

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
        positionEnd: lastPosition(state),
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
      expect(state, 'paren', ')')
      return {
        type: 'cast',
        expr,
        toType: typeTok.value,
        positionStart,
        positionEnd: lastPosition(state),
      }
    }

    // function call
    if (next.type === 'paren' && next.value === '(') {
      const funcName = tok.value

      // validate function names
      if (!isStringFunc(funcName) && !isAggregateFunc(funcName) && !isMathFunc(funcName)) {
        throw unknownFunctionError({ funcName, positionStart: tok.positionStart, positionEnd: tok.positionEnd })
      }

      consume(state) // function name
      consume(state) // '('

      /** @type {ExprNode[]} */
      const args = []
      let distinct = false

      // Check for DISTINCT or ALL keyword (for aggregate functions like COUNT(DISTINCT x))
      if (current(state).type === 'keyword' && current(state).value === 'DISTINCT') {
        consume(state) // consume DISTINCT
        distinct = true
      } else if (current(state).type === 'keyword' && current(state).value === 'ALL') {
        consume(state) // consume ALL (default behavior, just consume it)
      }

      if (current(state).type !== 'paren' || current(state).value !== ')') {
        while (true) {
          // Handle COUNT(*) - treat * as a special identifier
          if (current(state).type === 'operator' && current(state).value === '*') {
            const starTok = current(state)
            consume(state)
            args.push({
              type: 'identifier',
              name: '*',
              positionStart: starTok.positionStart,
              positionEnd: lastPosition(state),
            })
          } else {
            const arg = parseExpression(state)
            args.push(arg)
          }
          if (!match(state, 'comma')) break
        }
      }

      expect(state, 'paren', ')')

      // Validate argument count at parse time
      const validation = validateFunctionArgCount(funcName, args.length)
      if (!validation.valid) {
        throw argCountParseError({
          funcName,
          expected: validation.expected,
          received: args.length,
          positionStart,
          positionEnd: lastPosition(state),
        })
      }

      return {
        type: 'function',
        name: funcName,
        args,
        distinct: distinct || undefined,
        positionStart,
        positionEnd: lastPosition(state),
      }
    }

    // Niladic datetime functions (no parentheses required per ANSI SQL)
    const niladicFuncs = ['CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP']
    if (niladicFuncs.includes(tok.value)) {
      consume(state)
      return {
        type: 'function',
        name: tok.value,
        args: [],
        positionStart,
        positionEnd: lastPosition(state),
      }
    }

    consume(state)
    let name = tok.value

    // table.column
    if (current(state).type === 'dot') {
      consume(state)
      const columnTok = expectIdentifier(state)
      name = name + '.' + columnTok.value
    }

    return {
      type: 'identifier',
      name,
      positionStart,
      positionEnd: lastPosition(state),
    }
  }

  if (tok.type === 'number') {
    consume(state)
    return {
      type: 'literal',
      value: tok.numericValue ?? null,
      positionStart,
      positionEnd: lastPosition(state),
    }
  }

  if (tok.type === 'string') {
    consume(state)
    return {
      type: 'literal',
      value: tok.value,
      positionStart,
      positionEnd: lastPosition(state),
    }
  }

  if (tok.type === 'keyword') {
    if (tok.value === 'TRUE') {
      consume(state)
      return { type: 'literal', value: true, positionStart, positionEnd: lastPosition(state) }
    }
    if (tok.value === 'FALSE') {
      consume(state)
      return { type: 'literal', value: false, positionStart, positionEnd: lastPosition(state) }
    }
    if (tok.value === 'NULL') {
      consume(state)
      return { type: 'literal', value: null, positionStart, positionEnd: lastPosition(state) }
    }
    if (tok.value === 'EXISTS') {
      consume(state) // EXISTS
      const subquery = parseSubquery(state)
      return {
        type: 'exists',
        subquery,
        positionStart,
        positionEnd: lastPosition(state),
      }
    }
    if (tok.value === 'CASE') {
      consume(state) // CASE

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
        whenClauses.push({ condition, result })
      }

      if (whenClauses.length === 0) {
        throw missingClauseError({
          missing: 'at least one WHEN clause',
          context: 'CASE expression',
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
        positionEnd: lastPosition(state),
      }
    }
    if (tok.value === 'INTERVAL') {
      return parseInterval(state)
    }
  }

  if (tok.type === 'operator' && tok.value === '-') {
    consume(state)
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
  throw syntaxError({ expected: 'expression', received: found, positionStart: tok.positionStart, positionEnd: tok.positionEnd })
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseOr(state) {
  let node = parseAnd(state)
  while (match(state, 'keyword', 'OR')) {
    const right = parseAnd(state)
    node = {
      type: 'binary',
      op: 'OR',
      left: node,
      right,
      positionStart: node.positionStart,
      positionEnd: right.positionEnd,
    }
  }
  return node
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseAnd(state) {
  let node = parseNot(state)
  while (match(state, 'keyword', 'AND')) {
    const right = parseNot(state)
    node = {
      type: 'binary',
      op: 'AND',
      left: node,
      right,
      positionStart: node.positionStart,
      positionEnd: right.positionEnd,
    }
  }
  return node
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
    const nextTok = current(state)
    if (nextTok.type === 'keyword' && nextTok.value === 'EXISTS') {
      consume(state) // EXISTS
      const subquery = parseSubquery(state)
      return {
        type: 'not exists',
        subquery,
        positionStart,
        positionEnd: lastPosition(state),
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
  let node = parseMultiplicative(state)
  while (true) {
    const tok = current(state)
    if (tok.type === 'operator' && (tok.value === '+' || tok.value === '-')) {
      consume(state)
      const right = parseMultiplicative(state)
      node = {
        type: 'binary',
        op: tok.value,
        left: node,
        right,
        positionStart: node.positionStart,
        positionEnd: right.positionEnd,
      }
    } else {
      break
    }
  }
  return node
}

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
function parseMultiplicative(state) {
  let node = parsePrimary(state)
  while (true) {
    const tok = current(state)
    if (tok.type === 'operator' && (tok.value === '*' || tok.value === '/' || tok.value === '%')) {
      consume(state)
      const right = parsePrimary(state)
      node = {
        type: 'binary',
        op: tok.value,
        left: node,
        right,
        positionStart: node.positionStart,
        positionEnd: right.positionEnd,
      }
    } else {
      break
    }
  }
  return node
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
