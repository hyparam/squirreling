import { isCastType, isExtractField, isIntervalUnit, isKnownFunction, niladicFuncs } from '../validation/functions.js'
import { InvalidLiteralError, ParseError, SyntaxError, UnknownFunctionError } from '../validation/parseErrors.js'
import { RESERVED_KEYWORDS } from '../validation/keywords.js'
import { parseExpression } from './expression.js'
import { parseFunctionCall } from './functions.js'
import { parseSelectInternal } from './parse.js'
import { consume, current, expect, match, peekToken } from './state.js'

/**
 * @import { ExprNode, IntervalNode, ParserState, WhenClause } from '../types.js'
 */

/**
 * @param {ParserState} state
 * @returns {ExprNode}
 */
export function parsePrimary(state) {
  const tok = current(state)
  const { positionStart } = tok

  if (match(state, 'paren', '(')) {
    // Peek ahead to see if this is a scalar subquery
    const next = current(state)
    if (next.type === 'keyword' && next.value === 'SELECT') {
      // It's a scalar subquery
      const subquery = parseSelectInternal(state)
      expect(state, 'paren', ')')
      return {
        type: 'subquery',
        subquery,
        positionStart,
        positionEnd: state.lastPos,
      }
    }
    // Regular grouped expression
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
      const typeTok = expect(state, 'identifier')
      const toType = typeTok.value.toUpperCase()
      if (!isCastType(toType)) {
        throw new SyntaxError({
          expected: 'cast type (STRING, INT, BIGINT, FLOAT, BOOL)',
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
      const fieldTok = consume(state)
      if (!isExtractField(fieldTok.value)) {
        throw new SyntaxError({
          expected: 'extract field (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DOW, EPOCH)',
          after: 'EXTRACT(',
          ...fieldTok,
        })
      }
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
    const funcNameUpper = tok.value.toUpperCase()
    if (niladicFuncs.includes(funcNameUpper) || next.type === 'paren' && next.value === '(') {

      // Validate function existence early for better error messages
      if (!isKnownFunction(funcNameUpper, state.functions)) {
        throw new UnknownFunctionError({
          funcName: tok.value,
          positionStart,
          positionEnd: tok.positionEnd,
        })
      }

      return parseFunctionCall(state, positionStart)
    }

    // Table identifier
    let name = consume(state).value

    // table.column
    if (match(state, 'dot')) {
      name += '.' + expect(state, 'identifier').value
    }

    return {
      type: 'identifier',
      name,
      positionStart,
      positionEnd: state.lastPos,
    }
  }

  if (tok.type === 'number' || tok.type === 'string') {
    consume(state)
    return {
      type: 'literal',
      value: tok.numericValue ?? tok.value,
      positionStart,
      positionEnd: state.lastPos,
    }
  }

  // Keywords that can be used as function names (e.g., LEFT, RIGHT)
  if (tok.type === 'keyword') {
    const next = peekToken(state, 1)
    if (next.type === 'paren' && next.value === '(' && isKnownFunction(tok.value, state.functions)) {
      return parseFunctionCall(state, positionStart)
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
      expect(state, 'paren', '(')
      const subquery = parseSelectInternal(state)
      expect(state, 'paren', ')')
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
      const next = current(state)
      if (next.type !== 'keyword' || next.value !== 'WHEN') {
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
        throw new ParseError({
          message: 'CASE expression requires at least one WHEN clause',
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

  throw new SyntaxError({ expected: 'expression', ...tok })
}

/**
 * @param {ParserState} state
 * @returns {IntervalNode}
 */
function parseInterval(state) {
  const { positionStart } = expect(state, 'keyword', 'INTERVAL')

  // Get value (number or quoted string)
  const valueTok = consume(state)
  /** @type {number} */
  let value
  if (valueTok.type === 'number') {
    value = Number(valueTok.numericValue)
  } else if (valueTok.type === 'string') {
    value = parseFloat(valueTok.value)
    if (isNaN(value)) {
      throw new InvalidLiteralError({ expected: 'interval value', ...valueTok })
    }
  } else {
    throw new SyntaxError({ expected: 'interval value (number)', ...valueTok })
  }

  // Get unit keyword
  const unitTok = consume(state)
  if (unitTok.type !== 'keyword' || !isIntervalUnit(unitTok.value)) {
    throw new InvalidLiteralError({
      expected: 'interval unit',
      validValues: 'DAY, MONTH, YEAR, HOUR, MINUTE, SECOND',
      ...unitTok,
    })
  }

  return { type: 'interval', value, unit: unitTok.value, positionStart, positionEnd: state.lastPos }
}
