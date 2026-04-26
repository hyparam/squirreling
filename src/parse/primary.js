import { isCastType, isExtractField, isIntervalUnit, isKnownFunction, isTableFunction, niladicFuncs } from '../validation/functions.js'
import { InvalidLiteralError, ParseError, SyntaxError, UnknownFunctionError } from '../validation/parseErrors.js'
import { RESERVED_KEYWORDS } from '../validation/keywords.js'
import { parseExpression } from './expression.js'
import { parseFunctionCall } from './functions.js'
import { parseStatement } from './parse.js'
import { consume, current, expect, match, parseError, peekToken } from './state.js'

/**
 * @import { ExprNode, IntervalNode, ParserState, SqlPrimitive, WhenClause } from '../types.js'
 */

/**
 * Parse a primary expression, which is the innermost order of operations.
 *
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
      const subquery = parseStatement(state)
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

  // Array literal: [elem, elem, ...] — elements must be literals
  if (match(state, 'bracket', '[')) {
    /** @type {SqlPrimitive[]} */
    const values = []
    if (!match(state, 'bracket', ']')) {
      while (true) {
        const elemStart = current(state).positionStart
        const elem = parseExpression(state)
        if (elem.type !== 'literal') {
          throw new ParseError({
            message: 'Array literal elements must be constant literals',
            positionStart: elemStart,
            positionEnd: state.lastPos,
          })
        }
        values.push(elem.value)
        if (!match(state, 'comma')) break
      }
      expect(state, 'bracket', ']')
    }
    return {
      type: 'literal',
      value: values,
      positionStart,
      positionEnd: state.lastPos,
    }
  }

  if (tok.type === 'identifier') {
    const next = peekToken(state, 1)
    const funcNameUpper = tok.value.toUpperCase()

    // CAST(expr AS type)
    if (funcNameUpper === 'CAST' && next.type === 'paren' && next.value === '(') {
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
    if (funcNameUpper === 'EXTRACT' && next.type === 'paren' && next.value === '(') {
      consume(state) // EXTRACT
      consume(state) // '('
      const fieldTok = consume(state)
      const fieldUpper = fieldTok.value.toUpperCase()
      if (!isExtractField(fieldUpper)) {
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
        funcName: tok.originalValue ?? tok.value,
        args: [
          { type: 'literal', value: fieldTok.value, positionStart: fieldTok.positionStart, positionEnd: fieldTok.positionEnd },
          expr,
        ],
        positionStart,
        positionEnd: state.lastPos,
      }
    }

    // function call
    if (niladicFuncs.includes(funcNameUpper) || next.type === 'paren' && next.value === '(') {

      // Validate function existence early for better error messages
      if (!isKnownFunction(funcNameUpper, state.functions)) {
        throw new UnknownFunctionError({
          funcName: tok.value,
          positionStart,
          positionEnd: tok.positionEnd,
        })
      }

      if (isTableFunction(funcNameUpper)) {
        throw new ParseError({
          message: `${funcNameUpper} is a table function and can only be used in FROM clauses at position ${positionStart}`,
          positionStart,
          positionEnd: tok.positionEnd,
        })
      }

      return parseFunctionCall(state, positionStart)
    }

    // Table identifier
    let name = consume(state).value
    /** @type {string | undefined} */
    let prefix

    // table.column
    if (match(state, 'dot')) {
      prefix = name
      name = expect(state, 'identifier').value
    } else if (match(state, 'bracket', '[')) {
      // table['column'] — string subscript is equivalent to dot access
      const fieldTok = current(state)
      if (fieldTok.type !== 'string') {
        throw parseError(state, 'string literal')
      }
      consume(state)
      expect(state, 'bracket', ']')
      prefix = name
      name = fieldTok.value
    }

    return {
      type: 'identifier',
      name,
      prefix,
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
      const subquery = parseStatement(state)
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

  throw parseError(state, 'expression')
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
    value = Number(valueTok.numericValue)
  } else if (valueTok.type === 'string' && valueTok.value.trim() !== '') {
    value = Number(valueTok.value)
  } else {
    throw parseError(state, 'interval value (number)')
  }
  if (isNaN(value)) {
    throw new InvalidLiteralError({ expected: 'interval value', ...valueTok })
  }
  consume(state)

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
