import { expectNoAggregate } from '../validation/aggregates.js'
import { isTableFunction, validateFunctionArgs } from '../validation/functions.js'
import { ParseError } from '../validation/parseErrors.js'
import { parseExpression } from './expression.js'
import { isTableFunctionStart, parseFromFunction, parseTableAlias, tableFunctionColumnCount, tableFunctionDefaultColumns } from './parse.js'
import { consume, current, expect, match } from './state.js'

/**
 * @import { ExprNode, JoinClause, JoinType, ParserState } from '../types.js'
 */

/**
 * @param {ParserState} state
 * @returns {JoinClause[]}
 */
export function parseJoins(state) {
  /** @type {JoinClause[]} */
  const joins = []

  while (true) {
    const tok = current(state)

    // LATERAL VIEW [OUTER] func(args) tableAlias AS colAlias[, ...] (Spark/Hive style)
    if (current(state).type === 'keyword' && current(state).value === 'LATERAL') {
      const lateralStart = tok.positionStart
      consume(state)
      expect(state, 'keyword', 'VIEW')
      const isOuter = match(state, 'keyword', 'OUTER')
      const funcTok = current(state)
      if (funcTok.type !== 'identifier' || !isTableFunction(funcTok.value.toUpperCase())) {
        throw new ParseError({
          message: 'LATERAL VIEW requires a table function like EXPLODE',
          positionStart: funcTok.positionStart,
          positionEnd: funcTok.positionEnd,
        })
      }
      consume(state)
      const funcName = funcTok.value.toUpperCase()
      expect(state, 'paren', '(')
      /** @type {ExprNode[]} */
      const args = []
      if (!match(state, 'paren', ')')) {
        while (true) {
          args.push(parseExpression(state))
          if (!match(state, 'comma')) break
        }
        expect(state, 'paren', ')')
      }
      validateFunctionArgs(funcName, args.length, funcTok.positionStart, state.lastPos, state.functions)

      const aliasTok = current(state)
      if (aliasTok.type !== 'identifier') {
        throw new ParseError({
          message: 'LATERAL VIEW requires a table alias before AS',
          positionStart: aliasTok.positionStart,
          positionEnd: aliasTok.positionEnd,
        })
      }
      consume(state)
      const tableAlias = aliasTok.value

      expect(state, 'keyword', 'AS')
      /** @type {string[]} */
      const columnAliases = []
      const colStart = state.lastPos
      while (true) {
        const colTok = expect(state, 'identifier')
        columnAliases.push(colTok.value)
        if (!match(state, 'comma')) break
      }
      const maxCols = tableFunctionColumnCount(funcName)
      if (columnAliases.length > maxCols) {
        const colLabels = tableFunctionDefaultColumns(funcName).join(', ')
        throw new ParseError({
          message: maxCols === 1
            ? `${funcName} produces a single column; only one column alias is allowed`
            : `${funcName} produces at most ${maxCols} columns (${colLabels}); too many column aliases`,
          positionStart: colStart,
          positionEnd: state.lastPos,
        })
      }

      /** @type {import('../ast.js').FromFunction} */
      const fromFunction = {
        type: 'function',
        funcName,
        args,
        alias: tableAlias,
        columnAliases,
        positionStart: funcTok.positionStart,
        positionEnd: state.lastPos,
      }

      /** @type {JoinType} */
      const joinType = isOuter ? 'LEFT' : 'CROSS'
      /** @type {ExprNode | undefined} */
      const condition = isOuter
        ? { type: 'literal', value: true, positionStart: lateralStart, positionEnd: state.lastPos }
        : undefined

      joins.push({
        joinType,
        table: funcName,
        alias: tableAlias,
        on: condition,
        fromFunction,
        positionStart: lateralStart,
        positionEnd: state.lastPos,
      })
      continue
    }

    // Comma-join: implicit CROSS JOIN LATERAL, currently only for table functions.
    if (match(state, 'comma')) {
      if (!isTableFunctionStart(state)) {
        throw new ParseError({
          message: 'Comma-separated FROM is only supported with table functions like UNNEST; use explicit JOIN ... ON ... for regular tables',
          positionStart: tok.positionStart,
          positionEnd: state.lastPos,
        })
      }
      const fromFunction = parseFromFunction(state)
      joins.push({
        joinType: 'CROSS',
        table: fromFunction.funcName,
        alias: fromFunction.alias,
        fromFunction,
        positionStart: tok.positionStart,
        positionEnd: state.lastPos,
      })
      continue
    }

    // Check for join keywords
    /** @type {JoinType} */
    let joinType = 'INNER'

    if (match(state, 'keyword', 'INNER')) {
      joinType = 'INNER'
    } else if (match(state, 'keyword', 'LEFT')) {
      match(state, 'keyword', 'OUTER') // LEFT OUTER JOIN
      joinType = 'LEFT'
    } else if (match(state, 'keyword', 'RIGHT')) {
      match(state, 'keyword', 'OUTER') // RIGHT OUTER JOIN
      joinType = 'RIGHT'
    } else if (match(state, 'keyword', 'FULL')) {
      match(state, 'keyword', 'OUTER') // FULL OUTER JOIN
      joinType = 'FULL'
    } else if (match(state, 'keyword', 'POSITIONAL')) {
      joinType = 'POSITIONAL'
    } else if (match(state, 'keyword', 'CROSS')) {
      joinType = 'CROSS'
    } else if (!match(state, 'keyword', 'JOIN')) {
      // Not a join keyword, stop parsing joins
      break
    }

    // If we consumed a type keyword, expect JOIN next
    if (tok.value !== 'JOIN') {
      expect(state, 'keyword', 'JOIN')
    }

    // Optional LATERAL keyword; table functions are implicitly LATERAL.
    const lateralTok = current(state)
    const hasLateral = match(state, 'keyword', 'LATERAL')

    // Table function on the right side (e.g. JOIN UNNEST(t.arr) AS u(x))
    if (isTableFunctionStart(state)) {
      if (joinType === 'POSITIONAL') {
        throw new ParseError({
          message: 'POSITIONAL JOIN does not support table functions',
          positionStart: tok.positionStart,
          positionEnd: state.lastPos,
        })
      }
      if (joinType === 'RIGHT' || joinType === 'FULL') {
        throw new ParseError({
          message: `${joinType} JOIN not supported with table functions — right side depends on left row`,
          positionStart: tok.positionStart,
          positionEnd: state.lastPos,
        })
      }
      const fromFunction = parseFromFunction(state)

      /** @type {ExprNode | undefined} */
      let condition
      if (joinType !== 'CROSS') {
        expect(state, 'keyword', 'ON')
        condition = parseExpression(state)
        expectNoAggregate(condition, 'JOIN ON')
      }

      joins.push({
        joinType,
        table: fromFunction.funcName,
        alias: fromFunction.alias,
        on: condition,
        fromFunction,
        positionStart: tok.positionStart,
        positionEnd: state.lastPos,
      })
      continue
    }

    if (hasLateral) {
      throw new ParseError({
        message: 'LATERAL is only supported with table functions',
        positionStart: lateralTok.positionStart,
        positionEnd: lateralTok.positionEnd,
      })
    }

    if (joinType === 'CROSS') {
      throw new ParseError({
        message: 'CROSS JOIN is currently supported only with table functions like UNNEST',
        positionStart: tok.positionStart,
        positionEnd: state.lastPos,
      })
    }

    // Parse table name and optional alias
    const tableTok = expect(state, 'identifier')
    const tableAlias = parseTableAlias(state)

    // Parse ON condition (not for POSITIONAL joins)
    /** @type {ExprNode | undefined} */
    let condition
    if (joinType !== 'POSITIONAL') {
      expect(state, 'keyword', 'ON')
      condition = parseExpression(state)
      expectNoAggregate(condition, 'JOIN ON')
    }

    joins.push({
      joinType,
      table: tableTok.value,
      alias: tableAlias,
      on: condition,
      positionStart: tok.positionStart,
      positionEnd: tableTok.positionEnd,
    })
  }

  return joins
}
