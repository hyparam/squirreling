import { derivedAlias } from '../expression/alias.js'
import { evaluateAll, evaluateExpr } from '../expression/evaluate.js'
import { collectColumnsFromExpr } from '../plan/columns.js'
import { isAggregateFunc } from '../validation/functions.js'
import { finalizeAccumulator, newAccumulator, updateAccumulator } from './accumulator.js'
import { sortEntriesByTerms } from './sort.js'
import { keyify } from './utils.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { AsyncCells, AsyncRow, ExecuteContext, ExprNode, FunctionNode, IdentifierNode, QueryResults, SelectColumn, SqlPrimitive } from '../types.js'
 * @import { HashAggregateNode, ScalarAggregateNode } from '../plan/types.js'
 * @import { Accumulator } from './accumulator.js'
 */

// Accumulate rows in chunks of this size so aborts can fire and async cells overlap
const CHUNK_SIZE = 4000

// Aggregate functions whose state can be accumulated one row at a time with
// bounded memory. Aggregates outside this set (MEDIAN, ARRAY_AGG, STDDEV, ...)
// need the full value set, so their queries buffer rows instead.
const STREAMABLE_FUNCS = new Set(['COUNT', 'COUNTIF', 'SUM', 'AVG', 'MIN', 'MAX'])

/**
 * Specs are keyed by aggregate node identity, so two aggregates that differ
 * only in FILTER accumulate separately even though their derived aliases match.
 *
 * @typedef {{
 *   node: FunctionNode,
 *   funcName: string,
 *   star: boolean,
 * }} StreamingAggSpec
 */

/**
 * @typedef {{
 *   firstRow: AsyncRow | undefined,
 *   keyValues: SqlPrimitive[],
 *   accumulators: Accumulator[],
 * }} StreamingGroup
 */

/**
 * The streaming plan for an aggregate node: which aggregate calls to
 * accumulate, which expression nodes are group key references (substituted
 * from the group's key values), and whether any expression still needs a
 * representative row from the group. When needsRow is false, no input rows
 * are retained, so memory is bounded by the number of groups — plus, for
 * COUNT(DISTINCT ...), each group's set of distinct values — even for
 * high-cardinality GROUP BY.
 *
 * @typedef {{
 *   specs: StreamingAggSpec[],
 *   keyRefs: Map<ExprNode, number>,
 *   needsRow: boolean,
 * }} StreamingAggPlan
 */

/**
 * Structural signature of an expression node, ignoring source positions, so
 * an expression repeated in SELECT and GROUP BY compares equal.
 *
 * @param {ExprNode} node
 * @returns {string}
 */
function exprSig(node) {
  return JSON.stringify(node, (key, value) => {
    if (key === 'positionStart' || key === 'positionEnd') return undefined
    // JSON.stringify throws on BigInt literal values; wrap so 1n !== '1n'
    if (typeof value === 'bigint') return { bigint: value.toString() }
    return value
  })
}

/**
 * Extracts the aggregate calls an aggregate node needs so they can be
 * computed incrementally, without buffering the group's rows. Returns
 * undefined when any expression needs a buffered group: an aggregate outside
 * STREAMABLE_FUNCS, an aggregate over a non-scalar argument, or a subquery.
 * Also returns undefined when an aggregate references a column the child
 * does not produce: projection pushdown prunes columns whose output cells
 * are never read, and only the buffered path defers evaluation of those cells.
 *
 * @param {Pick<HashAggregateNode, 'columns' | 'having'> & Partial<Pick<HashAggregateNode, 'orderBy' | 'groupBy'>>} plan
 * @param {string[]} [childColumns] - columns produced by the child plan
 * @returns {StreamingAggPlan | undefined}
 */
export function planStreamingAggregates({ columns, having, orderBy, groupBy }, childColumns) {
  const groupExprs = groupBy ?? []
  const groupSigs = groupExprs.map(exprSig)
  /** @type {StreamingAggSpec[]} */
  const specs = []
  /** @type {Map<ExprNode, number>} */
  const keyRefs = new Map()
  let needsRow = false

  /**
   * Index of the group key the expression structurally matches, or -1.
   * Only exact matches qualify: a bare identifier and a qualified group key
   * (or vice versa) can resolve to different columns in a join, so mixed
   * qualification falls back to evaluating against the representative row.
   *
   * @param {ExprNode} node
   * @returns {number}
   */
  function matchGroupKey(node) {
    const signature = exprSig(node)
    for (let i = 0; i < groupExprs.length; i++) {
      if (groupSigs[i] === signature) return i
    }
    return -1
  }

  /**
   * Walks an expression collecting streamable aggregate calls and group key
   * references. Returns false if the expression cannot be evaluated from
   * precomputed values plus a representative row. `lazy` marks positions the
   * evaluator can skip (short-circuited AND/OR right sides and CASE
   * branches): an aggregate there may never be evaluated by the buffered
   * path, so accumulating it eagerly could evaluate expressions the query
   * never asks for, and the query falls back to buffered aggregation.
   *
   * @param {ExprNode} node
   * @param {boolean} lazy
   * @returns {boolean}
   */
  function walk(node, lazy) {
    const keyIndex = matchGroupKey(node)
    if (keyIndex >= 0) {
      keyRefs.set(node, keyIndex)
      return true
    }
    switch (node.type) {
    case 'literal':
    case 'interval':
      return true
    case 'identifier':
    case 'star':
      // resolves against the group's representative row
      needsRow = true
      return true
    case 'unary':
      return walk(node.argument, lazy)
    case 'binary':
      if (node.op === 'AND' || node.op === 'OR') {
        // the right side is skipped when the left side short-circuits
        return walk(node.left, lazy) && walk(node.right, true)
      }
      return walk(node.left, lazy) && walk(node.right, lazy)
    case 'cast':
      return walk(node.expr, lazy)
    case 'case':
      // only the first WHEN condition is always evaluated; later
      // conditions, results, and ELSE run only when reached
      return (!node.caseExpr || walk(node.caseExpr, lazy)) &&
        node.whenClauses.every((w, i) => walk(w.condition, lazy || i > 0) && walk(w.result, true)) &&
        (!node.elseResult || walk(node.elseResult, true))
    case 'in valuelist':
      // values after the first are skipped once an earlier value matches
      return walk(node.expr, lazy) && node.values.every((v, i) => walk(v, lazy || i > 0))
    case 'function': {
      const funcName = node.funcName.toUpperCase()
      if (!isAggregateFunc(funcName)) {
        return node.args.every(arg => walk(arg, lazy))
      }
      if (lazy) return false
      if (!STREAMABLE_FUNCS.has(funcName)) return false
      const star = node.args[0]?.type === 'star'
      if (!star && !node.args.every(arg => isScalarExpr(arg))) return false
      if (node.filter && !isScalarExpr(node.filter)) return false
      if (!specs.some(spec => spec.node === node)) {
        specs.push({ node, funcName, star })
      }
      return true
    }
    default:
      // subqueries, EXISTS, IN (subquery), window functions
      return false
    }
  }

  for (const col of columns) {
    if (col.type === 'star') {
      needsRow = true
      continue
    }
    if (!walk(col.expr, false)) return
  }
  if (having && !walk(having, false)) return
  const orderTerms = orderBy ?? []
  for (let i = 0; i < orderTerms.length; i++) {
    // the sorter evaluates later terms only to break ties on earlier terms
    if (!walk(orderTerms[i].expr, i > 0)) return
  }
  if (childColumns && !specsResolvable(specs, childColumns)) return
  return { specs, keyRefs, needsRow }
}

/**
 * Reports whether every identifier the streaming path evaluates eagerly
 * (aggregate arguments and FILTER conditions) can resolve against the
 * child's output columns, using the same exact-then-suffix matching as
 * identifier evaluation. Anything unresolvable means projection pushdown
 * pruned the column, so the query must buffer instead.
 *
 * @param {StreamingAggSpec[]} specs
 * @param {string[]} childColumns
 * @returns {boolean}
 */
function specsResolvable(specs, childColumns) {
  /** @type {IdentifierNode[]} */
  const identifiers = []
  for (const spec of specs) {
    collectColumnsFromExpr(spec.node, identifiers)
  }
  return identifiers.every(({ prefix, name }) => {
    if (childColumns.includes(prefix ? `${prefix}.${name}` : name)) return true
    if (!prefix) return childColumns.some(col => col.endsWith('.' + name))
    // a qualified name may also resolve as struct access on a base column,
    // or fall back to the bare column part
    return childColumns.includes(prefix) ||
      childColumns.some(col => col.endsWith('.' + prefix)) ||
      childColumns.includes(name)
  })
}

/**
 * Reports whether an expression is a plain scalar over row values: no
 * aggregates and no subqueries, so it can be evaluated per input row.
 *
 * @param {ExprNode} node
 * @returns {boolean}
 */
function isScalarExpr(node) {
  switch (node.type) {
  case 'literal':
  case 'identifier':
  case 'star':
  case 'interval':
    return true
  case 'unary':
    return isScalarExpr(node.argument)
  case 'binary':
    return isScalarExpr(node.left) && isScalarExpr(node.right)
  case 'cast':
    return isScalarExpr(node.expr)
  case 'case':
    return (!node.caseExpr || isScalarExpr(node.caseExpr)) &&
      node.whenClauses.every(w => isScalarExpr(w.condition) && isScalarExpr(w.result)) &&
      (!node.elseResult || isScalarExpr(node.elseResult))
  case 'in valuelist':
    return isScalarExpr(node.expr) && node.values.every(v => isScalarExpr(v))
  case 'function':
    return !isAggregateFunc(node.funcName.toUpperCase()) && node.args.every(arg => isScalarExpr(arg))
  default:
    return false
  }
}

/**
 * Replaces each precomputed node in an expression with its value as a
 * literal: aggregate calls (keyed by node identity) and group key references,
 * so the rest of the expression can be evaluated against a representative
 * row. Nodes without precomputed values are returned unchanged.
 *
 * @param {ExprNode} node
 * @param {Map<ExprNode, SqlPrimitive>} values - computed value per substituted node
 * @returns {ExprNode}
 */
function substituteValues(node, values) {
  if (values.has(node)) {
    // group keys over missing columns evaluate to undefined; preserve it
    // so streaming output matches the buffered path's evaluation result
    // eslint-disable-next-line no-extra-parens
    const value = /** @type {SqlPrimitive} */ (values.get(node))
    return {
      type: 'literal',
      value,
      positionStart: node.positionStart,
      positionEnd: node.positionEnd,
    }
  }
  switch (node.type) {
  case 'unary': {
    const argument = substituteValues(node.argument, values)
    return argument === node.argument ? node : { ...node, argument }
  }
  case 'binary': {
    const left = substituteValues(node.left, values)
    const right = substituteValues(node.right, values)
    return left === node.left && right === node.right ? node : { ...node, left, right }
  }
  case 'cast': {
    const expr = substituteValues(node.expr, values)
    return expr === node.expr ? node : { ...node, expr }
  }
  case 'case': {
    const caseExpr = node.caseExpr && substituteValues(node.caseExpr, values)
    const whenClauses = node.whenClauses.map(w => {
      const condition = substituteValues(w.condition, values)
      const result = substituteValues(w.result, values)
      return condition === w.condition && result === w.result ? w : { ...w, condition, result }
    })
    const elseResult = node.elseResult && substituteValues(node.elseResult, values)
    return { ...node, caseExpr, whenClauses, elseResult }
  }
  case 'in valuelist': {
    const expr = substituteValues(node.expr, values)
    const valueNodes = node.values.map(v => substituteValues(v, values))
    return { ...node, expr, values: valueNodes }
  }
  case 'function': {
    const args = node.args.map(arg => substituteValues(arg, values))
    return args.every((arg, i) => arg === node.args[i]) ? node : { ...node, args }
  }
  default:
    return node
  }
}

/**
 * Folds one chunk of rows into the group accumulators. Group keys, FILTER
 * conditions, and aggregate arguments are each evaluated across the whole
 * chunk so async cells overlap; the chunk is released afterwards.
 *
 * @param {object} options
 * @param {AsyncRow[]} options.chunk
 * @param {ExprNode[]} options.groupBy
 * @param {StreamingAggSpec[]} options.specs
 * @param {Map<unknown, StreamingGroup>} options.groups
 * @param {boolean} options.needsRow - retain each group's first row?
 * @param {ExecuteContext} options.context
 * @returns {Promise<void>}
 */
async function accumulateChunk({ chunk, groupBy, specs, groups, needsRow, context }) {
  /** @type {SqlPrimitive[][] | undefined} */
  let keyColumns
  if (groupBy.length) {
    keyColumns = await Promise.all(groupBy.map(expr => evaluateAll(expr, chunk, context)))
  }

  /** @type {(SqlPrimitive[] | undefined)[]} */
  const filters = new Array(specs.length)
  /** @type {(SqlPrimitive[] | undefined)[]} */
  const args = new Array(specs.length)
  for (let s = 0; s < specs.length; s++) {
    const { node, star } = specs[s]
    if (node.filter) {
      const passes = await evaluateAll(node.filter, chunk, context)
      filters[s] = passes
      if (!star) {
        // The buffered path filters the group before evaluating arguments,
        // so only evaluate the argument for rows that pass the FILTER
        /** @type {AsyncRow[]} */
        const passingRows = []
        /** @type {number[]} */
        const passingIndices = []
        for (let j = 0; j < chunk.length; j++) {
          if (passes[j]) {
            passingRows.push(chunk[j])
            passingIndices.push(j)
          }
        }
        const values = await evaluateAll(node.args[0], passingRows, context)
        const spread = new Array(chunk.length).fill(null)
        for (let k = 0; k < passingIndices.length; k++) {
          spread[passingIndices[k]] = values[k]
        }
        args[s] = spread
      }
    } else {
      args[s] = star ? undefined : await evaluateAll(node.args[0], chunk, context)
    }
  }

  for (let j = 0; j < chunk.length; j++) {
    const key = keyColumns
      ? keyColumns.length === 1 ? keyify(keyColumns[0][j]) : keyify(...keyColumns.map(c => c[j]))
      : true
    let group = groups.get(key)
    if (!group) {
      group = {
        firstRow: needsRow ? chunk[j] : undefined,
        keyValues: keyColumns ? keyColumns.map(c => c[j]) : [],
        accumulators: specs.map(spec => newAccumulator(spec.funcName, spec.node.distinct)),
      }
      groups.set(key, group)
    }
    for (let s = 0; s < specs.length; s++) {
      const filter = filters[s]
      if (filter && !filter[j]) continue
      const spec = specs[s]
      if (spec.star && spec.funcName === 'COUNT') {
        group.accumulators[s].count++
      } else {
        const arg = args[s]
        updateAccumulator(spec.funcName, group.accumulators[s], arg ? arg[j] : null)
      }
    }
  }
}

/**
 * Consumes the child rows into per-group accumulators, holding at most one
 * chunk of rows at a time. Returns undefined when aborted mid-collection so
 * callers end the row stream silently, matching the buffered path.
 *
 * @param {object} options
 * @param {QueryResults} options.child
 * @param {ExprNode[]} options.groupBy
 * @param {StreamingAggSpec[]} options.specs
 * @param {boolean} options.needsRow
 * @param {ExecuteContext} options.context
 * @returns {Promise<Map<unknown, StreamingGroup> | undefined>}
 */
async function accumulateGroups({ child, groupBy, specs, needsRow, context }) {
  /** @type {Map<unknown, StreamingGroup>} */
  const groups = new Map()
  /** @type {AsyncRow[]} */
  let chunk = []
  for await (const row of child.rows()) {
    chunk.push(row)
    if (chunk.length >= CHUNK_SIZE) {
      await accumulateChunk({ chunk, groupBy, specs, groups, needsRow, context })
      chunk = []
      await yieldToEventLoop()
      if (context.signal?.aborted) return
    }
  }
  if (chunk.length) {
    await accumulateChunk({ chunk, groupBy, specs, groups, needsRow, context })
  }
  context.signal?.throwIfAborted()
  return groups
}

/**
 * Builds a group's output row by substituting the group's finalized
 * aggregate and group key values into the select expressions and evaluating
 * them against the group's representative row (an empty row when no
 * expression needs one).
 *
 * @param {object} options
 * @param {SelectColumn[]} options.selectColumns
 * @param {StreamingAggSpec[]} options.specs
 * @param {Map<ExprNode, number>} options.keyRefs
 * @param {StreamingGroup} options.group
 * @param {ExecuteContext} options.context
 * @returns {{ outputRow: AsyncRow, values: Map<ExprNode, SqlPrimitive> }}
 */
function finalizeGroup({ selectColumns, specs, keyRefs, group, context }) {
  const firstRow = group.firstRow ?? { columns: [], cells: {} }

  /** @type {Map<ExprNode, SqlPrimitive>} */
  const values = new Map()
  for (let s = 0; s < specs.length; s++) {
    values.set(specs[s].node, finalizeAccumulator(specs[s].funcName, group.accumulators[s]))
  }
  for (const [node, keyIndex] of keyRefs) {
    values.set(node, group.keyValues[keyIndex])
  }

  /** @type {string[]} */
  const columns = []
  /** @type {AsyncCells} */
  const cells = {}
  for (const col of selectColumns) {
    if (col.type === 'star') {
      if (group.firstRow) {
        const prefix = col.table ? `${col.table}.` : undefined
        for (const key of firstRow.columns) {
          if (prefix && !key.startsWith(prefix)) continue
          const dotIndex = key.indexOf('.')
          const outputKey = prefix ? key.substring(prefix.length) : dotIndex >= 0 ? key.substring(dotIndex + 1) : key
          columns.push(outputKey)
          cells[outputKey] = firstRow.cells[key]
        }
      }
    } else {
      const alias = col.alias ?? derivedAlias(col.expr)
      const expr = substituteValues(col.expr, values)
      columns.push(alias)
      cells[alias] = () => evaluateExpr({ node: expr, row: firstRow, context })
    }
  }
  /** @type {AsyncRow} */
  const outputRow = { columns, cells }
  return { outputRow, values }
}

/**
 * Builds the row visible to HAVING and grouped ORDER BY: the group's
 * representative columns plus the select output aliases, mirroring the
 * buffered aggregate context row.
 *
 * @param {StreamingGroup} group
 * @param {AsyncRow} outputRow
 * @returns {AsyncRow}
 */
function groupContextRow(group, outputRow) {
  const firstRow = group.firstRow ?? { columns: [], cells: {} }
  return {
    columns: [...firstRow.columns, ...outputRow.columns],
    cells: { ...firstRow.cells, ...outputRow.cells },
  }
}

/**
 * Streaming GROUP BY execution: accumulates aggregates incrementally instead
 * of buffering every input row, then applies HAVING and grouped ORDER BY
 * against the finalized aggregate values.
 *
 * @param {object} options
 * @param {HashAggregateNode} options.plan
 * @param {StreamingAggPlan} options.streaming
 * @param {QueryResults} options.child
 * @param {ExecuteContext} options.context
 * @returns {() => AsyncGenerator<AsyncRow>}
 */
export function streamingHashAggregateRows({ plan, streaming, child, context }) {
  const { specs, keyRefs, needsRow } = streaming
  return async function* () {
    const groups = await accumulateGroups({ child, groupBy: plan.groupBy, specs, needsRow, context })
    if (!groups) return
    const { orderBy, having } = plan

    // Without ORDER BY, groups finalize and yield one at a time so output
    // rows are never all held at once; sorting needs the full set below.
    /** @type {{ row: AsyncRow, exprs: ExprNode[], outputRow: AsyncRow }[] | undefined} */
    const entries = orderBy?.length ? [] : undefined
    for (const group of groups.values()) {
      const { outputRow, values } = finalizeGroup({ selectColumns: plan.columns, specs, keyRefs, group, context })
      if (having) {
        const passes = await evaluateExpr({
          node: substituteValues(having, values),
          row: groupContextRow(group, outputRow),
          context,
        })
        if (!passes) continue
      }
      if (entries && orderBy) {
        entries.push({
          row: groupContextRow(group, outputRow),
          exprs: orderBy.map(term => substituteValues(term.expr, values)),
          outputRow,
        })
      } else {
        yield outputRow
      }
    }

    if (entries && orderBy) {
      // The shared sorter evaluates later ORDER BY terms only within ties
      // on earlier terms, so expensive sort keys are skipped when possible
      const sorted = await sortEntriesByTerms({ entries, orderBy, context })
      for (const { outputRow } of sorted) {
        yield outputRow
      }
    }
  }
}

/**
 * Streaming scalar aggregate execution: the whole input is one group,
 * accumulated incrementally with bounded memory.
 *
 * @param {object} options
 * @param {ScalarAggregateNode} options.plan
 * @param {StreamingAggPlan} options.streaming
 * @param {QueryResults} options.child
 * @param {ExecuteContext} options.context
 * @returns {() => AsyncGenerator<AsyncRow>}
 */
export function streamingScalarAggregateRows({ plan, streaming, child, context }) {
  const { specs, keyRefs, needsRow } = streaming
  return async function* () {
    const groups = await accumulateGroups({ child, groupBy: [], specs, needsRow, context })
    if (!groups) return
    /** @type {StreamingGroup} */
    const group = groups.get(true) ?? { firstRow: undefined, keyValues: [], accumulators: specs.map(spec => newAccumulator(spec.funcName, spec.node.distinct)) }

    const { outputRow, values } = finalizeGroup({ selectColumns: plan.columns, specs, keyRefs, group, context })
    if (plan.having) {
      const passes = await evaluateExpr({
        node: substituteValues(plan.having, values),
        row: groupContextRow(group, outputRow),
        context,
      })
      if (!passes) return
    }
    yield outputRow
  }
}
