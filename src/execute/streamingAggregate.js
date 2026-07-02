import { derivedAlias } from '../expression/alias.js'
import { evaluateAll, evaluateExpr } from '../expression/evaluate.js'
import { isAggregateFunc } from '../validation/functions.js'
import { compareForTerm, keyify } from './utils.js'
import { yieldToEventLoop } from './yield.js'

/**
 * @import { AsyncCells, AsyncRow, ExecuteContext, ExprNode, FunctionNode, QueryResults, SelectColumn, SqlPrimitive } from '../types.js'
 * @import { HashAggregateNode, ScalarAggregateNode } from '../plan/types.js'
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
 *   count: number,
 *   sum: number,
 *   min: SqlPrimitive,
 *   max: SqlPrimitive,
 *   seen: Set<unknown> | null,
 * }} StreamingAccumulator
 */

/**
 * @typedef {{
 *   firstRow: AsyncRow | undefined,
 *   accumulators: StreamingAccumulator[],
 * }} StreamingGroup
 */

/**
 * Extracts the aggregate calls an aggregate node needs so they can be
 * computed incrementally, without buffering the group's rows. Returns
 * undefined when any expression needs a buffered group: an aggregate outside
 * STREAMABLE_FUNCS, an aggregate over a non-scalar argument, or a subquery.
 *
 * @param {Pick<HashAggregateNode, 'columns' | 'having'> & Partial<Pick<HashAggregateNode, 'orderBy'>>} plan
 * @returns {StreamingAggSpec[] | undefined}
 */
export function planStreamingAggregates({ columns, having, orderBy }) {
  /** @type {StreamingAggSpec[]} */
  const specs = []
  for (const col of columns) {
    if (col.type === 'star') continue
    if (!collectAggregates(col.expr, specs)) return
  }
  if (having && !collectAggregates(having, specs)) return
  for (const term of orderBy ?? []) {
    if (!collectAggregates(term.expr, specs)) return
  }
  return specs
}

/**
 * Walks an expression collecting streamable aggregate calls into specs.
 * Returns false if the expression cannot be evaluated from precomputed
 * aggregate values plus a single representative row.
 *
 * @param {ExprNode} node
 * @param {StreamingAggSpec[]} specs
 * @returns {boolean}
 */
function collectAggregates(node, specs) {
  switch (node.type) {
  case 'literal':
  case 'identifier':
  case 'star':
  case 'interval':
    return true
  case 'unary':
    return collectAggregates(node.argument, specs)
  case 'binary':
    return collectAggregates(node.left, specs) && collectAggregates(node.right, specs)
  case 'cast':
    return collectAggregates(node.expr, specs)
  case 'case':
    return (!node.caseExpr || collectAggregates(node.caseExpr, specs)) &&
      node.whenClauses.every(w => collectAggregates(w.condition, specs) && collectAggregates(w.result, specs)) &&
      (!node.elseResult || collectAggregates(node.elseResult, specs))
  case 'in valuelist':
    return collectAggregates(node.expr, specs) && node.values.every(v => collectAggregates(v, specs))
  case 'function': {
    const funcName = node.funcName.toUpperCase()
    if (!isAggregateFunc(funcName)) {
      return node.args.every(arg => collectAggregates(arg, specs))
    }
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
 * Replaces each aggregate call in an expression with its computed value as a
 * literal, so the rest of the expression can be evaluated against a single
 * representative row. Nodes without aggregates are returned unchanged.
 *
 * @param {ExprNode} node
 * @param {Map<FunctionNode, SqlPrimitive>} values - computed value per aggregate node
 * @returns {ExprNode}
 */
function substituteAggregates(node, values) {
  switch (node.type) {
  case 'unary': {
    const argument = substituteAggregates(node.argument, values)
    return argument === node.argument ? node : { ...node, argument }
  }
  case 'binary': {
    const left = substituteAggregates(node.left, values)
    const right = substituteAggregates(node.right, values)
    return left === node.left && right === node.right ? node : { ...node, left, right }
  }
  case 'cast': {
    const expr = substituteAggregates(node.expr, values)
    return expr === node.expr ? node : { ...node, expr }
  }
  case 'case': {
    const caseExpr = node.caseExpr && substituteAggregates(node.caseExpr, values)
    const whenClauses = node.whenClauses.map(w => {
      const condition = substituteAggregates(w.condition, values)
      const result = substituteAggregates(w.result, values)
      return condition === w.condition && result === w.result ? w : { ...w, condition, result }
    })
    const elseResult = node.elseResult && substituteAggregates(node.elseResult, values)
    return { ...node, caseExpr, whenClauses, elseResult }
  }
  case 'in valuelist': {
    const expr = substituteAggregates(node.expr, values)
    const valueNodes = node.values.map(v => substituteAggregates(v, values))
    return { ...node, expr, values: valueNodes }
  }
  case 'function': {
    if (values.has(node)) {
      return {
        type: 'literal',
        value: values.get(node) ?? null,
        positionStart: node.positionStart,
        positionEnd: node.positionEnd,
      }
    }
    const args = node.args.map(arg => substituteAggregates(arg, values))
    return args.every((arg, i) => arg === node.args[i]) ? node : { ...node, args }
  }
  default:
    return node
  }
}

/**
 * @param {StreamingAggSpec} spec
 * @returns {StreamingAccumulator}
 */
function newAccumulator(spec) {
  return {
    count: 0,
    sum: 0,
    min: null,
    max: null,
    seen: spec.funcName === 'COUNT' && spec.node.distinct ? new Set() : null,
  }
}

/**
 * Folds one value into an accumulator, matching the buffered semantics in
 * evaluate.js: COUNT counts non-null, COUNTIF counts truthy, MIN/MAX compare
 * raw values, SUM/AVG only accumulate finite numbers.
 *
 * @param {StreamingAggSpec} spec
 * @param {StreamingAccumulator} acc
 * @param {SqlPrimitive} value
 */
function updateAccumulator(spec, acc, value) {
  switch (spec.funcName) {
  case 'COUNT':
    if (spec.star) acc.count++
    else if (value != null) {
      if (acc.seen) acc.seen.add(keyify(value))
      else acc.count++
    }
    break
  case 'COUNTIF':
    if (value) acc.count++
    break
  default: { // SUM, AVG, MIN, MAX
    if (value == null) break
    if (acc.min === null || value < acc.min) acc.min = value
    if (acc.max === null || value > acc.max) acc.max = value
    const num = Number(value)
    if (Number.isFinite(num)) {
      acc.sum += num
      acc.count++
    }
  }
  }
}

/**
 * @param {StreamingAggSpec} spec
 * @param {StreamingAccumulator} acc
 * @returns {SqlPrimitive}
 */
function finalizeAccumulator(spec, acc) {
  switch (spec.funcName) {
  case 'COUNT': return acc.seen ? acc.seen.size : acc.count
  case 'COUNTIF': return acc.count
  case 'SUM': return acc.count === 0 ? null : acc.sum
  case 'AVG': return acc.count === 0 ? null : acc.sum / acc.count
  case 'MIN': return acc.min
  case 'MAX': return acc.max
  default: return null
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
 * @param {ExecuteContext} options.context
 * @returns {Promise<void>}
 */
async function accumulateChunk({ chunk, groupBy, specs, groups, context }) {
  /** @type {unknown[] | undefined} */
  let keys
  if (groupBy.length === 1) {
    const values = await evaluateAll(groupBy[0], chunk, context)
    keys = values.map(v => keyify(v))
  } else if (groupBy.length > 1) {
    const columns = await Promise.all(groupBy.map(expr => evaluateAll(expr, chunk, context)))
    keys = chunk.map((_, j) => keyify(...columns.map(c => c[j])))
  }

  /** @type {(SqlPrimitive[] | undefined)[]} */
  const filters = new Array(specs.length)
  /** @type {(SqlPrimitive[] | undefined)[]} */
  const args = new Array(specs.length)
  for (let s = 0; s < specs.length; s++) {
    const { node, star } = specs[s]
    filters[s] = node.filter ? await evaluateAll(node.filter, chunk, context) : undefined
    args[s] = star ? undefined : await evaluateAll(node.args[0], chunk, context)
  }

  for (let j = 0; j < chunk.length; j++) {
    const key = keys ? keys[j] : true
    let group = groups.get(key)
    if (!group) {
      group = { firstRow: chunk[j], accumulators: specs.map(spec => newAccumulator(spec)) }
      groups.set(key, group)
    }
    for (let s = 0; s < specs.length; s++) {
      const filter = filters[s]
      if (filter && !filter[j]) continue
      const arg = args[s]
      updateAccumulator(specs[s], group.accumulators[s], arg ? arg[j] : null)
    }
  }
}

/**
 * Consumes the child rows into per-group accumulators, holding at most one
 * chunk of rows at a time.
 *
 * @param {object} options
 * @param {QueryResults} options.child
 * @param {ExprNode[]} options.groupBy
 * @param {StreamingAggSpec[]} options.specs
 * @param {ExecuteContext} options.context
 * @returns {Promise<Map<unknown, StreamingGroup>>}
 */
async function accumulateGroups({ child, groupBy, specs, context }) {
  /** @type {Map<unknown, StreamingGroup>} */
  const groups = new Map()
  /** @type {AsyncRow[]} */
  let chunk = []
  for await (const row of child.rows()) {
    chunk.push(row)
    if (chunk.length >= CHUNK_SIZE) {
      await accumulateChunk({ chunk, groupBy, specs, groups, context })
      chunk = []
      await yieldToEventLoop()
      context.signal?.throwIfAborted()
    }
  }
  if (chunk.length) {
    await accumulateChunk({ chunk, groupBy, specs, groups, context })
  }
  context.signal?.throwIfAborted()
  return groups
}

/**
 * Builds a group's output row and the context row visible to HAVING and
 * grouped ORDER BY, by substituting the group's finalized aggregate values
 * into the select expressions and evaluating them against the group's
 * representative row.
 *
 * @param {object} options
 * @param {SelectColumn[]} options.selectColumns
 * @param {StreamingAggSpec[]} options.specs
 * @param {StreamingGroup} options.group
 * @param {ExecuteContext} options.context
 * @returns {{ contextRow: AsyncRow, outputRow: AsyncRow, values: Map<FunctionNode, SqlPrimitive> }}
 */
function finalizeGroup({ selectColumns, specs, group, context }) {
  const firstRow = group.firstRow ?? { columns: [], cells: {} }

  /** @type {Map<FunctionNode, SqlPrimitive>} */
  const values = new Map()
  for (let s = 0; s < specs.length; s++) {
    values.set(specs[s].node, finalizeAccumulator(specs[s], group.accumulators[s]))
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
      const expr = substituteAggregates(col.expr, values)
      columns.push(alias)
      cells[alias] = () => evaluateExpr({ node: expr, row: firstRow, context })
    }
  }
  /** @type {AsyncRow} */
  const outputRow = { columns, cells }

  // Row visible to HAVING and grouped ORDER BY: the group's columns plus the
  // select output aliases, mirroring the buffered aggregate context row.
  /** @type {AsyncRow} */
  const contextRow = {
    columns: [...firstRow.columns, ...columns],
    cells: { ...firstRow.cells, ...cells },
  }
  return { contextRow, outputRow, values }
}

/**
 * Streaming GROUP BY execution: accumulates aggregates incrementally instead
 * of buffering every input row, then applies HAVING and grouped ORDER BY
 * against the finalized aggregate values.
 *
 * @param {object} options
 * @param {HashAggregateNode} options.plan
 * @param {StreamingAggSpec[]} options.specs
 * @param {QueryResults} options.child
 * @param {ExecuteContext} options.context
 * @returns {() => AsyncGenerator<AsyncRow>}
 */
export function streamingHashAggregateRows({ plan, specs, child, context }) {
  return async function* () {
    const groups = await accumulateGroups({ child, groupBy: plan.groupBy, specs, context })
    const { orderBy } = plan

    /** @type {{ outputRow: AsyncRow, contextRow: AsyncRow, values: Map<FunctionNode, SqlPrimitive>, orderValues: SqlPrimitive[] }[]} */
    const outputRows = []
    for (const group of groups.values()) {
      const { contextRow, outputRow, values } = finalizeGroup({ selectColumns: plan.columns, specs, group, context })
      if (plan.having) {
        const passes = await evaluateExpr({
          node: substituteAggregates(plan.having, values),
          row: contextRow,
          context,
        })
        if (!passes) continue
      }
      outputRows.push({ outputRow, contextRow, values, orderValues: [] })
    }

    if (orderBy?.length) {
      // Evaluate each sort key across all groups in concurrent chunks so
      // async cells and UDFs overlap
      for (let t = 0; t < orderBy.length; t++) {
        const term = orderBy[t]
        for (let start = 0; start < outputRows.length; start += CHUNK_SIZE) {
          if (start > 0) {
            await yieldToEventLoop()
            context.signal?.throwIfAborted()
          }
          const chunk = outputRows.slice(start, start + CHUNK_SIZE)
          const termValues = await Promise.all(chunk.map(entry => evaluateExpr({
            node: substituteAggregates(term.expr, entry.values),
            row: entry.contextRow,
            context,
          })))
          for (let j = 0; j < chunk.length; j++) {
            chunk[j].orderValues[t] = termValues[j]
          }
        }
      }
      outputRows.sort((a, b) => {
        for (let i = 0; i < orderBy.length; i++) {
          const cmp = compareForTerm(a.orderValues[i], b.orderValues[i], orderBy[i])
          if (cmp) return cmp
        }
        return 0
      })
    }

    for (const { outputRow } of outputRows) {
      yield outputRow
    }
  }
}

/**
 * Streaming scalar aggregate execution: the whole input is one group,
 * accumulated incrementally with bounded memory.
 *
 * @param {object} options
 * @param {ScalarAggregateNode} options.plan
 * @param {StreamingAggSpec[]} options.specs
 * @param {QueryResults} options.child
 * @param {ExecuteContext} options.context
 * @returns {() => AsyncGenerator<AsyncRow>}
 */
export function streamingScalarAggregateRows({ plan, specs, child, context }) {
  return async function* () {
    const groups = await accumulateGroups({ child, groupBy: [], specs, context })
    /** @type {StreamingGroup} */
    const group = groups.get(true) ?? { firstRow: undefined, accumulators: specs.map(spec => newAccumulator(spec)) }

    const { contextRow, outputRow, values } = finalizeGroup({ selectColumns: plan.columns, specs, group, context })
    if (plan.having) {
      const passes = await evaluateExpr({
        node: substituteAggregates(plan.having, values),
        row: contextRow,
        context,
      })
      if (!passes) return
    }
    yield outputRow
  }
}
