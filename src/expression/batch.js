import { readBatchColumn, selectedRowCount, valueAt } from '../backend/batch.js'
import { yieldToEventLoop } from '../execute/yield.js'
import { isStringFunc } from '../validation/functions.js'
import { applyBinaryOp } from './binary.js'
import { applyCast, evaluateJsonExtract } from './scalar.js'
import { evaluateStringFunc } from './strings.js'

/**
 * @import { ColumnResult, ColumnVector, CompileBatchExpressionOptions, CompiledBatchExpression, CompileState, RelationSchema, RowSelection, ValueKernel } from '../internalTypes.js'
 * @import { ExprNode, FunctionNode, SqlPrimitive } from '../types.js'
 */

const YIELD_INTERVAL = 4000

/**
 * Compiles a supported scalar expression into one column-level evaluation.
 * Dependencies resolve at most once per batch selection; the synchronous
 * kernel then evaluates every selected row without per-cell promises.
 *
 * Unsupported expressions return `undefined` so an operator can retain its
 * existing row evaluator without changing semantics.
 *
 * @param {CompileBatchExpressionOptions} options
 * @returns {CompiledBatchExpression | undefined}
 */
export function compileBatchExpression({ expression, schema }) {
  /** @type {CompileState} */
  const state = {
    schema,
    dependencies: [],
    dependencyPositions: new Map(),
  }
  const kernel = compileValueKernel(expression, state)
  if (!kernel) return undefined
  const { dependencies } = state

  return {
    dependencies,
    evaluate({ batch, selection, signal, rowOffset = 0 }) {
      signal?.throwIfAborted()
      const results = dependencies.map(function readDependency(columnIndex) {
        return readBatchColumn({ batch, columnIndex, selection, signal })
      })
      const vectors = resolveVectors(results)
      if (vectors instanceof Promise) {
        return vectors.then(function evaluateResolved(resolved) {
          signal?.throwIfAborted()
          return evaluateKernel(kernel, resolved, selection, signal, rowOffset)
        })
      }
      return evaluateKernel(kernel, vectors, selection, signal, rowOffset)
    },
  }
}

/**
 * @param {ExprNode} node
 * @param {CompileState} state
 * @returns {ValueKernel | undefined}
 */
function compileValueKernel(node, state) {
  if (node.type === 'literal') {
    return function literalValue() { return node.value }
  }

  if (node.type === 'identifier') {
    const columnIndex = resolveIdentifier(node, state.schema)
    if (columnIndex === undefined) return undefined
    let dependencyPosition = state.dependencyPositions.get(columnIndex)
    if (dependencyPosition === undefined) {
      dependencyPosition = state.dependencies.length
      state.dependencies.push(columnIndex)
      state.dependencyPositions.set(columnIndex, dependencyPosition)
    }
    return function identifierValue(vectors, rowIndex) {
      return valueAt(vectors[dependencyPosition], rowIndex)
    }
  }

  if (node.type === 'unary') {
    const argument = compileValueKernel(node.argument, state)
    if (!argument) return undefined
    return function unaryValue(vectors, rowIndex, rowOffset) {
      const value = argument(vectors, rowIndex, rowOffset)
      if (node.op === '-') return value == null ? null : -value
      if (node.op === 'NOT') return value == null ? null : !value
      if (node.op === 'IS NULL') return value == null
      return value != null
    }
  }

  if (node.type === 'binary') {
    if (node.left.type === 'interval' || node.right.type === 'interval') return undefined
    if ((node.op === 'AND' || node.op === 'OR') && readsIdentifier(node.right)) return undefined
    const left = compileValueKernel(node.left, state)
    const right = compileValueKernel(node.right, state)
    if (!left || !right) return undefined
    return function binaryValue(vectors, rowIndex, rowOffset) {
      const leftValue = left(vectors, rowIndex, rowOffset)
      if (node.op === 'AND' && leftValue != null && !leftValue) return false
      if (node.op === 'OR' && leftValue != null && Boolean(leftValue)) return true
      const rightValue = right(vectors, rowIndex, rowOffset)
      return applyBinaryOp(node.op, leftValue, rightValue)
    }
  }

  if (node.type === 'cast') {
    const argument = compileValueKernel(node.expr, state)
    if (!argument) return undefined
    return function castValue(vectors, rowIndex, rowOffset) {
      return applyCast(node, argument(vectors, rowIndex, rowOffset), rowOffset + rowIndex + 1)
    }
  }

  if (node.type === 'function') {
    return compileFunctionKernel(node, state)
  }

  return undefined
}

/**
 * @param {FunctionNode} node
 * @param {CompileState} state
 * @returns {ValueKernel | undefined}
 */
function compileFunctionKernel(node, state) {
  const funcName = node.funcName.toUpperCase()
  if (node.distinct || node.filter) return undefined
  if (funcName === 'COALESCE' && node.args.slice(1).some(readsIdentifier)) return undefined
  /** @type {ValueKernel[]} */
  const arguments_ = []
  for (const argumentNode of node.args) {
    const argument = compileValueKernel(argumentNode, state)
    if (!argument) return undefined
    arguments_.push(argument)
  }
  if (funcName === 'COALESCE') {
    return function coalesceValue(vectors, rowIndex, rowOffset) {
      for (const argument of arguments_) {
        const value = argument(vectors, rowIndex, rowOffset)
        if (value != null) return value
      }
      return null
    }
  }
  if (funcName === 'JSON_VALUE' || funcName === 'JSON_QUERY' || funcName === 'JSON_EXTRACT' || funcName === 'JSON_EXTRACT_STRING') {
    return function jsonValue(vectors, rowIndex, rowOffset) {
      const args = arguments_.map(function argumentValue(argument) {
        return argument(vectors, rowIndex, rowOffset)
      })
      return evaluateJsonExtract({ funcName, node, args, rowIndex: rowOffset + rowIndex + 1 })
    }
  }
  if (!isStringFunc(funcName)) return undefined
  return function stringFunctionValue(vectors, rowIndex, rowOffset) {
    const args = arguments_.map(function argumentValue(argument) {
      return argument(vectors, rowIndex, rowOffset)
    })
    return evaluateStringFunc({ funcName, node, args, rowIndex: rowOffset + rowIndex + 1 })
  }
}

/**
 * @param {import('../types.js').IdentifierNode} identifier
 * @param {RelationSchema} schema
 * @returns {number | undefined}
 */
function resolveIdentifier(identifier, schema) {
  const sourceName = identifier.prefix
    ? `${identifier.prefix}.${identifier.name}`
    : identifier.name
  const exact = schema.fields.findIndex(function exactName(field) {
    return field.name === sourceName
  })
  if (exact >= 0) return exact

  if (identifier.prefix) {
    const bareMatches = []
    for (let index = 0; index < schema.fields.length; index++) {
      if (schema.fields[index].name === identifier.name) bareMatches.push(index)
    }
    if (bareMatches.length === 1) return bareMatches[0]
    if (bareMatches.length > 1) return undefined
  }

  const suffix = `.${identifier.name}`
  const matches = []
  for (let i = 0; i < schema.fields.length; i++) {
    if (schema.fields[i].name.endsWith(suffix)) matches.push(i)
  }
  return matches.length === 1 ? matches[0] : undefined
}

/**
 * @param {ValueKernel} kernel
 * @param {ColumnVector[]} vectors
 * @param {RowSelection} selection
 * @param {AbortSignal} [signal]
 * @param {number} [rowOffset]
 * @returns {ColumnResult}
 */
function evaluateKernel(kernel, vectors, selection, signal, rowOffset = 0) {
  const length = selectedRowCount(selection)
  if (signal && length > YIELD_INTERVAL) {
    return evaluateKernelAsync(kernel, vectors, length, signal, rowOffset)
  }
  /** @type {SqlPrimitive[]} */
  const values = new Array(length)
  for (let rowIndex = 0; rowIndex < length; rowIndex++) {
    if (rowIndex % YIELD_INTERVAL === 0) signal?.throwIfAborted()
    values[rowIndex] = kernel(vectors, rowIndex, rowOffset)
  }
  return { type: 'values', values, length }
}

/**
 * Evaluates a large kernel in macrotask-sized chunks so timer-based aborts
 * can fire while a native batch is being processed.
 *
 * @param {ValueKernel} kernel
 * @param {ColumnVector[]} vectors
 * @param {number} length
 * @param {AbortSignal} signal
 * @param {number} rowOffset
 * @returns {Promise<ColumnVector>}
 */
async function evaluateKernelAsync(kernel, vectors, length, signal, rowOffset) {
  /** @type {SqlPrimitive[]} */
  const values = new Array(length)
  for (let start = 0; start < length; start += YIELD_INTERVAL) {
    if (start > 0) await yieldToEventLoop()
    signal.throwIfAborted()
    const end = Math.min(start + YIELD_INTERVAL, length)
    for (let rowIndex = start; rowIndex < end; rowIndex++) {
      values[rowIndex] = kernel(vectors, rowIndex, rowOffset)
    }
  }
  return { type: 'values', values, length }
}

/**
 * Returns whether a supported expression subtree can trigger a column read.
 *
 * @param {ExprNode} node
 * @returns {boolean}
 */
function readsIdentifier(node) {
  if (node.type === 'identifier') return true
  if (node.type === 'unary') return readsIdentifier(node.argument)
  if (node.type === 'binary') return readsIdentifier(node.left) || readsIdentifier(node.right)
  if (node.type === 'cast') return readsIdentifier(node.expr)
  if (node.type === 'function') return node.args.some(readsIdentifier)
  return false
}

/**
 * @param {ColumnResult[]} results
 * @returns {ColumnVector[] | Promise<ColumnVector[]>}
 */
function resolveVectors(results) {
  if (results.some(function isPromise(result) { return result instanceof Promise })) {
    return Promise.all(results)
  }
  /** @type {ColumnVector[]} */
  const vectors = []
  for (const result of results) {
    if (result instanceof Promise) throw new Error('Unexpected asynchronous column result')
    vectors.push(result)
  }
  return vectors
}
