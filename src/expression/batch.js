import { readBatchColumn, selectedRowCount, valueAt } from '../backend/batch.js'
import { isStringFunc } from '../validation/functions.js'
import { applyBinaryOp } from './binary.js'
import { evaluateStringFunc } from './strings.js'

/**
 * @import { ColumnResult, ColumnVector, CompileBatchExpressionOptions, CompiledBatchExpression, CompileState, RelationSchema, RowSelection, ValueKernel } from '../internalTypes.js'
 * @import { ExprNode, FunctionNode, SqlPrimitive } from '../types.js'
 */

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
    evaluate({ batch, selection, signal }) {
      signal?.throwIfAborted()
      const results = dependencies.map(function readDependency(columnIndex) {
        return readBatchColumn({ batch, columnIndex, selection, signal })
      })
      const vectors = resolveVectors(results)
      if (vectors instanceof Promise) {
        return vectors.then(function evaluateResolved(resolved) {
          signal?.throwIfAborted()
          return evaluateKernel(kernel, resolved, selection, signal)
        })
      }
      return evaluateKernel(kernel, vectors, selection, signal)
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
    return function unaryValue(vectors, rowIndex) {
      const value = argument(vectors, rowIndex)
      if (node.op === '-') return value == null ? null : -value
      if (node.op === 'NOT') return value == null ? null : !value
      if (node.op === 'IS NULL') return value == null
      return value != null
    }
  }

  if (node.type === 'binary') {
    if (node.left.type === 'interval' || node.right.type === 'interval') return undefined
    const left = compileValueKernel(node.left, state)
    const right = compileValueKernel(node.right, state)
    if (!left || !right) return undefined
    return function binaryValue(vectors, rowIndex) {
      const leftValue = left(vectors, rowIndex)
      if (node.op === 'AND' && leftValue != null && !leftValue) return false
      if (node.op === 'OR' && leftValue != null && Boolean(leftValue)) return true
      const rightValue = right(vectors, rowIndex)
      return applyBinaryOp(node.op, leftValue, rightValue)
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
  if (!isStringFunc(funcName) || node.distinct || node.filter) return undefined
  /** @type {ValueKernel[]} */
  const arguments_ = []
  for (const argumentNode of node.args) {
    const argument = compileValueKernel(argumentNode, state)
    if (!argument) return undefined
    arguments_.push(argument)
  }
  return function stringFunctionValue(vectors, rowIndex) {
    const args = arguments_.map(function argumentValue(argument) {
      return argument(vectors, rowIndex)
    })
    return evaluateStringFunc({ funcName, node, args, rowIndex: rowIndex + 1 })
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
 * @returns {ColumnVector}
 */
function evaluateKernel(kernel, vectors, selection, signal) {
  const length = selectedRowCount(selection)
  /** @type {SqlPrimitive[]} */
  const values = new Array(length)
  for (let rowIndex = 0; rowIndex < length; rowIndex++) {
    if (rowIndex % 4000 === 0) signal?.throwIfAborted()
    values[rowIndex] = kernel(vectors, rowIndex)
  }
  return { type: 'values', values, length }
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
