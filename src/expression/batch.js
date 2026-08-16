import { composeSelections, readBatchColumn, selectVector, selectedRowCount, valueAt } from '../backend/batch.js'
import { isPlainObject, sqlEquals } from '../execute/utils.js'
import { yieldToEventLoop } from '../execute/yield.js'
import { isStringFunc } from '../validation/functions.js'
import { ColumnNotFoundError } from '../validation/tables.js'
import { applyBinaryOp } from './binary.js'
import { applyCast, evaluateJsonExtract } from './scalar.js'
import { evaluateStringFunc } from './strings.js'

/**
 * @import { CompiledBatchExpression, CompileState, ValueKernel } from '../internalTypes.js'
 * @import { AsyncBatch, ColumnReadRequest, ColumnResult, ColumnVector, ExprNode, FunctionNode, RowSelection, SqlPrimitive } from '../types.js'
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
 * @param {ExprNode} expression
 * @param {readonly string[]} columns
 * @returns {CompiledBatchExpression | undefined}
 */
export function compileBatchExpression(expression, columns) {
  return compileEvaluator(expression, columns)
}

/**
 * Compiles an expression as one vector kernel when every dependency may be
 * read eagerly. Lazy control flow falls back to selection-aware evaluators
 * that only read columns for rows whose branch is reached.
 *
 * @param {ExprNode} node
 * @param {readonly string[]} columns
 * @returns {CompiledBatchExpression | undefined}
 */
function compileEvaluator(node, columns) {
  const kernel = compileKernelEvaluator(node, columns)
  if (kernel) return kernel

  if (node.type === 'unary') {
    const argument = compileEvaluator(node.argument, columns)
    if (!argument) return undefined
    return {
      async evaluate(context) {
        const vector = await argument.evaluate(context)
        return evaluateValues(context, function unaryValue(rowIndex) {
          const value = valueAt(vector, rowIndex)
          if (node.op === '-') return value == null ? null : -value
          if (node.op === 'NOT') return value == null ? null : !value
          if (node.op === 'IS NULL') return value == null
          return value != null
        })
      },
    }
  }

  if (node.type === 'binary') {
    if (node.left.type === 'interval' || node.right.type === 'interval') return undefined
    const left = compileEvaluator(node.left, columns)
    const right = compileEvaluator(node.right, columns)
    if (!left || !right) return undefined
    if (node.op === 'AND' || node.op === 'OR') {
      const operator = node.op
      return {
        evaluate(context) {
          return evaluateLogical(operator, left, right, context)
        },
      }
    }
    return {
      async evaluate(context) {
        const [leftVector, rightVector] = await Promise.all([
          left.evaluate(context),
          right.evaluate(context),
        ])
        return evaluateValues(context, function binaryValue(rowIndex) {
          return applyBinaryOp(node.op, valueAt(leftVector, rowIndex), valueAt(rightVector, rowIndex))
        })
      },
    }
  }

  if (node.type === 'cast') {
    const argument = compileEvaluator(node.expr, columns)
    if (!argument) return undefined
    return {
      async evaluate(context) {
        const vector = await argument.evaluate(context)
        return evaluateValues(context, function castValue(rowIndex, streamRowIndex) {
          return applyCast(node, valueAt(vector, rowIndex), streamRowIndex + 1)
        })
      },
    }
  }

  if (node.type === 'function') return compileFunctionEvaluator(node, columns)
  if (node.type === 'case') return compileCaseEvaluator(node, columns)
  return undefined
}

/**
 * @param {ExprNode} node
 * @param {readonly string[]} columns
 * @returns {CompiledBatchExpression | undefined}
 */
function compileKernelEvaluator(node, columns) {
  /** @type {CompileState} */
  const state = {
    columns,
    dependencies: [],
    dependencyPositions: new Map(),
  }
  const kernel = compileValueKernel(node, state)
  if (!kernel) return undefined
  const { dependencies } = state
  return {
    evaluate(context) {
      const { batch, selection, signal, rowOffset = 0, rowOrdinals } = context
      signal?.throwIfAborted()
      const results = dependencies.map(function readDependency(columnIndex) {
        return readBatchColumn({ batch, columnIndex, selection, signal })
      })
      const vectors = resolveVectors(results)
      if (vectors instanceof Promise) {
        return vectors.then(function evaluateResolved(resolved) {
          signal?.throwIfAborted()
          return evaluateKernel(kernel, resolved, selection, signal, rowOffset, rowOrdinals)
        })
      }
      return evaluateKernel(kernel, vectors, selection, signal, rowOffset, rowOrdinals)
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
    const accesses = resolveIdentifier(node, state.columns)
    if (!accesses) return undefined
    const dependencyPositions = accesses.map(function registerAccess(access) {
      let dependencyPosition = state.dependencyPositions.get(access.columnIndex)
      if (dependencyPosition === undefined) {
        dependencyPosition = state.dependencies.length
        state.dependencies.push(access.columnIndex)
        state.dependencyPositions.set(access.columnIndex, dependencyPosition)
      }
      return dependencyPosition
    })
    return function identifierValue(vectors, rowIndex, streamRowIndex) {
      for (let index = 0; index < accesses.length; index++) {
        const access = accesses[index]
        const value = valueAt(vectors[dependencyPositions[index]], rowIndex)
        if (!access.field) return value
        if (isPlainObject(value) && Object.prototype.hasOwnProperty.call(value, access.field)) {
          return value[access.field]
        }
      }
      throw new ColumnNotFoundError({
        missingColumn: `${node.prefix}.${node.name}`,
        availableColumns: [...state.columns],
        rowIndex: streamRowIndex + 1,
        ...node,
      })
    }
  }

  if (node.type === 'unary') {
    const argument = compileValueKernel(node.argument, state)
    if (!argument) return undefined
    return function unaryValue(vectors, rowIndex, streamRowIndex) {
      const value = argument(vectors, rowIndex, streamRowIndex)
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
    return function binaryValue(vectors, rowIndex, streamRowIndex) {
      const leftValue = left(vectors, rowIndex, streamRowIndex)
      if (node.op === 'AND' && leftValue != null && !leftValue) return false
      if (node.op === 'OR' && leftValue != null && Boolean(leftValue)) return true
      const rightValue = right(vectors, rowIndex, streamRowIndex)
      return applyBinaryOp(node.op, leftValue, rightValue)
    }
  }

  if (node.type === 'cast') {
    const argument = compileValueKernel(node.expr, state)
    if (!argument) return undefined
    return function castValue(vectors, rowIndex, streamRowIndex) {
      return applyCast(node, argument(vectors, rowIndex, streamRowIndex), streamRowIndex + 1)
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
    return function coalesceValue(vectors, rowIndex, streamRowIndex) {
      for (const argument of arguments_) {
        const value = argument(vectors, rowIndex, streamRowIndex)
        if (value != null) return value
      }
      return null
    }
  }
  if (funcName === 'NULLIF') {
    return function nullIfValue(vectors, rowIndex, streamRowIndex) {
      const first = arguments_[0](vectors, rowIndex, streamRowIndex)
      const second = arguments_[1](vectors, rowIndex, streamRowIndex)
      return sqlEquals(first, second) ? null : first
    }
  }
  if (funcName === 'JSON_VALUE' || funcName === 'JSON_QUERY' || funcName === 'JSON_EXTRACT' || funcName === 'JSON_EXTRACT_STRING') {
    return function jsonValue(vectors, rowIndex, streamRowIndex) {
      const args = arguments_.map(function argumentValue(argument) {
        return argument(vectors, rowIndex, streamRowIndex)
      })
      return evaluateJsonExtract({ funcName, node, args, rowIndex: streamRowIndex + 1 })
    }
  }
  if (!isStringFunc(funcName)) return undefined
  return function stringFunctionValue(vectors, rowIndex, streamRowIndex) {
    const args = arguments_.map(function argumentValue(argument) {
      return argument(vectors, rowIndex, streamRowIndex)
    })
    return evaluateStringFunc({ funcName, node, args, rowIndex: streamRowIndex + 1 })
  }
}

/**
 * @param {FunctionNode} node
 * @param {readonly string[]} columns
 * @returns {CompiledBatchExpression | undefined}
 */
function compileFunctionEvaluator(node, columns) {
  const funcName = node.funcName.toUpperCase()
  if (node.distinct || node.filter) return undefined
  /** @type {CompiledBatchExpression[]} */
  const arguments_ = []
  for (const argumentNode of node.args) {
    const argument = compileEvaluator(argumentNode, columns)
    if (!argument) return undefined
    arguments_.push(argument)
  }
  if (funcName === 'COALESCE') {
    return {
      evaluate(context) {
        return evaluateCoalesce(arguments_, context)
      },
    }
  }
  if (funcName !== 'NULLIF' && funcName !== 'JSON_VALUE' && funcName !== 'JSON_QUERY' &&
    funcName !== 'JSON_EXTRACT' && funcName !== 'JSON_EXTRACT_STRING' && !isStringFunc(funcName)) return undefined
  return {
    async evaluate(context) {
      const vectors = await Promise.all(arguments_.map(function evaluateArgument(argument) {
        return argument.evaluate(context)
      }))
      return evaluateValues(context, function functionValue(rowIndex, streamRowIndex) {
        const args = vectors.map(function argumentValue(vector) { return valueAt(vector, rowIndex) })
        if (funcName === 'NULLIF') return sqlEquals(args[0], args[1]) ? null : args[0]
        if (funcName === 'JSON_VALUE' || funcName === 'JSON_QUERY' || funcName === 'JSON_EXTRACT' || funcName === 'JSON_EXTRACT_STRING') {
          return evaluateJsonExtract({ funcName, node, args, rowIndex: streamRowIndex + 1 })
        }
        return evaluateStringFunc({ funcName, node, args, rowIndex: streamRowIndex + 1 })
      })
    },
  }
}

/**
 * @param {import('../types.js').CaseNode} node
 * @param {readonly string[]} columns
 * @returns {CompiledBatchExpression | undefined}
 */
function compileCaseEvaluator(node, columns) {
  const caseExpression = node.caseExpr ? compileEvaluator(node.caseExpr, columns) : undefined
  if (node.caseExpr && !caseExpression) return undefined
  /** @type {{ condition: CompiledBatchExpression, result: CompiledBatchExpression }[]} */
  const clauses = []
  for (const clause of node.whenClauses) {
    const condition = compileEvaluator(clause.condition, columns)
    const result = compileEvaluator(clause.result, columns)
    if (!condition || !result) return undefined
    clauses.push({ condition, result })
  }
  const elseResult = node.elseResult ? compileEvaluator(node.elseResult, columns) : undefined
  if (node.elseResult && !elseResult) return undefined
  return {
    evaluate(context) {
      return evaluateCase(caseExpression, clauses, elseResult, context)
    },
  }
}

/**
 * Evaluates the right side only for rows whose left side does not determine
 * the logical result. The resulting subset is also passed to deferred source
 * and computed columns, preserving row evaluator short-circuit behavior.
 *
 * @param {'AND' | 'OR'} operator
 * @param {CompiledBatchExpression} left
 * @param {CompiledBatchExpression} right
 * @param {ColumnReadRequest} context
 * @returns {Promise<ColumnVector>}
 */
async function evaluateLogical(operator, left, right, context) {
  const leftVector = await left.evaluate(context)
  const length = selectedRowCount(context.selection)
  /** @type {SqlPrimitive[]} */
  const values = new Array(length)
  const needed = new Uint32Array(length)
  let neededCount = 0
  await visitRows(length, context.signal, function classifyLeft(rowIndex) {
    const value = valueAt(leftVector, rowIndex)
    const decided = operator === 'AND'
      ? value != null && !value
      : value != null && Boolean(value)
    if (decided) {
      values[rowIndex] = operator === 'AND' ? false : true
    } else {
      needed[neededCount++] = rowIndex
    }
  })
  if (neededCount === 0) return { type: 'values', values, length }

  const neededIndices = needed.subarray(0, neededCount)
  const rightVector = await right.evaluate(subsetContext(context, neededIndices))
  await visitRows(neededCount, context.signal, function combineRight(subsetIndex) {
    const rowIndex = neededIndices[subsetIndex]
    values[rowIndex] = applyBinaryOp(
      operator,
      valueAt(leftVector, rowIndex),
      valueAt(rightVector, subsetIndex)
    )
  })
  return { type: 'values', values, length }
}

/**
 * @param {CompiledBatchExpression | undefined} caseExpression
 * @param {{ condition: CompiledBatchExpression, result: CompiledBatchExpression }[]} clauses
 * @param {CompiledBatchExpression | undefined} elseResult
 * @param {ColumnReadRequest} context
 * @returns {Promise<ColumnVector>}
 */
async function evaluateCase(caseExpression, clauses, elseResult, context) {
  const length = selectedRowCount(context.selection)
  /** @type {SqlPrimitive[]} */
  const values = new Array(length)
  const caseVector = caseExpression ? await caseExpression.evaluate(context) : undefined
  let remaining = allIndices(length)

  for (const clause of clauses) {
    if (remaining.length === 0) break
    const remainingContext = subsetContext(context, remaining)
    const conditionVector = await clause.condition.evaluate(remainingContext)
    const matched = new Uint32Array(remaining.length)
    const unmatched = new Uint32Array(remaining.length)
    let matchedCount = 0
    let unmatchedCount = 0
    await visitRows(remaining.length, context.signal, function classifyCondition(subsetIndex) {
      const rowIndex = remaining[subsetIndex]
      const condition = valueAt(conditionVector, subsetIndex)
      const matches = caseVector
        ? sqlEquals(valueAt(caseVector, rowIndex), condition)
        : Boolean(condition)
      if (matches) matched[matchedCount++] = rowIndex
      else unmatched[unmatchedCount++] = rowIndex
    })

    if (matchedCount > 0) {
      const matchedIndices = matched.subarray(0, matchedCount)
      const resultVector = await clause.result.evaluate(subsetContext(context, matchedIndices))
      await visitRows(matchedCount, context.signal, function scatterResult(subsetIndex) {
        values[matchedIndices[subsetIndex]] = valueAt(resultVector, subsetIndex)
      })
    }
    remaining = unmatched.subarray(0, unmatchedCount)
  }

  if (remaining.length > 0 && elseResult) {
    const resultVector = await elseResult.evaluate(subsetContext(context, remaining))
    await visitRows(remaining.length, context.signal, function scatterElse(subsetIndex) {
      values[remaining[subsetIndex]] = valueAt(resultVector, subsetIndex)
    })
  } else {
    for (const rowIndex of remaining) values[rowIndex] = null
  }
  return { type: 'values', values, length }
}

/**
 * Evaluates each argument only for rows that remained null after the previous
 * argument, matching COALESCE's lazy row semantics.
 *
 * @param {CompiledBatchExpression[]} arguments_
 * @param {ColumnReadRequest} context
 * @returns {Promise<ColumnVector>}
 */
async function evaluateCoalesce(arguments_, context) {
  const length = selectedRowCount(context.selection)
  /** @type {SqlPrimitive[]} */
  const values = new Array(length)
  let remaining = allIndices(length)
  for (const argument of arguments_) {
    if (remaining.length === 0) break
    const vector = await argument.evaluate(subsetContext(context, remaining))
    const nullRows = new Uint32Array(remaining.length)
    let nullCount = 0
    await visitRows(remaining.length, context.signal, function chooseValue(subsetIndex) {
      const rowIndex = remaining[subsetIndex]
      const value = valueAt(vector, subsetIndex)
      if (value == null) nullRows[nullCount++] = rowIndex
      else values[rowIndex] = value
    })
    remaining = nullRows.subarray(0, nullCount)
  }
  for (const rowIndex of remaining) values[rowIndex] = null
  return { type: 'values', values, length }
}

/**
 * @param {ColumnReadRequest} context
 * @param {Uint32Array} indices - positions in the context's selected rows
 * @returns {ColumnReadRequest}
 */
function subsetContext(context, indices) {
  const length = selectedRowCount(context.selection)
  const selection = composeSelections(context.selection, {
    type: 'indices',
    indices,
    length,
  })
  /** @type {ColumnVector} */
  const rowOrdinals = context.rowOrdinals
    ? selectVector(context.rowOrdinals, { type: 'indices', indices, length })
    : { type: 'typed', values: indices, length: indices.length }
  return { ...context, selection, rowOrdinals }
}

/**
 * @param {number} length
 * @returns {Uint32Array}
 */
function allIndices(length) {
  const indices = new Uint32Array(length)
  for (let index = 0; index < length; index++) indices[index] = index
  return indices
}

/**
 * @param {ColumnReadRequest} context
 * @param {(rowIndex: number, streamRowIndex: number) => SqlPrimitive} evaluate
 * @returns {Promise<ColumnVector>}
 */
async function evaluateValues(context, evaluate) {
  const length = selectedRowCount(context.selection)
  /** @type {SqlPrimitive[]} */
  const values = new Array(length)
  await visitRows(length, context.signal, function evaluateRow(rowIndex) {
    values[rowIndex] = evaluate(rowIndex, streamRowIndex(context, rowIndex))
  })
  return { type: 'values', values, length }
}

/**
 * @param {number} length
 * @param {AbortSignal | undefined} signal
 * @param {(rowIndex: number) => void} visit
 * @returns {Promise<void>}
 */
async function visitRows(length, signal, visit) {
  for (let start = 0; start < length; start += YIELD_INTERVAL) {
    if (signal && start > 0) await yieldToEventLoop()
    signal?.throwIfAborted()
    const end = Math.min(start + YIELD_INTERVAL, length)
    for (let rowIndex = start; rowIndex < end; rowIndex++) visit(rowIndex)
  }
}

/**
 * @param {ColumnReadRequest} context
 * @param {number} rowIndex
 * @returns {number}
 */
function streamRowIndex(context, rowIndex) {
  const ordinal = context.rowOrdinals ? Number(valueAt(context.rowOrdinals, rowIndex)) : rowIndex
  return (context.rowOffset ?? 0) + ordinal
}

/**
 * @param {import('../types.js').IdentifierNode} identifier
 * @param {readonly string[]} columns
 * @returns {{ columnIndex: number, field?: string }[] | undefined}
 */
function resolveIdentifier(identifier, columns) {
  const sourceName = identifier.prefix
    ? `${identifier.prefix}.${identifier.name}`
    : identifier.name
  const exact = columns.indexOf(sourceName)
  if (exact >= 0) return [{ columnIndex: exact }]

  if (identifier.prefix) {
    const prefix = `${identifier.prefix}.`
    const prefixedMatches = []
    const baseMatches = []
    const baseSuffix = `.${identifier.prefix}`
    for (let index = 0; index < columns.length; index++) {
      const fieldName = columns[index]
      if (fieldName.startsWith(prefix)) prefixedMatches.push(index)
      if (fieldName === identifier.prefix || fieldName.endsWith(baseSuffix)) baseMatches.push(index)
    }

    /** @type {{ columnIndex: number, field?: string }[]} */
    const accesses = []
    if (prefixedMatches.length === 1) {
      accesses.push({ columnIndex: prefixedMatches[0], field: identifier.name })
    }
    if (baseMatches.length === 1) {
      accesses.push({ columnIndex: baseMatches[0], field: identifier.name })
    }
    const bare = columns.indexOf(identifier.name)
    if (bare >= 0) accesses.push({ columnIndex: bare })
    return accesses.length > 0 ? accesses : undefined
  }

  const suffix = `.${identifier.name}`
  const matches = []
  for (let i = 0; i < columns.length; i++) {
    if (columns[i].endsWith(suffix)) matches.push(i)
  }
  return matches.length === 1 ? [{ columnIndex: matches[0] }] : undefined
}

/**
 * @param {ValueKernel} kernel
 * @param {ColumnVector[]} vectors
 * @param {RowSelection} selection
 * @param {AbortSignal} [signal]
 * @param {number} [rowOffset]
 * @param {ColumnVector} [rowOrdinals]
 * @returns {ColumnResult}
 */
function evaluateKernel(kernel, vectors, selection, signal, rowOffset = 0, rowOrdinals) {
  const length = selectedRowCount(selection)
  if (signal && length > YIELD_INTERVAL) {
    return evaluateKernelAsync(kernel, vectors, length, signal, rowOffset, rowOrdinals)
  }
  /** @type {SqlPrimitive[]} */
  const values = new Array(length)
  for (let rowIndex = 0; rowIndex < length; rowIndex++) {
    if (rowIndex % YIELD_INTERVAL === 0) signal?.throwIfAborted()
    const ordinal = rowOrdinals ? Number(valueAt(rowOrdinals, rowIndex)) : rowIndex
    values[rowIndex] = kernel(vectors, rowIndex, rowOffset + ordinal)
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
 * @param {ColumnVector} [rowOrdinals]
 * @returns {Promise<ColumnVector>}
 */
async function evaluateKernelAsync(kernel, vectors, length, signal, rowOffset, rowOrdinals) {
  /** @type {SqlPrimitive[]} */
  const values = new Array(length)
  for (let start = 0; start < length; start += YIELD_INTERVAL) {
    if (start > 0) await yieldToEventLoop()
    signal.throwIfAborted()
    const end = Math.min(start + YIELD_INTERVAL, length)
    for (let rowIndex = start; rowIndex < end; rowIndex++) {
      const ordinal = rowOrdinals ? Number(valueAt(rowOrdinals, rowIndex)) : rowIndex
      values[rowIndex] = kernel(vectors, rowIndex, rowOffset + ordinal)
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
