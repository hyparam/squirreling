import type { ColumnReadRequest, ColumnResult, ColumnVector, SqlPrimitive } from './types.js'

export interface CompiledBatchExpression {
  evaluate(context: ColumnReadRequest): ColumnResult
}

export type ValueKernel = (
  vectors: ColumnVector[],
  rowIndex: number,
  streamRowIndex: number,
) => SqlPrimitive

export interface CompileState {
  dependencies: number[]
  dependencyPositions: Map<number, number>
  columns: readonly string[]
}

export interface BatchAggregateInputs {
  keys: CompiledBatchExpression[]
  filters: (CompiledBatchExpression | undefined)[]
  args: (CompiledBatchExpression | undefined)[]
}

export type BatchProjection =
  | { type: 'column', columnIndex: number }
  | { type: 'constant', value: SqlPrimitive }
  | { type: 'expression', expression: CompiledBatchExpression }
