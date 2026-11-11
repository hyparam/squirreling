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
  // One value slot per aggregate, followed by its key for MIN_BY/MAX_BY.
  args: (CompiledBatchExpression | undefined)[]
}

export type BatchProjection =
  | { type: 'column', columnIndex: number }
  | { type: 'constant', value: SqlPrimitive }
  | { type: 'expression', expression: CompiledBatchExpression }

/** Bounds of a dynamic join filter's keys within one comparison domain. */
export interface KeyRange {
  kind: 'string' | 'number' | 'bigint' | undefined
  min: string | number | bigint | undefined
  max: string | number | bigint | undefined
}
