import type { SqlPrimitive } from './types.js'

export type RowSelection =
  | { type: 'all', length: number }
  | { type: 'range', start: number, end: number, length: number }
  | { type: 'indices', indices: Uint32Array, length: number }

export type NumericArray =
  | Int8Array
  | Uint8Array
  | Uint8ClampedArray
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float32Array
  | Float64Array
  | BigInt64Array
  | BigUint64Array

export type ColumnVector =
  | { type: 'values', values: readonly SqlPrimitive[], length: number }
  | { type: 'typed', values: NumericArray, validity?: Uint8Array, length: number }
  | { type: 'constant', value: SqlPrimitive, length: number }
  | { type: 'selected', source: ColumnVector, selection: RowSelection, length: number }

export interface ColumnReadRequest {
  selection: RowSelection
  signal?: AbortSignal
}

export type ColumnResult = ColumnVector | Promise<ColumnVector>
export type ReadColumn = (request: ColumnReadRequest) => ColumnResult

export interface EvaluationContext {
  batch: AsyncBatch
  selection: RowSelection
  signal?: AbortSignal
  rowOffset?: number
  rowOrdinals?: ColumnVector
}

export interface CompiledBatchExpression {
  evaluate(context: EvaluationContext): ColumnResult
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

export type BatchColumn =
  | ColumnVector
  | { type: 'source', read: ReadColumn }
  | {
      type: 'computed'
      input: AsyncBatch
      expression: CompiledBatchExpression
      rowOffset: number
      rowOrdinals: ColumnVector
    }

export interface AsyncBatch {
  columnNames: string[]
  selection: RowSelection
  columns: readonly BatchColumn[]
}

export interface ReadBatchColumnOptions {
  batch: AsyncBatch
  columnIndex: number
  selection?: RowSelection
  signal?: AbortSignal
}

export interface RowsToBatchesOptions {
  batchRows?: number
  signal?: AbortSignal
}

export interface InternalBatchResults {
  columns: string[]
  batches(): AsyncIterable<AsyncBatch>
  signal?: AbortSignal
}
