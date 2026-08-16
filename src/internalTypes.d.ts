import type { AsyncRow, ExprNode, QueryResults, SqlPrimitive } from './types.js'

export type FieldId = number

export type SqlType =
  | { type: 'unknown' }
  | { type: 'string' }
  | { type: 'number' }
  | { type: 'bigint' }
  | { type: 'boolean' }
  | { type: 'date' }
  | { type: 'array', items: SqlType }
  | { type: 'struct', fields: readonly Field[] }

export interface Field {
  id: FieldId
  name: string
  dataType: SqlType
  nullable: boolean
}

export interface RelationSchema {
  fields: readonly Field[]
}

export interface RowRange {
  start: number
  end: number
}

export type RowSelection =
  | { type: 'all', length: number }
  | { type: 'range', start: number, end: number, length: number }
  | { type: 'ranges', ranges: readonly RowRange[], length: number }
  | { type: 'indices', indices: Uint32Array, length: number }
  | { type: 'bitmap', values: Uint8Array, length: number }

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

export interface ColumnEvaluationRequest {
  batch: AsyncBatch
  selection: RowSelection
  signal?: AbortSignal
}

export type EvaluateColumn = (request: ColumnEvaluationRequest) => ColumnResult

export interface CompileBatchExpressionOptions {
  expression: ExprNode
  schema: RelationSchema
}

export interface CompiledBatchExpression {
  dependencies: readonly number[]
  evaluate: EvaluateColumn
}

export type ValueKernel = (vectors: ColumnVector[], rowIndex: number) => SqlPrimitive

export interface CompileState {
  dependencies: number[]
  dependencyPositions: Map<number, number>
  schema: RelationSchema
}

export type BatchColumn =
  | { type: 'loaded', vector: ColumnVector }
  | { type: 'source', read: ReadColumn }
  | { type: 'computed', input: AsyncBatch, evaluate: EvaluateColumn }

export interface AsyncBatch {
  schema: RelationSchema
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
  schema: RelationSchema
  batches(): AsyncIterable<AsyncBatch>
  signal?: AbortSignal
}

export interface RegisteredBatchResults {
  results: QueryResults
  batchResults: InternalBatchResults
}

export type { AsyncRow, SqlPrimitive }
