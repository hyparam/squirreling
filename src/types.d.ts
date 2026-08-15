import type { ExprNode, SqlPrimitive, Statement } from './ast.js'
import type { QueryPlan } from './plan/types.js'

export * from './ast.js'
export { ParserState, Token, TokenType } from './parse/types.js'
export { QueryPlan } from './plan/types.js'

/**
 * Result of executing a SQL query.
 */
export interface QueryResults {
  columns: string[]
  rows(): AsyncGenerator<AsyncRow>
  /** Native batch output during the row-to-batch executor migration. */
  batches?(): AsyncIterable<AsyncBatch>
  schema?: RelationSchema
  numRows?: number
  maxRows?: number
}

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

/**
 * A selection over a base domain of `length` rows.
 */
export type RowSelection =
  | { type: 'all', length: number }
  | { type: 'range', start: number, end: number, length: number }
  | { type: 'ranges', ranges: readonly RowRange[], length: number }
  | { type: 'indices', indices: Uint32Array, length: number }
  | {
      type: 'bitmap'
      values: Uint8Array
      length: number
    }

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
  | {
      type: 'values'
      values: readonly SqlPrimitive[]
      length: number
    }
  | {
      type: 'typed'
      values: NumericArray
      validity?: Uint8Array
      length: number
    }
  | {
      type: 'constant'
      value: SqlPrimitive
      length: number
    }
  | {
      type: 'selected'
      source: ColumnVector
      selection: RowSelection
      length: number
    }

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
  rowOffset?: number
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

export type BatchProjection =
  | { type: 'column', columnIndex: number }
  | { type: 'constant', value: SqlPrimitive }
  | { type: 'expression', expression: CompiledBatchExpression }

export type BatchColumn =
  | { type: 'loaded', vector: ColumnVector }
  | { type: 'source', read: ReadColumn }
  | {
      type: 'computed'
      input: AsyncBatch
      evaluate: EvaluateColumn
    }

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

export interface ColumnDemand {
  field: FieldId
  phase: number
  purpose: 'filter' | 'output'
  mode: 'required' | 'deferred'
}

export interface ScanRequest {
  columns: readonly ColumnDemand[]
  filter?: ExprNode
  limit?: number
  offset?: number
}

export interface ScanProperties {
  exactRows?: number
  maxRows?: number
}

export interface ScanResidual {
  filter?: ExprNode
  limit?: number
  offset?: number
}

export interface ReadBatchesOptions {
  signal?: AbortSignal
}

export type ReadBatches = (options?: ReadBatchesOptions) => AsyncIterable<AsyncBatch>

export interface PreparedScan {
  schema: RelationSchema
  residual: ScanResidual
  properties: ScanProperties
  batches: ReadBatches
}

export type PrepareScan = (request: ScanRequest) => PreparedScan

// parseSql(options)
export interface ParseSqlOptions {
  query: string
  functions?: Record<string, UserDefinedFunction>
}

// executeSql(options)
export interface ExecuteSqlOptions {
  tables: Record<string, Row | AsyncDataSource>
  query: string | Statement
  functions?: Record<string, UserDefinedFunction>
  signal?: AbortSignal
}

// planSql(options)
export interface PlanSqlOptions {
  query: string | Statement
  functions?: Record<string, UserDefinedFunction>
  tables?: Record<string, AsyncDataSource>
  // Optional CTE plan/column maps populated during planning. Callers can pass
  // in Maps to capture the resolved CTEs for later reference (e.g. by
  // subqueries that are re-planned during execution).
  ctePlans?: Map<string, QueryPlan>
  cteColumns?: Map<string, string[]>
}

// executePlan(plan, context)
export interface ExecuteContext {
  tables: Record<string, AsyncDataSource>
  functions?: Record<string, UserDefinedFunction>
  signal?: AbortSignal
  // current query's FROM + JOIN aliases (e.g. ['a', 'b'])
  scope?: string[]
  // the enclosing query's current row, for resolving correlated references
  outerRow?: AsyncRow
  // aliases from the enclosing query that are valid correlated references
  outerAliases?: Set<string>
  // CTE plans and column metadata from the enclosing WITH, for resolving
  // CTE references in subqueries re-planned during execution
  ctePlans?: Map<string, QueryPlan>
  cteColumns?: Map<string, string[]>
}

// AsyncRow represents a row with async cell values
export interface AsyncRow {
  columns: string[]
  cells: AsyncCells
  // Optional pre-materialized row values keyed by output column name.
  // When present, consumers can skip the AsyncCell Promise roundtrip.
  resolved?: Record<string, SqlPrimitive>
}
export type AsyncCells = Record<string, AsyncCell>
export type AsyncCell = () => Promise<SqlPrimitive>

export type Row = Record<string, SqlPrimitive>[]

/**
 * Async data source for streaming SQL execution.
 */
export interface AsyncDataSource {
  numRows?: number
  columns: string[]
  schema?: RelationSchema
  prepareScan?: PrepareScan
  scan?(options: ScanOptions): ScanResults
  // Optional method for fast column scans
  scanColumn?(options: ScanColumnOptions): AsyncIterable<ArrayLike<SqlPrimitive>> | ScanColumnResults
}

/**
 * Result of a scan: streaming rows and flags indicating which hints were
 * applied by the data source.
 */
export interface ScanResults {
  rows(): AsyncIterable<AsyncRow>
  appliedWhere: boolean // WHERE filter applied at scan time?
  appliedLimitOffset: boolean // LIMIT and OFFSET applied at scan time?
}

/**
 * Scan options passed to data sources for query optimization.
 * Sources may ignore these hints, but if they are applied, must set the applied
 * flags in ScanResult to inform the engine.
 */
export interface ScanOptions {
  columns?: string[] // columns needed (undefined means all columns)
  where?: ExprNode // where clause
  // important: only apply limit/offset if where is fully applied by the data source
  // otherwise, the data source must return at least enough rows to ensure the engine
  // can apply limit/offset correctly after filtering
  limit?: number
  offset?: number
  signal?: AbortSignal
}

/**
 * Options for scanning a single column.
 */
export interface ScanColumnOptions {
  column: string
  where?: ExprNode
  limit?: number
  offset?: number
  signal?: AbortSignal
}

/** Result of a column scan, mirroring the ScanResults hint flags. */
export interface ScanColumnResults {
  chunks(): AsyncIterable<ArrayLike<SqlPrimitive>>
  appliedWhere: boolean
  appliedLimitOffset: boolean
}

export interface FunctionSignature {
  min: number
  max?: number
  signature?: string
}

export interface UserDefinedFunction {
  apply: (...args: SqlPrimitive[]) => SqlPrimitive | Promise<SqlPrimitive>
  arguments: FunctionSignature
}

export type AggregateFunc = 'COUNT' | 'COUNTIF' | 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'MIN_BY' | 'ARG_MIN' | 'MAX_BY' | 'ARG_MAX' | 'ANY_VALUE' | 'ARRAY_AGG' | 'LIST' | 'JSON_ARRAYAGG' | 'STDDEV_SAMP' | 'STDDEV_POP' | 'MEDIAN' | 'PERCENTILE_CONT' | 'APPROX_QUANTILE' | 'STRING_AGG'

export type RegExpFunction = 'REGEXP_SUBSTR' | 'REGEXP_EXTRACT' | 'REGEXP_REPLACE' | 'REGEXP_MATCHES' | 'REGEXP_LIKE'

export type MathFunc =
  | 'FLOOR'
  | 'CEIL'
  | 'CEILING'
  | 'ROUND'
  | 'ABS'
  | 'SIGN'
  | 'MOD'
  | 'EXP'
  | 'LN'
  | 'LOG10'
  | 'POW'
  | 'POWER'
  | 'SQRT'
  | 'SIN'
  | 'COS'
  | 'TAN'
  | 'COT'
  | 'ASIN'
  | 'ACOS'
  | 'ATAN'
  | 'ATAN2'
  | 'DEGREES'
  | 'RADIANS'
  | 'PI'
  | 'RAND'
  | 'RANDOM'

export type StringFunc =
  | 'UPPER'
  | 'LOWER'
  | 'CONCAT'
  | 'LENGTH'
  | 'OCTET_LENGTH'
  | 'SUBSTRING'
  | 'SUBSTR'
  | 'TRIM'
  | 'REPLACE'
  | 'LEFT'
  | 'RIGHT'
  | 'INSTR'
  | 'POSITION'
  | 'STRPOS'
  | 'SPLIT_PART'
  | 'STRING_SPLIT'

export type SpatialFunc =
  | 'ST_INTERSECTS'
  | 'ST_CONTAINS'
  | 'ST_CONTAINSPROPERLY'
  | 'ST_WITHIN'
  | 'ST_OVERLAPS'
  | 'ST_TOUCHES'
  | 'ST_EQUALS'
  | 'ST_CROSSES'
  | 'ST_COVERS'
  | 'ST_COVEREDBY'
  | 'ST_DWITHIN'
  | 'ST_GEOMFROMTEXT'
  | 'ST_MAKEENVELOPE'
  | 'ST_ASTEXT'
