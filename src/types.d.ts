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
  // Optional column-oriented batch iterator. Operators set this when they can
  // produce batches natively (e.g. when the underlying source has scanBatches
  // and the operator can chain through them without per-row materialization).
  // Consumers may prefer batches() over rows() for throughput; rows() is still
  // the canonical interface and always works.
  batches?(): AsyncIterable<ColumnBatch>
  numRows?: number
  maxRows?: number
}

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
  budget?: SqlExecutionBudget
}

/**
 * Execution-time budget enforced across a single SQL query. All fields are
 * optional; an unset field means no limit. When any field is exceeded the
 * executor throws a SqlBudgetError that identifies which limit was breached.
 */
export interface SqlExecutionBudget {
  // Max rows pinned in materializing buffers (sort, hash, etc.) summed across
  // operators. Each row added to a buffer counts once.
  maxRowsToMaterialize?: number
  // Max approximate heap bytes used by intermediate buffers, summed across
  // operators. Bytes are estimated per row (default 64 if no estimate given).
  maxHeapBytes?: number
  // Max approximate intermediate bytes for any single materializing operator.
  maxIntermediateBytes?: number
  // Wall-clock timeout in milliseconds. Checked on each materialized row and
  // explicitly via tracker.checkTimeout() in long-running streaming loops.
  timeoutMs?: number
  // When false, the executor must not use derived column scan fast paths.
  // Default true (existing behavior).
  allowDerivedColumnScan?: boolean
}

/**
 * Internal tracker created from a SqlExecutionBudget. Operators that
 * materialize rows obtain an operator-scoped handle via tracker.operator(name)
 * and call .addRow() before pinning each row. Plumbed through ExecuteContext.
 */
export interface BudgetTracker {
  budget: SqlExecutionBudget
  // Whether scalar-aggregate scanColumn fast paths may run. False when
  // budget.allowDerivedColumnScan === false.
  allowDerivedColumnScan: boolean
  // Returns an operator-scoped handle that tracks per-operator intermediate
  // bytes and contributes to the global row/heap counters.
  operator(name: string): BudgetOperator
  // Throws SqlBudgetError if the wall-clock deadline has passed.
  checkTimeout(): void
}

export interface BudgetOperator {
  // Records one materialized row. Optional approxBytes lets operators that
  // know more (e.g. typed arrays) provide a better estimate.
  addRow(approxBytes?: number): void
  // Throws SqlBudgetError if the wall-clock deadline has passed.
  checkTimeout(): void
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
  // Budget tracker for resource limits. When set, materializing operators
  // call .operator(name).addRow() before pinning each row.
  budget?: BudgetTracker
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
  scan(options: ScanOptions): ScanResults
  // Optional method for fast column scans
  scanColumn?(options: ScanColumnOptions): AsyncIterable<ArrayLike<SqlPrimitive>>
  // Optional column-oriented batch scan. Sources that implement this can
  // hand the engine columnar data directly without per-row materialization.
  // Sources without scanBatches are bridged via adaptRowsToBatches.
  scanBatches?(options: BatchScanOptions): AsyncIterable<ColumnBatch>
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
  limit?: number
  offset?: number
  signal?: AbortSignal
}

/**
 * Options for a column-oriented batch scan. Mirrors the minimal shape of
 * ScanOptions; pushdown of WHERE/LIMIT/OFFSET is intentionally not part of
 * the batch interface in Phase 5a. Filters are applied by the engine on the
 * yielded batches (or after adapting batches back to rows).
 */
export interface BatchScanOptions {
  columns?: string[] // columns needed (undefined means all columns)
  batchSize?: number // suggested rows per batch — sources may use any size
  signal?: AbortSignal
}

/**
 * A column-oriented batch of rows. Either rowStart (sequential, contiguous)
 * or rowIds (arbitrary identifiers, e.g. after a filter) identifies the rows
 * — sources should set whichever they have. rowCount is authoritative for
 * how many entries to read from each column array.
 */
export interface ColumnBatch {
  rowStart?: number
  rowIds?: Uint32Array | BigUint64Array
  rowCount: number
  columns: Record<string, ArrayLike<SqlPrimitive>>
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

export type AggregateFunc = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'ARRAY_AGG' | 'JSON_ARRAYAGG' | 'STDDEV_SAMP' | 'STDDEV_POP' | 'MEDIAN' | 'PERCENTILE_CONT' | 'APPROX_QUANTILE' | 'STRING_AGG'

export type RegExpFunction = 'REGEXP_SUBSTR' | 'REGEXP_EXTRACT' | 'REGEXP_REPLACE' | 'REGEXP_MATCHES'

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
  | 'SUBSTRING'
  | 'SUBSTR'
  | 'TRIM'
  | 'REPLACE'
  | 'LEFT'
  | 'RIGHT'
  | 'INSTR'
  | 'POSITION'
  | 'STRPOS'

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
