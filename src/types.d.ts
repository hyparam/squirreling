import type { ExprNode, SelectStatement, SqlPrimitive, Statement } from './ast.js'

export * from './ast.js'
export { ParserState, Token, TokenType } from './parse/types.js'
export { QueryPlan } from './plan/types.js'

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
}

// executePlan(plan, context)
export interface ExecuteContext {
  tables: Record<string, AsyncDataSource>
  functions?: Record<string, UserDefinedFunction>
  signal?: AbortSignal
}

// AsyncRow represents a row with async cell values
export interface AsyncRow {
  columns: string[]
  cells: AsyncCells
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
  scanColumn?(options: ScanColumnOptions): ScanColumnResults
}

/**
 * Result of a scan: streaming rows and flags indicating which hints were
 * applied by the data source.
 */
export interface ScanResults {
  rows: AsyncIterable<AsyncRow>
  appliedWhere: boolean // WHERE filter applied at scan time?
  appliedLimitOffset: boolean // LIMIT and OFFSET applied at scan time?
}

/**
 * Scan options passed to data sources for query optimization.
 * Sources may ignore these hints, but if they are applied, must set the applied
 * flags in ScanResults to inform the engine.
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
  signal?: AbortSignal
}

/**
 * Result of a column scan: streaming chunks of column values.
 */
export type ScanColumnResults = AsyncIterable<ArrayLike<SqlPrimitive>>

export interface FunctionSignature {
  min: number
  max?: number
  signature?: string
}

export interface UserDefinedFunction {
  apply: (...args: SqlPrimitive[]) => SqlPrimitive | Promise<SqlPrimitive>
  arguments: FunctionSignature
}

export type AggregateFunc = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'JSON_ARRAYAGG' | 'STDDEV_SAMP' | 'STDDEV_POP' | 'MEDIAN' | 'PERCENTILE_CONT' | 'APPROX_QUANTILE'

export type RegExpFunction = 'REGEXP_SUBSTR' | 'REGEXP_EXTRACT' | 'REGEXP_REPLACE'

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
