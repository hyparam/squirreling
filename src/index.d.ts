import type { AsyncDataSource, AsyncRow, BatchScanOptions, ColumnBatch, ExecuteContext, ExecuteSqlOptions, ExprNode, ParseSqlOptions, PlanSqlOptions, QueryPlan, QueryResults, SqlPrimitive, Statement, Token } from './types.js'
export type {
  AsyncCells,
  AsyncDataSource,
  AsyncRow,
  BatchScanOptions,
  ColumnBatch,
  ExecuteContext,
  ExecuteSqlOptions,
  ExprNode,
  ParseSqlOptions,
  PlanSqlOptions,
  QueryPlan,
  QueryResults,
  ScanOptions,
  ScanResults,
  SelectStatement,
  SetOperationStatement,
  SqlPrimitive,
  Statement,
  Token,
  UserDefinedFunction,
  WithStatement,
} from './types.js'

/**
 * Executes a SQL SELECT query against tables
 *
 * @param options
 * @param options.tables - source data as a list of objects or an AsyncDataSource
 * @param options.query - SQL query string
 * @param options.functions - user-defined functions available in the SQL context
 * @param options.signal - AbortSignal to cancel the query
 * @returns async generator yielding rows matching the query
 */
export function executeSql(options: ExecuteSqlOptions): QueryResults

/**
 * Executes a query plan and yields result rows
 *
 * @param options
 * @param options.plan - the query plan to execute
 * @param options.context - execution context with tables, functions, and signal
 * @returns async generator yielding result rows
 */
export function executePlan(options: { plan: QueryPlan, context: ExecuteContext }): QueryResults

/**
 * Parses a SQL query string into an abstract syntax tree
 *
 * @param options
 * @param options.query - SQL query string to parse
 * @param options.functions - user-defined functions available in the SQL context
 * @returns parsed SQL statement
 */
export function parseSql(options: ParseSqlOptions): Statement

/**
 * Builds a query plan from a SQL query string or AST
 *
 * @param options
 * @param options.query - SQL query string or parsed SelectStatement
 * @param options.functions - user-defined functions available in the SQL context
 * @param options.tables - optional table metadata for planning
 * @returns the root of the query plan tree
 */
export function planSql(options: PlanSqlOptions): QueryPlan

/**
 * Tokenizes a SQL query string into an array of tokens
 *
 * @param query - SQL query string to tokenize
 * @returns array of tokens
 */
export function tokenizeSql(query: string): Token[]

/**
 * Collects all results from an async generator into an array
 *
 * @param asyncGen - the async generator
 * @returns array of all yielded values
 */
export function collect(results: QueryResults): Promise<Record<string, SqlPrimitive>[]>

export function asyncRow(row: Record<string, SqlPrimitive>, columns: string[]): AsyncRow

export function cachedDataSource(source: AsyncDataSource): AsyncDataSource

/**
 * Adapts a stream of AsyncRow into a stream of ColumnBatch by buffering rows
 * and materializing the requested columns into per-batch arrays.
 */
export function adaptRowsToBatches(
  rows: AsyncIterable<AsyncRow>,
  columns: string[],
  options?: { batchSize?: number, rowStart?: number, signal?: AbortSignal },
): AsyncIterable<ColumnBatch>

/**
 * Adapts a stream of ColumnBatch into a stream of AsyncRow. Each yielded row
 * has resolved values prefilled so consumers can skip the AsyncCell await.
 */
export function adaptBatchesToRows(
  batches: AsyncIterable<ColumnBatch>,
): AsyncIterable<AsyncRow>

/**
 * Returns batches from a data source, using its native scanBatches when
 * available and otherwise falling back to scan() + adaptRowsToBatches.
 */
export function scanBatches(
  source: AsyncDataSource,
  options?: BatchScanOptions,
): AsyncIterable<ColumnBatch>

/**
 * Generates a default alias for a derived column expression.
 * Useful for generating column names pre-execution.
 *
 * @param expr - the expression node
 * @returns the generated alias
 */
export function derivedAlias(expr: ExprNode): string
