import type { AsyncBatch, AsyncDataSource, AsyncRow, ColumnResult, ColumnVector, ExecuteContext, ExecuteSqlOptions, ExprNode, ParseSqlOptions, PlanSqlOptions, QueryPlan, QueryResults, ReadBatchColumnOptions, RelationSchema, RowsToBatchesOptions, RowSelection, SqlPrimitive, Statement, Token } from './types.js'
export type {
  AsyncBatch,
  AsyncCells,
  AsyncDataSource,
  AsyncRow,
  BatchColumn,
  ColumnDemand,
  ColumnEvaluationRequest,
  ColumnReadRequest,
  ColumnResult,
  ColumnVector,
  ExecuteContext,
  ExecuteSqlOptions,
  ExprNode,
  Field,
  FieldId,
  NumericArray,
  ParseSqlOptions,
  PlanSqlOptions,
  PreparedScan,
  PrepareScan,
  QueryPlan,
  QueryResults,
  ReadBatches,
  ReadBatchesOptions,
  ReadBatchColumnOptions,
  RelationSchema,
  RowRange,
  RowsToBatchesOptions,
  RowSelection,
  ScanOptions,
  ScanProperties,
  ScanRequest,
  ScanResidual,
  ScanResults,
  SelectStatement,
  SetOperationStatement,
  SqlType,
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
 * @param options.signal - AbortSignal to cancel the query; an aborted query rejects with the signal's reason
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
 * Collects every external table referenced from FROM and JOIN clauses in a
 * parsed statement, descending into subqueries (IN, EXISTS, derived tables,
 * scalar subqueries) and the branches of compound queries. CTE names defined
 * by an enclosing WITH are skipped. Returned in first-seen order with
 * duplicates removed.
 *
 * @param statement - parsed SQL statement (output of `parseSql`)
 * @returns table names referenced in the query, excluding CTE aliases
 */
export function extractTables(statement: Statement): string[]

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

export function selectedRowCount(selection: RowSelection): number

export function composeSelections(outer: RowSelection, inner: RowSelection): RowSelection

export function valueAt(vector: ColumnVector, index: number): SqlPrimitive

export function selectVector(vector: ColumnVector, selection: RowSelection): ColumnVector

export function readBatchColumn(options: ReadBatchColumnOptions): ColumnResult

export function selectBatch(batch: AsyncBatch, selection: RowSelection): AsyncBatch

export function rowsToBatches(
  rows: AsyncIterable<AsyncRow>,
  schema: RelationSchema,
  options?: RowsToBatchesOptions,
): AsyncIterable<AsyncBatch>

export function batchesToRows(
  batches: AsyncIterable<AsyncBatch>,
  signal?: AbortSignal,
): AsyncIterable<AsyncRow>

/**
 * Generates a default alias for a derived column expression.
 * Useful for generating column names pre-execution.
 *
 * @param expr - the expression node
 * @returns the generated alias
 */
export function derivedAlias(expr: ExprNode): string
