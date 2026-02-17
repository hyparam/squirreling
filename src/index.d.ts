import type { AsyncDataSource, AsyncRow, ExecuteContext, ExecuteSqlOptions, ParseSqlOptions, PlanSqlOptions, QueryPlan, SelectStatement, SqlPrimitive, Token } from './types.js'
export type {
  AsyncCells,
  AsyncDataSource,
  AsyncRow,
  ExecuteContext,
  ExecuteSqlOptions,
  ExprNode,
  ParseSqlOptions,
  PlanSqlOptions,
  QueryPlan,
  ScanOptions,
  ScanResults,
  SelectStatement,
  SqlPrimitive,
  Token,
  UserDefinedFunction,
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
export function executeSql(options: ExecuteSqlOptions): AsyncGenerator<AsyncRow>

/**
 * Executes a query plan and yields result rows
 *
 * @param options
 * @param options.plan - the query plan to execute
 * @param options.context - execution context with tables, functions, and signal
 * @returns async generator yielding result rows
 */
export function executePlan(options: { plan: QueryPlan, context: ExecuteContext }): AsyncGenerator<AsyncRow>

/**
 * Parses a SQL query string into an abstract syntax tree
 *
 * @param options
 * @param options.query - SQL query string to parse
 * @param options.functions - user-defined functions available in the SQL context
 * @returns parsed SQL select statement
 */
export function parseSql(options: ParseSqlOptions): SelectStatement

/**
 * Builds a query plan from a SQL query string or AST
 *
 * @param options
 * @param options.query - SQL query string or parsed SelectStatement
 * @param options.functions - user-defined functions available in the SQL context
 * @returns the root of the query plan tree
 */
export function planSql(options: PlanSqlOptions): QueryPlan

/**
 * Tokenizes a SQL query string into an array of tokens
 *
 * @param sql - SQL query string to tokenize
 * @returns array of tokens
 */
export function tokenizeSql(sql: string): Token[]

/**
 * Collects all results from an async generator into an array
 *
 * @param asyncGen - the async generator
 * @returns array of all yielded values
 */
export function collect<T>(asyncGen: AsyncGenerator<AsyncRow>): Promise<Record<string, SqlPrimitive>[]>

export function cachedDataSource(source: AsyncDataSource): AsyncDataSource
