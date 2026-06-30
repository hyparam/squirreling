import type { AsyncDataSource, AsyncRow, ExecuteContext, ExecuteSqlOptions, ExprNode, ParseSqlOptions, PlanSqlOptions, QueryPlan, QueryResults, SqlPrimitive, Statement, Token } from './types.js'
export type {
  AsyncCells,
  AsyncDataSource,
  AsyncRow,
  ExecuteContext,
  ExecuteSqlOptions,
  ExecutionBudget,
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
 * @param options.budget - execution budget for in-memory buffering operators
 * @returns async generator yielding rows matching the query
 */
export function executeSql(options: ExecuteSqlOptions): QueryResults

/**
 * Error thrown when an operator would accumulate more in-memory state than the
 * configured execution budget allows. V1 refuses over the ceiling; the engine
 * does not spill to disk or truncate the result.
 *
 * The emit contract depends on the operator:
 * - Buffering operators (ORDER BY, GROUP BY, the scalar-aggregate slow path)
 *   fully buffer before emitting, so the refusal is all-or-nothing: the error
 *   is thrown before any row is yielded.
 * - Streaming operators (DISTINCT, COUNT(DISTINCT)) bound only their dedup-set
 *   memory and may already have yielded rows before a later key trips the
 *   ceiling. A consumer must treat a thrown error as invalidating the whole
 *   result, not as a truncation point.
 */
export class QueryBudgetExceededError extends Error {
  constructor(options: { operator: string, limitKind: 'rows' | 'bytes', limit: number, observed: number })
  // buffering operator that hit the ceiling (e.g. 'ORDER BY')
  operator: string
  // which ceiling tripped
  limitKind: 'rows' | 'bytes'
  // configured ceiling value that was exceeded
  limit: number
  // buffered rows/bytes at the point of refusal
  observed: number
}

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

/**
 * Generates a default alias for a derived column expression.
 * Useful for generating column names pre-execution.
 *
 * @param expr - the expression node
 * @returns the generated alias
 */
export function derivedAlias(expr: ExprNode): string
