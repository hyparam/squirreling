import type { AsyncDataSource, AsyncRow, ExecuteSqlOptions, ParseSqlOptions, SelectStatement, SqlPrimitive, Token } from './types.js'
export type { AsyncCells, AsyncDataSource, AsyncRow, ExprNode, ParseSqlOptions, SelectStatement, SqlPrimitive, Token, UserDefinedFunction } from './types.js'

/**
 * Executes a SQL SELECT query against an array of data rows
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
 * Parses a SQL query string into an abstract syntax tree
 *
 * @param options
 * @param options.query - SQL query string to parse
 * @param options.functions - user-defined functions available in the SQL context
 * @returns parsed SQL select statement
 */
export function parseSql(options: ParseSqlOptions): SelectStatement

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
