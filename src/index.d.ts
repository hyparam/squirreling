import type { AsyncRow, ExecuteSqlOptions, SelectStatement, SqlPrimitive } from './types.js'
export type { AsyncDataSource, AsyncRow, SqlPrimitive } from './types.js'

/**
 * Executes a SQL SELECT query against an array of data rows
 *
 * @param options
 * @param options.tables - source data as a list of objects or an AsyncDataSource
 * @param options.query - SQL query string
 * @returns async generator yielding rows matching the query
 */
export function executeSql(options: ExecuteSqlOptions): AsyncGenerator<AsyncRow>

/**
 * Parses a SQL query string into an abstract syntax tree
 *
 * @param query - SQL query string to parse
 * @returns parsed SQL select statement
 */
export function parseSql(query: string): SelectStatement

/**
 * Collects all results from an async generator into an array
 *
 * @param asyncGen - the async generator
 * @returns array of all yielded values
 */
export function collect<T>(asyncGen: AsyncGenerator<AsyncRow>): Promise<Record<string, SqlPrimitive>[]>
