import type { ExecuteSqlOptions, SelectStatement } from './types.js'

/**
 * Executes a SQL SELECT query against an array of data rows
 *
 * @param options
 * @param options.source - source data as a list of objects or a DataSource
 * @param options.sql - SQL query string
 * @returns rows matching the query
 */
export function executeSql(options: ExecuteSqlOptions): Record<string, any>[]

/**
 * Parses a SQL query string into an abstract syntax tree
 *
 * @param sql - SQL query string to parse
 * @returns parsed SQL select statement
 */
export function parseSql(sql: string): SelectStatement
