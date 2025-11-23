import type { RowSource, SelectStatement } from './types.js'

/**
 * Executes a SQL SELECT query against an array of data rows
 *
 * @param rows - data rows to query as a list of objects
 * @param sql - SQL query string
 * @returns rows matching the query
 */
export function executeSql(rows: RowSource[], sql: string): RowSource[]

/**
 * Parses a SQL query string into an abstract syntax tree
 *
 * @param sql - SQL query string to parse
 * @returns parsed SQL select statement
 */
export function parseSql(sql: string): SelectStatement
