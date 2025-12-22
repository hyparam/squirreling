// parseSql(options)
export interface ParseSqlOptions {
  query: string
  functions?: Record<string, UserDefinedFunction>
}

// executeSql(options)
export interface ExecuteSqlOptions {
  tables: Record<string, Row | AsyncDataSource>
  query: string | SelectStatement
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
 * Provides an async iterator over rows.
 */
export interface AsyncDataSource {
  scan(options: ScanOptions): AsyncIterable<AsyncRow>
}
export interface ScanOptions {
  hints?: QueryHints
  signal?: AbortSignal
}
/**
 * Hints passed to data sources for query optimization.
 * All hints are optional and "best effort" - sources may ignore them.
 */
export interface QueryHints {
  columns?: string[] // columns needed
  where?: ExprNode // where clause
  // important: only apply limit/offset if where is fully applied by the data source
  // otherwise, the data source must return at least enough rows to ensure the engine
  // can apply limit/offset correctly after filtering
  // even with offset, the datasource must return rows starting from offset 0
  // but doesn't need to resolve async rows before the offset
  limit?: number
  offset?: number
}

export type SqlPrimitive =
  | string
  | number
  | bigint
  | boolean
  | Date
  | null
  | SqlPrimitive[]
  | Record<string, any>

export interface UserDefinedFunction {
  apply: (...args: SqlPrimitive[]) => SqlPrimitive | Promise<SqlPrimitive>
  arguments: {
    min: number
    max?: number
  }
}

export interface SelectStatement {
  distinct: boolean
  columns: SelectColumn[]
  from: FromTable | FromSubquery
  joins: JoinClause[]
  where?: ExprNode
  groupBy: ExprNode[]
  having?: ExprNode
  orderBy: OrderByItem[]
  limit?: number
  offset?: number
}

export interface FromTable {
  kind: 'table'
  table: string
  alias?: string
}

export interface FromSubquery {
  kind: 'subquery'
  query: SelectStatement
  alias: string
}

export type ArithmeticOp = '+' | '-' | '*' | '/' | '%'

export type BinaryOp = 'AND' | 'OR' | 'LIKE' | ComparisonOp | ArithmeticOp

export type ComparisonOp = '=' | '!=' | '<>' | '<' | '>' | '<=' | '>='

export interface ExprNodeBase {
  positionStart: number
  positionEnd: number
}

export interface LiteralNode extends ExprNodeBase {
  type: 'literal'
  value: SqlPrimitive
}

export interface IdentifierNode extends ExprNodeBase {
  type: 'identifier'
  name: string
}

export interface UnaryNode extends ExprNodeBase {
  type: 'unary'
  op: 'NOT' | 'IS NULL' | 'IS NOT NULL' | '-'
  argument: ExprNode
}

export interface BinaryNode extends ExprNodeBase {
  type: 'binary'
  op: BinaryOp
  left: ExprNode
  right: ExprNode
}

export interface FunctionNode extends ExprNodeBase {
  type: 'function'
  name: string
  args: ExprNode[]
  distinct?: boolean
}

export interface CastNode extends ExprNodeBase {
  type: 'cast'
  expr: ExprNode
  toType: string
}

export interface InSubqueryNode extends ExprNodeBase {
  type: 'in'
  expr: ExprNode
  subquery: SelectStatement
}

export interface InValuesNode extends ExprNodeBase {
  type: 'in valuelist'
  expr: ExprNode
  values: ExprNode[]
}

export interface ExistsNode extends ExprNodeBase {
  type: 'exists' | 'not exists'
  subquery: SelectStatement
}

export interface WhenClause {
  condition: ExprNode
  result: ExprNode
}

export interface CaseNode extends ExprNodeBase {
  type: 'case'
  caseExpr?: ExprNode
  whenClauses: WhenClause[]
  elseResult?: ExprNode
}

export interface SubqueryNode extends ExprNodeBase {
  type: 'subquery'
  subquery: SelectStatement
}

export type IntervalUnit = 'DAY' | 'MONTH' | 'YEAR' | 'HOUR' | 'MINUTE' | 'SECOND'

export interface IntervalNode extends ExprNodeBase {
  type: 'interval'
  value: number
  unit: IntervalUnit
}

export type ExprNode =
  | LiteralNode
  | IdentifierNode
  | UnaryNode
  | BinaryNode
  | FunctionNode
  | CastNode
  | InSubqueryNode
  | InValuesNode
  | ExistsNode
  | CaseNode
  | SubqueryNode
  | IntervalNode

export interface StarColumn {
  kind: 'star'
  table?: string
  alias?: string
}

export type AggregateFunc = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'JSON_ARRAYAGG'

export type MathFunc =
  | 'FLOOR'
  | 'CEIL'
  | 'CEILING'
  | 'ABS'
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
  | 'REGEXP_SUBSTR'
  | 'JSON_VALUE'
  | 'JSON_QUERY'
  | 'JSON_OBJECT'
  | 'CURRENT_DATE'
  | 'CURRENT_TIME'
  | 'CURRENT_TIMESTAMP'

export interface DerivedColumn {
  kind: 'derived'
  expr: ExprNode
  alias?: string
}

export type SelectColumn = StarColumn | DerivedColumn

export interface OrderByItem {
  expr: ExprNode
  direction: 'ASC' | 'DESC'
  nulls?: 'FIRST' | 'LAST'
}

export type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS'

export interface JoinClause {
  joinType: JoinType
  table: string
  alias?: string
  on?: ExprNode
}

export interface ParserState {
  tokens: Token[]
  pos: number
  lastPos?: number
  functions?: Record<string, UserDefinedFunction>
}

// Tokenizer types
export type TokenType =
  | 'keyword'
  | 'identifier'
  | 'number'
  | 'string'
  | 'operator'
  | 'comma'
  | 'dot'
  | 'paren'
  | 'semicolon'
  | 'eof'

export interface Token {
  type: TokenType
  value: string
  positionStart: number
  positionEnd: number
  numericValue?: number | bigint
  originalValue?: string
}
