
/**
 * Async data source for streaming SQL execution.
 * Provides an async iterator over rows.
 */
export interface AsyncDataSource {
  getRows(): AsyncIterable<AsyncRow>
}
export type AsyncRow = Record<string, AsyncCell>
export type AsyncCell = () => Promise<SqlPrimitive>

export type Row = Record<string, SqlPrimitive>[]

export interface ExecuteSqlOptions {
  tables: Record<string, Row | AsyncDataSource>
  query: string
}

export type SqlPrimitive = string | number | bigint | boolean | null

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

export type BinaryOp =
  | 'AND'
  | 'OR'
  | '='
  | '!='
  | '<>'
  | '<'
  | '>'
  | '<='
  | '>='
  | 'LIKE'

export interface LiteralNode {
  type: 'literal'
  value: SqlPrimitive
}

export interface IdentifierNode {
  type: 'identifier'
  name: string
}

export interface UnaryNode {
  type: 'unary'
  op: 'NOT' | 'IS NULL' | 'IS NOT NULL' | '-'
  argument: ExprNode
}

export interface BinaryNode {
  type: 'binary'
  op: BinaryOp
  left: ExprNode
  right: ExprNode
}

export interface FunctionNode {
  type: 'function'
  name: string
  args: ExprNode[]
}

export interface CastNode {
  type: 'cast'
  expr: ExprNode
  toType: string
}

export interface BetweenNode {
  type: 'between' | 'not between'
  expr: ExprNode
  lower: ExprNode
  upper: ExprNode
}

export interface InSubqueryNode {
  type: 'in' | 'not in'
  expr: ExprNode
  subquery: SelectStatement
}

export interface InValuesNode {
  type: 'in valuelist' | 'not in valuelist'
  expr: ExprNode
  values: ExprNode[]
}

export interface ExistsNode {
  type: 'exists' | 'not exists'
  subquery: SelectStatement
}

export interface WhenClause {
  condition: ExprNode
  result: ExprNode
}

export interface CaseNode {
  type: 'case'
  caseExpr?: ExprNode
  whenClauses: WhenClause[]
  elseResult?: ExprNode
}

export interface SubqueryNode {
  type: 'subquery'
  subquery: SelectStatement
}

export type ExprNode =
  | LiteralNode
  | IdentifierNode
  | UnaryNode
  | BinaryNode
  | FunctionNode
  | CastNode
  | BetweenNode
  | InSubqueryNode
  | InValuesNode
  | ExistsNode
  | CaseNode
  | SubqueryNode

export interface StarColumn {
  kind: 'star'
  table?: string
  alias?: string
}

export type AggregateFunc = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX'

export type StringFunc = 'UPPER' | 'LOWER' | 'CONCAT' | 'LENGTH' | 'SUBSTRING' | 'SUBSTR' | 'TRIM' | 'REPLACE'

export interface AggregateArgStar {
  kind: 'star'
}

export interface AggregateArgExpression {
  kind: 'expression'
  expr: ExprNode
}

export type AggregateArg = AggregateArgStar | AggregateArgExpression

export interface AggregateColumn {
  kind: 'aggregate'
  func: AggregateFunc
  arg: AggregateArg
  alias?: string
}

export interface DerivedColumn {
  kind: 'derived'
  expr: ExprNode
  alias?: string
}

export type SelectColumn = StarColumn | AggregateColumn | DerivedColumn

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
}

export interface ExprCursor {
  current(): Token
  peek(offset: number): Token
  consume(): Token
  match(type: TokenType, value?: string): boolean
  expect(type: TokenType, value: string): Token
  expectIdentifier(): Token
  parseSubquery: () => SelectStatement
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
  position: number
  numericValue?: number
  originalValue?: string
}
