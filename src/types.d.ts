export interface RowSource {
  getCell(name: string): any
  getKeys(): string[]
}

export interface DataSource {
  getNumRows(): number
  getRow(index: number): RowSource
}

export interface ExecuteSqlOptions {
  source: Record<string, any>[] | DataSource
  query: string
}

export type SqlPrimitive = string | number | bigint | boolean | null

export interface FromSubquery {
  kind: 'subquery'
  query: SelectStatement
  alias: string
}

export interface SelectStatement {
  distinct: boolean
  columns: SelectColumn[]
  from?: string | FromSubquery
  joins: JoinClause[]
  where?: ExprNode
  groupBy: ExprNode[]
  having?: ExprNode
  orderBy: OrderByItem[]
  limit?: number
  offset?: number
}

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

export interface InNode {
  type: 'in' | 'not in'
  expr: ExprNode
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
  | InNode

export interface StarColumn {
  kind: 'star'
  alias?: string
}

export interface SimpleColumn {
  kind: 'column'
  column: string
  alias?: string
}

export type AggregateFunc = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX'

export type StringFunc = 'UPPER' | 'LOWER' | 'CONCAT' | 'LENGTH' | 'SUBSTRING' | 'TRIM'

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

export interface FunctionColumn {
  kind: 'function'
  func: StringFunc
  args: ExprNode[]
  alias?: string
}

export interface OperationColumn {
  kind: 'operation'
  expr: ExprNode
  alias?: string
}

export type SelectColumn = StarColumn | SimpleColumn | AggregateColumn | FunctionColumn | OperationColumn

export interface OrderByItem {
  expr: ExprNode
  direction: 'ASC' | 'DESC'
}

export type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS'

export interface JoinClause {
  type: JoinType
  table: string
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
  parseSubquery?: () => SelectStatement
}
