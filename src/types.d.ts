export type Row = Record<string, any>

export type SqlPrimitive = string | number | boolean | null

export interface SelectStatement {
  distinct: boolean
  columns: SelectColumn[]
  from?: string
  joins: JoinClause[]
  where?: ExprNode
  groupBy: ExprNode[]
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

export type ExprNode = LiteralNode | IdentifierNode | UnaryNode | BinaryNode | FunctionNode

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

export interface AggregateArgColumn {
  kind: 'column'
  column: string
}

export type AggregateArg = AggregateArgStar | AggregateArgColumn

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

export type SelectColumn = StarColumn | SimpleColumn | AggregateColumn | FunctionColumn

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
}
