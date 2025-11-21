export type Row = Record<string, any>

export type SqlPrimitive = string | number | boolean | null

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
  op: 'NOT' | 'IS NULL' | 'IS NOT NULL'
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
  alias?: string | null
}

export interface SimpleColumn {
  kind: 'column'
  column: string
  alias?: string | null
}

export type AggregateFunc = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX'

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
  alias?: string | null
}

export interface FunctionColumn {
  kind: 'function'
  func: string
  args: ExprNode[]
  alias?: string | null
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
  on: ExprNode | null
}

export interface SelectAst {
  distinct: boolean
  columns: SelectColumn[]
  from: string | null
  joins: JoinClause[]
  where: ExprNode | null
  groupBy: ExprNode[]
  orderBy: OrderByItem[]
  limit: number | null
  offset: number | null
}

export interface ParserState {
  tokens: Token[]
  pos: number
}

export interface ExprCursor {
  current(): Token
  peek(offset?: number): Token
  consume(): Token
  match(type: TokenType, value?: string): boolean
  matchKeyword(keywordUpper: string): boolean
  expect(type: TokenType, value?: string): Token
  expectKeyword(keywordUpper: string): Token
  expectIdentifier(): Token
}
