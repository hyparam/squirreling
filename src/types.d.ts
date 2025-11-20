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
  op: 'NOT'
  argument: ExprNode
}

export interface BinaryNode {
  type: 'binary'
  op: BinaryOp
  left: ExprNode
  right: ExprNode
}

export type ExprNode = LiteralNode | IdentifierNode | UnaryNode | BinaryNode

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

export type SelectColumn = StarColumn | SimpleColumn | AggregateColumn

export interface OrderByItem {
  expr: string
  direction: 'ASC' | 'DESC'
}

export interface SelectAst {
  distinct: boolean
  columns: SelectColumn[]
  from: string | null
  where: ExprNode | null
  groupBy: string[]
  orderBy: OrderByItem[]
  limit: number | null
  offset: number | null
}

interface ParserState {
  tokens: Token[]
  pos: number
}
