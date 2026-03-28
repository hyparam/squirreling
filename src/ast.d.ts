export type SqlPrimitive =
  | string
  | number
  | bigint
  | boolean
  | Date
  | null
  | SqlPrimitive[]
  | Record<string, any>

export interface SelectStatement extends AstBase {
  type: 'select'
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

export type SetOperator = 'UNION' | 'INTERSECT' | 'EXCEPT'

export interface SetOperationStatement extends AstBase {
  type: 'compound'
  operator: SetOperator
  all: boolean
  left: Statement
  right: Statement
  orderBy: OrderByItem[]
  limit?: number
  offset?: number
}

export interface WithStatement extends AstBase {
  type: 'with'
  ctes: CTEDefinition[]
  query: Statement
}

export type Statement = SelectStatement | SetOperationStatement | WithStatement

export interface CTEDefinition extends AstBase {
  name: string
  query: Statement
}

export interface FromTable extends AstBase {
  type: 'table'
  table: string
  alias?: string
}

export interface FromSubquery extends AstBase {
  type: 'subquery'
  query: Statement
  alias?: string
}

export type ArithmeticOp = '+' | '-' | '*' | '/' | '%'

export type BinaryOp = 'AND' | 'OR' | 'LIKE' | ComparisonOp | ArithmeticOp

export type ComparisonOp = '=' | '!=' | '<>' | '<' | '>' | '<=' | '>='

export interface LiteralNode extends AstBase {
  type: 'literal'
  value: SqlPrimitive
}

export interface IdentifierNode extends AstBase {
  type: 'identifier'
  name: string
}

export interface UnaryNode extends AstBase {
  type: 'unary'
  op: 'NOT' | 'IS NULL' | 'IS NOT NULL' | '-'
  argument: ExprNode
}

export interface BinaryNode extends AstBase {
  type: 'binary'
  op: BinaryOp
  left: ExprNode
  right: ExprNode
}

export interface FunctionNode extends AstBase {
  type: 'function'
  funcName: string
  args: ExprNode[]
  distinct?: boolean
  filter?: ExprNode
}

export type CastType = 'TEXT' | 'STRING' | 'VARCHAR' | 'INTEGER' | 'INT' | 'BIGINT' | 'FLOAT' | 'REAL' | 'DOUBLE' | 'BOOLEAN' | 'BOOL'

export interface CastNode extends AstBase {
  type: 'cast'
  expr: ExprNode
  toType: CastType
}

export interface InSubqueryNode extends AstBase {
  type: 'in'
  expr: ExprNode
  subquery: Statement
}

export interface InValuesNode extends AstBase {
  type: 'in valuelist'
  expr: ExprNode
  values: ExprNode[]
}

export interface ExistsNode extends AstBase {
  type: 'exists' | 'not exists'
  subquery: Statement
}

export interface WhenClause extends AstBase {
  condition: ExprNode
  result: ExprNode
}

export interface CaseNode extends AstBase {
  type: 'case'
  caseExpr?: ExprNode
  whenClauses: WhenClause[]
  elseResult?: ExprNode
}

export interface SubqueryNode extends AstBase {
  type: 'subquery'
  subquery: Statement
}

export type IntervalUnit = 'DAY' | 'MONTH' | 'YEAR' | 'HOUR' | 'MINUTE' | 'SECOND'

export interface IntervalNode extends AstBase {
  type: 'interval'
  value: number
  unit: IntervalUnit
}

export interface StarNode extends AstBase {
  type: 'star'
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
  | StarNode

export interface StarColumn {
  type: 'star'
  table?: string
}

export interface DerivedColumn {
  type: 'derived'
  expr: ExprNode
  alias?: string
}

export type SelectColumn = StarColumn | DerivedColumn

export interface OrderByItem {
  expr: ExprNode
  direction: 'ASC' | 'DESC'
  nulls?: 'FIRST' | 'LAST'
}

export type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS' | 'POSITIONAL'

export interface JoinClause extends AstBase {
  joinType: JoinType
  table: string
  alias?: string
  on?: ExprNode
}

// All AST node derive from this base, which includes position info for error reporting and other purposes
interface AstBase {
  positionStart: number // start position in query (0-based, inclusive)
  positionEnd: number // end position in query (0-based, exclusive)
}
