import { DerivedColumn, ExprNode, JoinType, OrderByItem, ScanOptions, SelectColumn, SetOperator } from '../types.js'

export type QueryPlan =
  | ScanNode
  | CountNode
  | FilterNode
  | ProjectNode
  | SortNode
  | DistinctNode
  | LimitNode
  | HashAggregateNode
  | ScalarAggregateNode
  | HashJoinNode
  | NestedLoopJoinNode
  | PositionalJoinNode
  | SetOperationNode
  | SubqueryNode
  | TableFunctionNode
  | WindowNode

// Scan node
export interface ScanNode {
  type: 'Scan'
  table: string
  hints: ScanOptions
}

// Count node for COUNT(*) optimization
export interface CountNode {
  type: 'Count'
  table: string
  columns: DerivedColumn[]
}

// Single-child nodes
export interface FilterNode {
  type: 'Filter'
  condition: ExprNode
  child: QueryPlan
}

export interface ProjectNode {
  type: 'Project'
  columns: SelectColumn[]
  child: QueryPlan
}

export interface SortNode {
  type: 'Sort'
  orderBy: OrderByItem[]
  child: QueryPlan
}

export interface DistinctNode {
  type: 'Distinct'
  child: QueryPlan
}

export interface LimitNode {
  type: 'Limit'
  limit?: number
  offset?: number
  child: QueryPlan
}

// Aggregate nodes
export interface HashAggregateNode {
  type: 'HashAggregate'
  groupBy: ExprNode[]
  columns: SelectColumn[]
  orderBy?: OrderByItem[]
  having?: ExprNode
  child: QueryPlan
}

export interface ScalarAggregateNode {
  type: 'ScalarAggregate'
  columns: SelectColumn[]
  having?: ExprNode
  child: QueryPlan
}

// Join nodes
export interface HashJoinNode {
  type: 'HashJoin'
  joinType: JoinType
  leftAlias: string
  rightAlias: string
  leftKeys: ExprNode[]
  rightKeys: ExprNode[]
  // Non-equi conjuncts from the ON clause (e.g. range predicates) applied to
  // each merged candidate after the hash lookup succeeds.
  residual?: ExprNode
  left: QueryPlan
  right: QueryPlan
}

export interface NestedLoopJoinNode {
  type: 'NestedLoopJoin'
  joinType: JoinType
  leftAlias: string
  rightAlias: string
  condition?: ExprNode
  left: QueryPlan
  right: QueryPlan
  lateral?: boolean
}

export interface PositionalJoinNode {
  type: 'PositionalJoin'
  leftAlias: string
  rightAlias: string
  left: QueryPlan
  right: QueryPlan
}

// Set operation node (UNION, INTERSECT, EXCEPT)
export interface SetOperationNode {
  type: 'SetOperation'
  operator: SetOperator
  all: boolean
  left: QueryPlan
  right: QueryPlan
}

// Wraps a derived-table or CTE subplan with the lexical alias scope of its
// inner SELECT, so the executor can set context.scope while traversing the
// subtree. Correlated subqueries inside the subtree resolve outer references
// against this scope, not whichever ancestor most recently went through
// executeStatement.
export interface SubqueryNode {
  type: 'Subquery'
  scope: string[]
  child: QueryPlan
}

// Table-valued function (e.g. UNNEST) used in FROM clause
export interface TableFunctionNode {
  type: 'TableFunction'
  funcName: string
  args: ExprNode[]
  columnNames: string[]
}

export interface WindowSpec {
  alias: string
  funcName: string
  args: ExprNode[]
  partitionBy: ExprNode[]
  orderBy: OrderByItem[]
}

export interface WindowNode {
  type: 'Window'
  windows: WindowSpec[]
  child: QueryPlan
}
