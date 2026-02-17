import { ExprNode, JoinType, OrderByItem, ScanOptions, SelectColumn } from '../types.js'

export type QueryPlan =
  | ScanNode
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

// Scan node
export interface ScanNode {
  type: 'Scan'
  table: string
  hints: ScanOptions
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
  aliases?: Map<string, ExprNode>
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
  leftKey: ExprNode
  rightKey: ExprNode
  left: QueryPlan
  right: QueryPlan
}

export interface NestedLoopJoinNode {
  type: 'NestedLoopJoin'
  joinType: JoinType
  leftAlias: string
  rightAlias: string
  condition: ExprNode
  left: QueryPlan
  right: QueryPlan
}

export interface PositionalJoinNode {
  type: 'PositionalJoin'
  leftAlias: string
  rightAlias: string
  left: QueryPlan
  right: QueryPlan
}
