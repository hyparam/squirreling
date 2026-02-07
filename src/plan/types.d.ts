import { AsyncDataSource, ExprNode, JoinType, OrderByItem, QueryHints, SelectColumn, UserDefinedFunction } from '../types.js'

export interface ExecuteContext {
  tables: Record<string, AsyncDataSource>
  functions?: Record<string, UserDefinedFunction>
  signal?: AbortSignal
}

export type QueryPlan =
  | ScanNode
  | SubqueryScanNode
  | FilterNode
  | ProjectNode
  | HashJoinNode
  | NestedLoopJoinNode
  | PositionalJoinNode
  | HashAggregateNode
  | ScalarAggregateNode
  | SortNode
  | DistinctNode
  | LimitNode

export interface ScanNode {
  type: 'Scan'
  table: string
  alias?: string
  hints?: QueryHints
}

export interface SubqueryScanNode {
  type: 'SubqueryScan'
  subquery: QueryPlan
  alias: string
}

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

export interface HashJoinNode {
  type: 'HashJoin'
  joinType: JoinType
  leftKey: ExprNode
  rightKey: ExprNode
  left: QueryPlan
  right: QueryPlan
}

export interface NestedLoopJoinNode {
  type: 'NestedLoopJoin'
  joinType: JoinType
  condition: ExprNode
  left: QueryPlan
  right: QueryPlan
}

export interface PositionalJoinNode {
  type: 'PositionalJoin'
  left: QueryPlan
  right: QueryPlan
}

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
