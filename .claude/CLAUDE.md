# CLAUDE.md

Squirreling is a streaming async SQL engine written in JavaScript.
Uses async everywhere and late materialization for efficiency.

## Build and Test Commands

```bash
npm test          # Run all tests
npm run lint      # Run ESLint
npm run lint:fix  # Fix linting issues
npm run coverage  # Run tests with coverage
npx vitest test/parse/parse.test.js  # Run a single test file
npx tsc           # Type check with TypeScript
```

## Code Style

- No semicolons
- Single quotes
- 2-space indentation
- Prefer `function` over arrow `=>` for named functions
- JSDoc type annotations required for all functions (@param, @returns, @yields)

## Architecture

The main flow is:

```
SQL string → parse → SelectStatement → plan → QueryPlan → execute → AsyncIterable<AsyncRow>
```

### Key Components

**Parser** (`src/parse/`):
- `tokenize.js` - lexer that converts SQL to tokens
- `parse.js` - recursive descent parser producing `SelectStatement` AST
- `expression.js` - expression parser with operator precedence
- `functions.js`, `comparison.js`, `joins.js` - specialized sub-parsers

**Planner** (`src/plan/`):
- `plan.js` - `planSql()` builds a `QueryPlan` tree from a `SelectStatement`
- `columns.js` - column projection pushdown analysis
- Resolves aliases and CTEs at plan time
- Plan node types: `ScanNode`, `FilterNode`, `ProjectNode`, `SortNode`, `DistinctNode`, `LimitNode`, `HashAggregateNode`, `ScalarAggregateNode`, `HashJoinNode`, `NestedLoopJoinNode`, `PositionalJoinNode`

**Executor** (`src/execute/`):
- `execute.js` - `executeSql()` and `executePlan()`, dispatches to node-specific executors
- `join.js`, `aggregates.js`, `sort.js` - executors for join, aggregate, and sort nodes

**Expression Evaluator** (`src/expression/`):
- `evaluate.js` - evaluates expression AST nodes against rows
- `alias.js` - `derivedAlias()` generates default column aliases for expressions
- `binary.js` - binary operator evaluation
- `strings.js`, `math.js`, `regexp.js`, `date.js` - built-in function implementations

**Backend** (`src/backend/`):
- `dataSource.js` - `cachedDataSource()` wrapper for caching async cell evaluations

### Core Types

Types defined in `src/types.d.ts` include:

- `AsyncRow` - row with `columns: string[]` and `cells: Record<string, () => Promise<SqlPrimitive>>`
- `AsyncDataSource` - provides `scan(options)` returning `ScanResults`
- `ScanOptions` - optimization hints passed to data sources (columns, where, limit/offset, signal)
- `ScanResults` - `{ rows, appliedWhere, appliedLimitOffset }` - scan must report which hints were applied
- `SqlPrimitive` - primitive SQL value types (string, number, bigint, boolean, Date, null, arrays, objects)
- `SelectStatement` - AST node for SELECT queries
- `QueryPlan` - tree of plan nodes produced by `planSql()`
- `ExecuteContext` - tables, functions, and signal passed through execution
