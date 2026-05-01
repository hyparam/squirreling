# Squirreling

Squirreling is a streaming async SQL engine written in JavaScript.
Uses async everywhere and late materialization for efficiency.

## Build and Test Commands

```bash
npm test          # Run all tests
npm run lint      # Run ESLint
npm run lint:fix  # Fix linting issues
npm run coverage  # Run tests with coverage
npx vitest run test/parse/parse.test.js  # Run a single test file
npx vitest run -t 'test name' # Run a single test by name
npx tsc           # Type check with TypeScript
```

## Code Style

- No semicolons
- Single quotes
- 2-space indentation
- Prefer `function` over arrow `=>` for named functions
- JSDoc type annotations required for all functions (@param, @returns, @yields)
- Imports at the top, never do inline imports of modules or types
- Tests:
  - Prefer `toEqual` over `toMatchObject` for more precise assertions
  - Do not add `: undefined` values to test fixtures (toEqual ignores undefined properties)
  - Pass the full exact error message string to `toThrow()` never a regex or substring

## Closure hygiene for AsyncRow cells

`AsyncCell` is `SqlPrimitive | (() => Promise<SqlPrimitive>)` — a cell is
either a bare value or a thunk. Two rules:

**1. Prefer raw values over thunks.** If you already have the value, store
it directly: `cells[col] = value`. Never wrap a known value in
`() => Promise.resolve(value)` — closures pin their V8 scope's *entire*
context, not just the variables they name, so a thunk created inside a
loop body silently retains every local in that scope (rows, sort entries,
window buffers).

**2. When a thunk is genuinely needed, build it via a helper in
`src/execute/cells.js`.** The helper's parameter list is the closure's
context — by construction, the caller's loop locals can't sneak in.

```js
// BAD — closure pins `rows` (the entire window buffer).
for (let i = 0; i < rows.length; i++) {
  cells[alias] = () => Promise.resolve(windowValues[i])
}

// GOOD — bare value, no closure.
for (let i = 0; i < rows.length; i++) {
  cells[alias] = windowValues[i]
}

// GOOD — genuinely lazy expression, built via helper.
cells[alias] = expressionCell(col.expr, row, rowIndex, context)
```

Companion rule: **don't widen an existing cell's retention by re-wrapping
it.** Copy the cell reference (`cells[alias] = row.cells[col]`); never wrap
it in a new closure (`cells[alias] = () => row.cells[col]()`) — that pins
`row` in the new closure's context.

For buffered nodes (aggregate, sort, buffered window, hash-join build):
once the input is buffered, lazy evaluation past that point buys nothing
and forces every output row to retain the buffer. Eagerly evaluate or
`materializeRow()` before yielding.

## Architecture

The main flow inside `executeSql({ tables, query, functions, signal })`:

```
1. tokenizeSql(query: string) → Token[]
2. parseSql({ query }) → SelectStatement
3. planSql({ query, tables }) → QueryPlan
4. executePlan({ plan, context }) → AsyncIterable<AsyncRow>
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

**Spatial** (`src/spatial/`):
- `geometry.d.ts` - geometry type definitions
- `bbox.js` - bounding box calculations
- `equality.js` - geometry equality comparisons
- `operations.js` - spatial relationship operations (intersects, contains, etc.)
- `spatial.js` - spatial function implementations (ST_Intersects, ST_Distance, etc.)
- `wkt.js` - WKT parsing and serialization

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
