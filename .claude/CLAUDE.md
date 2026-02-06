# CLAUDE.md

Squirreling is a streaming async SQL engine written in JavaScript.
Uses async everywhere and late materialization for efficiency.

## Build and Test Commands

```bash
npm test              # Run all tests
npm run lint          # Run ESLint
npm run lint:fix      # Fix linting issues
npm run coverage      # Run tests with coverage
npx vitest test/parse/parse.test.js  # Run a single test file
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
SQL string → parse → SelectStatement → execute → AsyncIterable<AsyncRow>
```

### Key Components

**Parser** (`src/parse/`):
- `tokenize.js` - lexer that converts SQL to tokens
- `parse.js` - recursive descent parser producing `SelectStatement` AST
- `expression.js` - expression parser with operator precedence

**Expression Evaluator** (`src/expression/`):
- `evaluate.js` - evaluates expression AST nodes against rows
- `strings.js`, `math.js`, `regexp.js`, `date.js` - built-in function implementations

**Executor** (`src/execute/`):
- `execute.js` - main entry point, orchestrates query execution
- `join.js` - implements JOIN operations

### Core Types

Types defined in `src/types.d.ts` include:

- `AsyncRow` - row with `columns: string[]` and `cells: Record<string, () => Promise<SqlPrimitive>>`
- `AsyncDataSource` - provides `scan(options)` returning `AsyncIterable<AsyncRow>`
- `QueryHints` - optimization hints passed to data sources (columns, where, limit/offset)
- `SqlPrimitive` - primitive SQL value types (string, number, boolean, null)
- `SelectStatement` - AST node for SELECT queries
