# Squirreling SQL Engine

![squirreling engine](squirreling.jpg)

[![npm](https://img.shields.io/npm/v/squirreling)](https://www.npmjs.com/package/squirreling)
[![downloads](https://img.shields.io/npm/dt/squirreling)](https://www.npmjs.com/package/squirreling)
[![minzipped](https://img.shields.io/bundlephobia/minzip/squirreling)](https://www.npmjs.com/package/squirreling)
[![workflow status](https://github.com/hyparam/squirreling/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/squirreling/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-95-darkred)
[![dependencies](https://img.shields.io/badge/Dependencies-0-blueviolet)](https://www.npmjs.com/package/squirreling?activeTab=dependencies)

Squirreling is a streaming async SQL engine in pure JavaScript. Built for the browser from the ground up: streaming input and output, pluggable data sources, and lazy async cell evaluation. This makes Squirreling ideal for querying data from network sources, APIs, or LLMs where latency and cost matter.

- **Standard SQL**: Full SQL support for querying data (read-only)
- **Async UDFs**: User-defined functions can call APIs or models
- **Tiny**: 13 kb bundle, zero dependencies, instant startup

The key idea is **cell-level lazy evaluation**: rows are native AsyncGenerators and cells are async thunks `() => Promise<T>`. This means expensive operations only execute for cells that actually appear in your query results. Unlike WebAssembly databases, Squirreling is fully async with true streaming during network fetches.

## Usage

Squirreling returns an AsyncGenerator of AsyncRows, allowing you to process rows one at a time without loading everything into memory. AsyncRows are made up of AsyncCells, allowing for late materialization of values.

```typescript
import { executeSql } from 'squirreling'

// Input table (in-memory for this example)
const users = [
  { id: 1, name: 'Alice', active: true },
  { id: 2, name: 'Bob', active: false },
  { id: 3, name: 'Charlie', active: true },
  // ...more rows
]

// Squirreling return types
interface AsyncRow {
  columns: string[]
  cells: Record<string, AsyncCell>
}
type AsyncCell = () => Promise<SqlPrimitive>

// Returns an AsyncIterable of rows with async cell loading
const asyncRows: AsyncIterable<AsyncRow> = executeSql({
  tables: { users },
  query: 'SELECT * FROM users',
})

// Process rows as they arrive (streaming)
for await (const { cells } of asyncRows) {
  console.log(`User id=${await cells.id()}, name=${await cells.name()}`)
}
```

Squirreling exports a helper function `collect` to gather all rows into an array:

```javascript
import { collect, executeSql } from 'squirreling'

// Collect all rows and cells into a materialized array
const rows: Record<string, SqlPrimitive>[] = await collect(executeSql({
  tables: { users },
  query: 'SELECT active, count(*) as cnt FROM users GROUP BY active',
}))
console.log(`Collected rows:`, rows)
// Collected rows: [ { active: true, cnt: 2 }, { active: false, cnt: 1 } ]
```

### User-Defined Functions

Pass custom functions via the `functions` option. UDFs can be sync or async, making them ideal for calling APIs, models, or other external services:

```javascript
const rows = await collect(executeSql({
  tables: { products },
  query: 'SELECT name,AI_SCORE(description) AS score FROM products',
  functions: {
    AI_SCORE: {
      apply: async (text) => completions(`Rate the following product description from 1 to 10: ${text}`),
      arguments: { min: 1, max: 1 },
    },
  },
}))
```

Because Squirreling uses lazy cell evaluation, the `AI_SCORE` function only executes for cells that are actually materialized. Combined with `LIMIT` or `WHERE`, you can efficiently query expensive operations.

### Custom Data Sources

Squirreling can work with any data source that implements the `AsyncDataSource` interface.

```typescript
interface AsyncDataSource {
  scan(options: ScanOptions): ScanResults
}

interface ScanOptions {
  columns?: string[] // columns to scan (undefined means all)
  where?: ExprNode
  limit?: number
  offset?: number
  signal?: AbortSignal
}

interface ScanResults {
  rows: AsyncIterable<AsyncRow> // async iterable of rows
  appliedWhere: boolean // WHERE filter applied at scan time?
  appliedLimitOffset: boolean // LIMIT and OFFSET applied at scan time?
}
```

The `scan()` method returns a `ScanResults` object containing a row stream and flags indicating which query hints were applied by the data source. This allows optional push down optimizations like filtering, limiting, and offsetting at the data source level when possible. Set `appliedWhere` or `appliedLimitOffset` to `true` if the data source handled them, `false` if the engine should apply them.

```typescript
const customSource: AsyncDataSource = {
  scan({ columns, where, limit, offset, signal }) {
    // Use hints to optimize your scan, or ignore them
    return {
      rows: fetchAllRows({ columns, signal }),
      appliedWhere: false, // source returned all rows, engine will filter
      appliedLimitOffset: false, // source returned all rows, engine will limit/skip
    }
  },
}
```

## Supported SQL Syntax

Squirreling mostly follows the SQL standard. The following features are supported:

- `SELECT` statements with `DISTINCT`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
- `WITH` clause for Common Table Expressions (CTEs)
- Subqueries in `SELECT`, `FROM`, and `WHERE` clauses
- `JOIN` operations: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`, `CROSS JOIN`, `POSITIONAL JOIN`
- `GROUP BY` and `HAVING` clauses
- Expressions: `CASE`, `CAST`, `BETWEEN`, `IN`, `LIKE`, `IS NULL`, `IS NOT NULL`

### Quoting

- Single quotes for string literals: `'hello world'`
- Double quotes for identifiers with spaces or special characters: `"column name"`
- Escape quotes by doubling: `'can''t'` or `"col""name"`

### Functions

- Aggregate: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV_POP`, `STDDEV_SAMP`, `JSON_ARRAYAGG`
- String: `CONCAT`, `SUBSTRING`, `REPLACE`, `LENGTH`, `UPPER`, `LOWER`, `TRIM`, `LEFT`, `RIGHT`, `INSTR`
- Math: `ABS`, `SIGN`, `CEIL`, `FLOOR`, `ROUND`, `MOD`, `RAND`, `RANDOM`, `LN`, `LOG10`, `EXP`, `POWER`, `SQRT`
- Trig: `SIN`, `COS`, `TAN`, `COT`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `DEGREES`, `RADIANS`, `PI`
- Date: `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `INTERVAL`
- Json: `JSON_VALUE`, `JSON_QUERY`, `JSON_OBJECT`
- Array: `ARRAY_LENGTH`, `ARRAY_POSITION`, `ARRAY_SORT`, `CARDINALITY`
- Regex: `REGEXP_SUBSTR`, `REGEXP_REPLACE`
- Conditional: `COALESCE`, `NULLIF`
- User-defined functions (UDFs)
