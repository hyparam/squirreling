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
for await (const { id, name } of asyncRows) {
  console.log(`User id=${await id()}, name=${await name()}`)
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

## Supported SQL Syntax

Squirreling mostly follows the SQL standard. The following features are supported:

- `SELECT` statements with `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
- `WITH` clause for Common Table Expressions (CTEs)
- Subqueries in `SELECT`, `FROM`, and `WHERE` clauses
- `JOIN` operations: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`, `POSITIONAL JOIN`
- `GROUP BY` and `HAVING` clauses

### Functions

- Aggregate: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `JSON_ARRAYAGG`
- String: `CONCAT`, `SUBSTRING`, `REPLACE`, `LENGTH`, `UPPER`, `LOWER`, `TRIM`, `LEFT`, `RIGHT`, `INSTR`
- Math: `ABS`, `SIGN`, `CEIL`, `FLOOR`, `ROUND`, `MOD`, `RAND`, `RANDOM`, `LN`, `LOG10`, `EXP`, `POWER`, `SQRT`
- Trig: `SIN`, `COS`, `TAN`, `COT`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `DEGREES`, `RADIANS`, `PI`
- Date: `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `INTERVAL`
- Json: `JSON_VALUE`, `JSON_QUERY`, `JSON_OBJECT`
- Regex: `REGEXP_SUBSTR`, `REGEXP_REPLACE`
- User-defined functions (UDFs)
