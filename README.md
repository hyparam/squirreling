# Squirreling SQL Engine

![squirreling engine](squirreling.jpg)

[![npm](https://img.shields.io/npm/v/squirreling)](https://www.npmjs.com/package/squirreling)
[![downloads](https://img.shields.io/npm/dt/squirreling)](https://www.npmjs.com/package/squirreling)
[![minzipped](https://img.shields.io/bundlephobia/minzip/squirreling)](https://www.npmjs.com/package/squirreling)
[![workflow status](https://github.com/hyparam/squirreling/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/squirreling/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-95-darkred)
[![dependencies](https://img.shields.io/badge/Dependencies-0-blueviolet)](https://www.npmjs.com/package/squirreling?activeTab=dependencies)

Squirreling is a streaming async SQL engine for JavaScript. It is designed to provide efficient streaming of results from pluggable backends for highly efficient retrieval of data for browser applications.

## Features

- Lightweight and fast
- Easy to integrate with frontend applications
- Lets you move query execution closer to your users
- Supports standard SQL queries
- Async streaming for large datasets
- Native javascript Promises, AsyncGenerators, AbortSignals
- Async user-defined functions (UDFs)
- Constant memory usage for simple queries with LIMIT
- Robust error handling and validation designed for LLM tool use
- In-memory data option for simple use cases
- Late materialization for efficiency
- Select only

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

## Supported SQL Features

- `SELECT` statements with `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
- Subqueries in `SELECT`, `FROM`, and `WHERE` clauses
- `JOIN` operations: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`
- `GROUP BY` and `HAVING` clauses
- Aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `JSON_ARRAYAGG`
- String functions: `CONCAT`, `SUBSTRING`, `LENGTH`, `UPPER`, `LOWER`
- Math functions: `ABS`, `CEIL`, `FLOOR`, `ROUND`, `MOD`, `RAND`, `RANDOM`, `LN`, `LOG10`, `EXP`, `POWER`, `SQRT`, `SIN`, `COS`, `TAN`, `COT`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `DEGREES`, `RADIANS`, `PI`
- Date functions: `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `INTERVAL`
- Json functions: `JSON_VALUE`, `JSON_QUERY`, `JSON_OBJECT`
- Basic expressions and arithmetic operations
