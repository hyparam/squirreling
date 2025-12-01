# Squirreling SQL Engine

![squirreling engine](squirreling.jpg)

[![npm](https://img.shields.io/npm/v/squirreling)](https://www.npmjs.com/package/squirreling)
[![downloads](https://img.shields.io/npm/dt/squirreling)](https://www.npmjs.com/package/squirreling)
[![minzipped](https://img.shields.io/bundlephobia/minzip/squirreling)](https://www.npmjs.com/package/squirreling)
[![workflow status](https://github.com/hyparam/squirreling/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/squirreling/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-93-darkred)
[![dependencies](https://img.shields.io/badge/Dependencies-0-blueviolet)](https://www.npmjs.com/package/squirreling?activeTab=dependencies)

Squirreling is a streaming async SQL engine for JavaScript. It is designed to provide efficient streaming of results from pluggable backends for highly efficient retrieval of data for browser applications.

## Features

- Lightweight and fast
- Easy to integrate with frontend applications
- Lets you move query execution closer to your users
- Supports standard SQL queries
- Async streaming for large datasets
- Constant memory usage for simple queries with LIMIT
- Robust error handling and validation designed for LLM tool use
- In-memory data option for simple use cases
- Select only
- No joins (yet)

## Usage

Squirreling returns an async generator, allowing you to process rows one at a time without loading everything into memory.

```javascript
import { executeSql } from 'squirreling'

// In-memory table
const users = [
  { id: 1, name: 'Alice', active: true },
  { id: 2, name: 'Bob', active: false },
  { id: 3, name: 'Charlie', active: true },
  // ...more rows
]

// Process rows as they arrive (streaming)
for await (const { cnt } of executeSql({
  tables: { users },
  query: 'SELECT count(*) as cnt FROM users WHERE active = TRUE LIMIT 10',
})) {
  console.log('Count', cnt)
}
```

There is an exported helper function `collect` to gather all rows into an array if needed:

```javascript
import { collect, executeSql } from 'squirreling'

const allUsers = await collect(executeSql({
  tables: { users },
  query: 'SELECT * FROM users',
}))
console.log(allUsers)
```
