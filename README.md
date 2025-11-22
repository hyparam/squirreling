# Squirreling SQL Engine

[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)

Squirreling is a lightweight SQL engine for JavaScript applications, designed to provide efficient and easy-to-use database functionalities in the browser.

## Features

- Lightweight and fast
- Easy to integrate with JavaScript applications
- Supports standard SQL queries
- In-memory database for quick data access
- Robust error handling and validation

## Usage

```javascript
import { executeSql } from 'squirreling'

const data = [
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
]

const result = executeSql(data, 'SELECT UPPER(name) AS name_upper FROM users')
console.log(result)
// Output: [ { name_upper: 'ALICE' }, { name_upper: 'BOB' } ]
```
