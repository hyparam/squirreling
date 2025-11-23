# Squirreling SQL Engine

![squirreling engine](squirreling.jpg)

[![npm](https://img.shields.io/npm/v/squirreling)](https://www.npmjs.com/package/squirreling)
[![downloads](https://img.shields.io/npm/dt/squirreling)](https://www.npmjs.com/package/squirreling)
[![minzipped](https://img.shields.io/bundlephobia/minzip/squirreling)](https://www.npmjs.com/package/squirreling)
[![workflow status](https://github.com/hyparam/squirreling/actions/workflows/ci.yml/badge.svg)](https://github.com/hyparam/squirreling/actions)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-89-darkred)
[![dependencies](https://img.shields.io/badge/Dependencies-0-blueviolet)](https://www.npmjs.com/package/squirreling?activeTab=dependencies)

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

const source = [
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
]

const result = executeSql({ source, query: 'SELECT UPPER(name) AS name_upper FROM users' })
console.log(result)
// Output: [ { name_upper: 'ALICE' }, { name_upper: 'BOB' } ]
```
