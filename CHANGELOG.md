# Changelog

## [0.9.1]
 - Export `derivedAlias` to construct column aliases

## [0.9.0]
 - Export `planSql()` and `executePlan()` for separate plan and execute phases
 - Pushdown columns to join tables
 - Fix `ORDER BY COUNT(*)`
 - Parse error when non-count aggregate is applied to star

## [0.8.0]
 - Breaking: Scan **must** return whether filters and offsets were applied (#9)
 - Breaking: Refactor `QueryHints` into `ScanOptions`
 - Parse error on negative offset/limit

## [0.7.10]
 - `NULLIF` function

## [0.7.9]
 - `FILTER` clause for aggregates
 - `STDDEV_SAMP` and `STDDEV_POP` aggregate functions

## [0.7.8]
 - Alias without `AS` keyword
 - `ROUND` and `SIGN` math functions

## [0.7.7]
 - Fix sum of empty set returning null
 - Fix loose equality in `IN` and `CASE`

## [0.7.6]
 - Order by column alias

## [0.7.5]
 - Throw for column not found
 - Fix mixed wildcard and select expressions

## [0.7.4]
 - Don't pass `*` to column hints

## [0.7.3]
 - Common table expressions (CTE)

## [0.7.2]
 - Export `tokenizeSql`
 - `POSITIONAL JOIN`

## [0.7.1]
 - `COALESCE` function
 - `REGEXP` functions
 - More string functions
 - `UserDefinedFunction` object type

## [0.7.0]
 - User defined functions
 - Parse options
 - Validate function args at parse time

## [0.6.1]
 - Aggregate expressions
 - Scan abort signal

## [0.6.0]
 - Column names in `AsyncRow`
 - Rename `getRows()` to `scan()`
 - `executeSql` accepts string or `SelectStatement`
 - Trig functions

## [0.5.0]
 - Row numbers in execution errors
 - Structured parse and execution errors
 - Positions in expression nodes
 - Math functions

## [0.4.8]
 - Tokenize bigints and negative numbers
 - Structured error messages
 - `INTERVAL` support
 - Current date functions

## [0.4.7]
 - Arithmetic operators in expressions
 - `JSON_OBJECT`, `JSON_VALUE`, `JSON_QUERY`, `JSON_ARRAYAGG` functions

## [0.4.6]
 - Soft equality

## [0.4.5]
 - Fix comparison of bigints

## [0.4.4]
 - Pushdown hints to `AsyncDataSource`

## [0.4.3]
 - `COUNT DISTINCT`
 - Nested function in aggregate

## [0.4.2]
 - Fix double sorting on non-grouped
 - `NOT`, `LIKE`, `IN`, `BETWEEN` operators
 - `ORDER BY RANDOM()`
 - Multi-column sort with tie-breaking

## [0.4.1]
 - Joins (#2)
 - From table alias

## [0.4.0]
 - Yield async rows
 - Refactor `AsyncRow`
 - Cached data source

## [0.3.1]
 - Scalar subquery
 - `AsyncDataSource` from generator
 - Optimize order by with limit
 - Stream `DISTINCT`

## [0.3.0]
 - Async everything (#1)
 - Subqueries
 - Named table sources
 - `REPLACE` function

## [0.2.6]
 - `CASE` expression

## [0.2.5]
 - `SUBSTRING` / `SUBSTR` function
 - Qualified stars
 - `NULLS FIRST` / `NULLS LAST`

## [0.2.4]
 - `IN` value-list
 - `CAST` expression

## [0.2.3]
 - `WHERE EXISTS`
 - `WHERE IN` subquery
 - Subqueries

## [0.2.2]
 - Rename `sql` to `query` and split up tests

## [0.2.1]
 - Fix types

## [0.2.0]
 - Pluggable `DataSource` and `RowSource`
 - Change `executeSql` signature to take an object
 - Validation

## [0.1.2]
 - `BETWEEN` operator
 - `HAVING` clause

## [0.1.1]
 - Select-list expressions

## [0.1.0]
 - Initial release
