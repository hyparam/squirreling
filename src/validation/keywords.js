export const KEYWORDS = new Set([
  'WITH', 'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IS', 'GROUP', 'BY',
  'HAVING', 'ORDER', 'ASC', 'DESC', 'NULLS', 'LIMIT', 'OFFSET', 'AS', 'ALL',
  'DISTINCT', 'TRUE', 'FALSE', 'NULL', 'LIKE', 'IN', 'EXISTS', 'BETWEEN',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'JOIN', 'INNER', 'LEFT', 'RIGHT',
  'FULL', 'OUTER', 'POSITIONAL', 'ON', 'INTERVAL', 'DAY', 'MONTH', 'YEAR',
  'HOUR', 'MINUTE', 'SECOND', 'FILTER',
  'UNION', 'INTERSECT', 'EXCEPT',
])

// Reserved keywords that cannot be used as identifiers in expressions.
// Non-reserved keywords (e.g. DAY, MONTH, FILTER, ASC) can be used as column alias references.
export const RESERVED_KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'WITH',
  'AND', 'OR', 'NOT', 'IS', 'LIKE', 'IN', 'BETWEEN',
  'TRUE', 'FALSE', 'NULL',
  'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INTERVAL',
  'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
  'AS', 'ALL', 'DISTINCT',
  'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'ON',
  'UNION', 'INTERSECT', 'EXCEPT',
])

// Keywords that cannot be used as implicit aliases after a column
export const RESERVED_AFTER_COLUMN = new Set([
  'FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
  'UNION', 'INTERSECT', 'EXCEPT',
])

// Keywords that cannot be used as table aliases
export const RESERVED_AFTER_TABLE = new Set([
  'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'JOIN', 'INNER',
  'LEFT', 'RIGHT', 'FULL', 'CROSS', 'ON', 'POSITIONAL',
  'UNION', 'INTERSECT', 'EXCEPT',
])
