import { describe, expect, it } from 'vitest'
import { extractTables } from '../../src/parse/extractTables.js'
import { parseSql } from '../../src/parse/parse.js'

/**
 * @param {string} query
 * @returns {string[]}
 */
function tablesIn(query) {
  return extractTables(parseSql({ query }))
}

describe('extractTables', () => {
  it('returns the FROM table for a simple SELECT', () => {
    expect(tablesIn('SELECT * FROM users')).toEqual(['users'])
  })

  it('returns FROM and JOIN tables in first-seen order', () => {
    expect(tablesIn('SELECT * FROM a JOIN b ON a.id = b.aid LEFT JOIN c ON c.bid = b.id'))
      .toEqual(['a', 'b', 'c'])
  })

  it('deduplicates self-joins', () => {
    expect(tablesIn('SELECT * FROM a x JOIN a y ON x.id = y.parent_id'))
      .toEqual(['a'])
  })

  it('preserves the original case of identifiers', () => {
    expect(tablesIn('SELECT * FROM "MyTable" JOIN OtherTable ON 1 = 1'))
      .toEqual(['MyTable', 'OtherTable'])
  })

  it('descends into a derived table (subquery in FROM)', () => {
    expect(tablesIn('SELECT * FROM (SELECT id FROM inner_t) sub'))
      .toEqual(['inner_t'])
  })

  it('finds tables inside a WHERE IN (SELECT ...) subquery', () => {
    expect(tablesIn('SELECT * FROM a WHERE a.id IN (SELECT b_id FROM b)'))
      .toEqual(['a', 'b'])
  })

  it('finds tables inside an EXISTS subquery', () => {
    expect(tablesIn('SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = a.id)'))
      .toEqual(['a', 'b'])
  })

  it('finds tables inside a NOT EXISTS subquery', () => {
    expect(tablesIn('SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b)'))
      .toEqual(['a', 'b'])
  })

  it('finds tables inside a scalar subquery in the SELECT list', () => {
    expect(tablesIn('SELECT (SELECT MAX(x) FROM b) AS m FROM a'))
      .toEqual(['a', 'b'])
  })

  it('finds tables inside HAVING and GROUP BY subqueries', () => {
    expect(tablesIn('SELECT k, COUNT(*) FROM a GROUP BY k HAVING COUNT(*) > (SELECT MIN(c) FROM b)'))
      .toEqual(['a', 'b'])
  })

  it('walks both branches of a UNION', () => {
    expect(tablesIn('SELECT id FROM a UNION ALL SELECT id FROM b'))
      .toEqual(['a', 'b'])
  })

  it('walks INTERSECT and EXCEPT compounds', () => {
    expect(tablesIn('SELECT id FROM a INTERSECT SELECT id FROM b EXCEPT SELECT id FROM c'))
      .toEqual(['a', 'b', 'c'])
  })

  it('skips CTE names and reports only their underlying tables', () => {
    expect(tablesIn('WITH cte AS (SELECT * FROM users) SELECT * FROM cte'))
      .toEqual(['users'])
  })

  it('skips CTE references made by sibling CTEs', () => {
    expect(tablesIn('WITH foo AS (SELECT * FROM users), bar AS (SELECT * FROM foo) SELECT * FROM bar'))
      .toEqual(['users'])
  })

  it('matches CTE names case-insensitively', () => {
    expect(tablesIn('WITH Cte AS (SELECT * FROM users) SELECT * FROM CTE'))
      .toEqual(['users'])
  })

  it('does not let a nested WITH leak its CTE names as tables', () => {
    expect(tablesIn('WITH outer_cte AS (WITH inner_cte AS (SELECT * FROM users) SELECT * FROM inner_cte) SELECT * FROM outer_cte'))
      .toEqual(['users'])
  })

  it('reports a CTE-shadowed name as a table when the CTE is out of scope', () => {
    // The outer SELECT references `users` directly; the EXISTS subquery has
    // its own WITH that defines a CTE also named `users`. That CTE is not
    // visible outside its own scope, so the outer reference is a real table.
    expect(tablesIn('SELECT * FROM users WHERE EXISTS (WITH users AS (SELECT * FROM other) SELECT * FROM users)'))
      .toEqual(['users', 'other'])
  })

  it('walks join ON conditions for subqueries', () => {
    expect(tablesIn('SELECT * FROM a JOIN b ON a.id = (SELECT MAX(x) FROM c)'))
      .toEqual(['a', 'b', 'c'])
  })

  it('walks compound ORDER BY for subqueries', () => {
    expect(tablesIn('SELECT id FROM a UNION SELECT id FROM b ORDER BY (SELECT MIN(x) FROM c)'))
      .toEqual(['a', 'b', 'c'])
  })

  it('walks CASE expressions', () => {
    expect(tablesIn('SELECT CASE WHEN x IN (SELECT y FROM b) THEN 1 ELSE (SELECT z FROM c) END FROM a'))
      .toEqual(['a', 'b', 'c'])
  })

  it('walks function arguments and FILTER clauses', () => {
    expect(tablesIn('SELECT COUNT(*) FILTER (WHERE id IN (SELECT id FROM b)) FROM a'))
      .toEqual(['a', 'b'])
  })

  it('walks CAST expressions', () => {
    expect(tablesIn('SELECT CAST((SELECT x FROM b) AS INTEGER) FROM a'))
      .toEqual(['a', 'b'])
  })

})
