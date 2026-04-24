import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'
import { ParseError } from '../../src/validation/parseErrors.js'

describe('parseSql error handling', () => {
  describe('basic syntax errors', () => {
    it('should throw error on non-SELECT query', () => {
      expect(() => parseSql({ query: '' }))
        .toThrow('Expected SELECT but found end of query at position 0')
      expect(() => parseSql({ query: 'hi' }))
        .toThrow('Expected SELECT but found "hi" at position 0')
      expect(() => parseSql({ query: 'WHERE true' }))
        .toThrow('Expected SELECT but found "WHERE" at position 0')
    })

    it('should throw error on incomplete SELECT statement', () => {
      expect(() => parseSql({ query: 'SELECT name' }))
        .toThrow('Expected FROM after "name" but found end of query at position 11')
      expect(() => parseSql({ query: 'SELECT *' }))
        .toThrow('Expected FROM after "*" but found end of query at position 8')
      expect(() => parseSql({ query: 'SELECT * FROM' }))
        .toThrow('Expected identifier after "FROM" but found end of query at position 13')
      // Test lowercase as well
      expect(() => parseSql({ query: 'select * from' }))
        .toThrow('Expected identifier after "from" but found end of query at position 13')
    })

    it('should throw error on invalid table name after FROM', () => {
      expect(() => parseSql({ query: 'SELECT * FROM 3' }))
        .toThrow('Expected identifier after "FROM" but found "3" at position 14')
    })

    it('should throw error on dangling comma', () => {
      expect(() => parseSql({ query: 'SELECT name,' }))
        .toThrow('Expected expression after "," but found end of query at position 12')
      expect(() => parseSql({ query: 'SELECT name, FROM users' }))
        .toThrow('Expected expression after "," but found "FROM" at position 13')
      expect(() => parseSql({ query: 'SELECT name FROM users,' }))
        .toThrow('Comma-separated FROM is only supported with table functions like UNNEST; use explicit JOIN ... ON ... for regular tables')
    })

    it('should throw error on illegal keywords after SELECT', () => {
      expect(() => parseSql({ query: 'SELECT where FROM users' }))
        .toThrow('Expected expression after "SELECT" but found "WHERE" at position 7')
      expect(() => parseSql({ query: 'SELECT WHERE * FROM users' }))
        .toThrow('Expected expression after "SELECT" but found "WHERE" at position 7')
      expect(() => parseSql({ query: 'SELECT join FROM users' }))
        .toThrow('Expected expression after "SELECT" but found "JOIN" at position 7')
      expect(() => parseSql({ query: 'SELECT JOIN * FROM users' }))
        .toThrow('Expected expression after "SELECT" but found "JOIN" at position 7')
    })

    it('should throw error on empty query', () => {
      expect(() => parseSql({ query: '' }))
        .toThrow('Expected SELECT but found end of query at position 0')
    })

    it('should throw error on nonsense', () => {
      expect(() => parseSql({ query: '@' }))
        .toThrow('Expected SELECT but found "@" at position 0')
      expect(() => parseSql({ query: ' #' }))
        .toThrow('Expected SELECT but found "#" at position 1')
      expect(() => parseSql({ query: '.' }))
        .toThrow('Expected SELECT but found "." at position 0')
    })

    it('should throw error on unexpected tokens after query', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users; SELECT' }))
        .toThrow('Expected end of query after ";" but found "SELECT" at position 21')
    })
  })

  describe('SELECT list errors', () => {
    it('should throw error on invalid column name', () => {
      expect(() => parseSql({ query: 'SELECT FROM users' }))
        .toThrow('Expected expression after "SELECT" but found "FROM" at position 7')
    })

    it('should throw error on missing column after comma', () => {
      expect(() => parseSql({ query: 'SELECT name, FROM users' }))
        .toThrow('Expected expression after "," but found "FROM" at position 13')
    })

    it('should throw error on missing alias after AS', () => {
      expect(() => parseSql({ query: 'SELECT name AS FROM users' }))
        .toThrow('Expected alias after "AS" but found "FROM" at position 15')
    })

    it('should throw error on missing alias after lowercase as', () => {
      expect(() => parseSql({ query: 'SELECT name as FROM users' }))
        .toThrow('Expected alias after "as" but found "FROM" at position 15')
    })

    it('should throw error on invalid function call syntax', () => {
      expect(() => parseSql({ query: 'SELECT name.id() FROM users' }))
        .toThrow('Expected FROM after "id" but found "(" at position 14')
    })
  })

  describe('aggregate function errors', () => {
    it('should throw error on missing opening paren in aggregate', () => {
      expect(() => parseSql({ query: 'SELECT COUNT name myalias FROM users' }))
        .toThrow('Expected FROM after "name" but found "myalias" at position 18')
    })

    it('should throw error on missing closing paren in aggregate', () => {
      expect(() => parseSql({ query: 'SELECT COUNT(name FROM users' }))
        .toThrow('Expected ) after "name" but found "FROM" at position 18')
    })

    it('should throw error on empty aggregate function', () => {
      expect(() => parseSql({ query: 'SELECT COUNT() FROM users' }))
        .toThrow('COUNT(expression) function requires 1 argument, got 0')
    })

    it('should throw error on missing aggregate argument', () => {
      expect(() => parseSql({ query: 'SELECT SUM() FROM users' }))
        .toThrow('SUM(expression) function requires 1 argument, got 0')
    })

    it('should throw error when expecting closing paren in aggregate', () => {
      expect(() => parseSql({ query: 'SELECT COUNT(name( FROM users' }))
        .toThrow('Unknown function "name" at position 13')
    })

    it('should throw error for SUM(*)', () => {
      expect(() => parseSql({ query: 'SELECT SUM(*) FROM users' }))
        .toThrow('SUM cannot be applied to "*"')
    })

    it('should throw error for AVG(*)', () => {
      expect(() => parseSql({ query: 'SELECT AVG(*) FROM users' }))
        .toThrow('AVG cannot be applied to "*"')
    })

    it('should throw error for MIN(*)', () => {
      expect(() => parseSql({ query: 'SELECT MIN(*) FROM users' }))
        .toThrow('MIN cannot be applied to "*"')
    })

    it('should throw error for MAX(*)', () => {
      expect(() => parseSql({ query: 'SELECT MAX(*) FROM users' }))
        .toThrow('MAX cannot be applied to "*"')
    })

    it('should throw error for JSON_ARRAYAGG(*)', () => {
      expect(() => parseSql({ query: 'SELECT JSON_ARRAYAGG(*) FROM users' }))
        .toThrow('JSON_ARRAYAGG cannot be applied to "*"')
    })

    it('should throw error for STDDEV_POP(*)', () => {
      expect(() => parseSql({ query: 'SELECT STDDEV_POP(*) FROM users' }))
        .toThrow('STDDEV_POP cannot be applied to "*"')
    })

    it('should throw error for COUNT(DISTINCT *)', () => {
      expect(() => parseSql({ query: 'SELECT COUNT(DISTINCT *) FROM users' }))
        .toThrow('COUNT(DISTINCT *) is not allowed')
    })

    it('should throw error for FILTER clause on non-aggregate function', () => {
      expect(() => parseSql({ query: 'SELECT UPPER(name) FILTER (WHERE name > \'a\') FROM users' }))
        .toThrow('FILTER cannot be applied to non-aggregate function "UPPER"')
    })
  })

  describe('window function errors', () => {
    it('should throw error for COUNT with OVER', () => {
      expect(() => parseSql({ query: 'SELECT COUNT(*) OVER () FROM t' }))
        .toThrow('Window functions are not supported: COUNT(...) OVER (...)')
    })

    it('should throw error for SUM with OVER', () => {
      expect(() => parseSql({ query: 'SELECT SUM(x) OVER () FROM t' }))
        .toThrow('Window functions are not supported: SUM(...) OVER (...)')
    })

    it('should throw error for AVG with OVER and PARTITION BY', () => {
      expect(() => parseSql({ query: 'SELECT AVG(x) OVER (PARTITION BY y) FROM t' }))
        .toThrow('Window functions are not supported: AVG(...) OVER (...)')
    })

    it('should throw error for OVER after FILTER', () => {
      expect(() => parseSql({ query: 'SELECT COUNT(*) FILTER (WHERE x > 0) OVER () FROM t' }))
        .toThrow('Window functions are not supported: COUNT(...) OVER (...)')
    })

    it('should throw error for ROW_NUMBER window function', () => {
      expect(() => parseSql({ query: 'SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t' }))
        .toThrow('Window function "ROW_NUMBER" is not supported')
    })

    it('should throw error for RANK window function', () => {
      expect(() => parseSql({ query: 'SELECT RANK() OVER (ORDER BY id) FROM t' }))
        .toThrow('Window function "RANK" is not supported')
    })

    it('should throw error for DENSE_RANK window function', () => {
      expect(() => parseSql({ query: 'SELECT DENSE_RANK() OVER (ORDER BY id) FROM t' }))
        .toThrow('Window function "DENSE_RANK" is not supported')
    })

    it('should throw error for LAG window function', () => {
      expect(() => parseSql({ query: 'SELECT LAG(x) OVER (ORDER BY id) FROM t' }))
        .toThrow('Window function "LAG" is not supported')
    })

    it('should throw error for LEAD window function', () => {
      expect(() => parseSql({ query: 'SELECT LEAD(x) OVER (ORDER BY id) FROM t' }))
        .toThrow('Window function "LEAD" is not supported')
    })

    it('should throw error for NTILE window function', () => {
      expect(() => parseSql({ query: 'SELECT NTILE(4) OVER (ORDER BY id) FROM t' }))
        .toThrow('Window function "NTILE" is not supported')
    })
  })

  describe('string function errors', () => {
    it('should throw error on missing opening paren in string function', () => {
      expect(() => parseSql({ query: 'SELECT UPPER name myalias FROM users' }))
        .toThrow('Expected FROM after "name" but found "myalias" at position 18')
    })

    it('should throw error on missing closing paren in string function', () => {
      expect(() => parseSql({ query: 'SELECT UPPER(name FROM users' }))
        .toThrow('Expected ) after "name" but found "FROM" at position 18')
    })

    it('should throw error on invalid string function name', () => {
      expect(() => parseSql({ query: 'SELECT FOOBAR(name) FROM users' }))
        .toThrow('Unknown function "FOOBAR" at position 7')
    })

    it('should suggest similar functions with shared prefix', () => {
      expect(() => parseSql({ query: 'SELECT JSON_EXTRACT_STRING(name) FROM users' }))
        .toThrow('Unknown function "JSON_EXTRACT_STRING" at position 7. Did you mean JSON_EXTRACT, JSON_ARRAY_LENGTH, JSON_ARRAYAGG, JSON_OBJECT?')
    })

    it('should suggest similar functions by edit distance', () => {
      expect(() => parseSql({ query: 'SELECT UPER(name) FROM users' }))
        .toThrow('Unknown function "UPER" at position 7. Did you mean UPPER, LOWER, POWER, POW?')
    })
  })

  describe('CAST errors', () => {
    it('should throw error for unsupported CAST type', () => {
      expect(() => parseSql({ query: 'SELECT CAST(x AS BINARY) FROM t' }))
        .toThrow('Expected cast type (STRING, INT, BIGINT, FLOAT, BOOL) after "AS" but found "BINARY" at position 17')
    })
  })

  describe('WHERE clause errors', () => {
    it('should throw error on incomplete WHERE clause', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users WHERE' }))
        .toThrow('Expected expression after "WHERE" but found end of query at position 25')
    })

    it('should throw error on dangling NOT in WHERE clause', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users WHERE age NOT ORDER BY id' }))
        .toThrow('Expected LIKE, BETWEEN, or IN after "NOT" but found "ORDER" at position 34')
    })

    it('should throw error on dangling NOT in WHERE comparison', () => {
      expect(() => parseSql({ query: 'SELECT * FROM t WHERE x NOT y' }))
        .toThrow('Expected LIKE, BETWEEN, or IN after "NOT" but found "y" at position 28')
    })

    it('should throw error on missing closing paren in WHERE', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users WHERE (age > 18' }))
        .toThrow('Expected ) after "18" but found end of query at position 35')
    })

    it('should throw error on incomplete comparison in WHERE', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users WHERE age >' }))
        .toThrow('Expected expression after ">" but found end of query at position 31')
    })

    it('should throw error when expecting closing paren in WHERE', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users WHERE (age > 18(' }))
        .toThrow('Expected ) after "18" but found "(" at position 35')
    })

    it('should throw error for aggregate function in WHERE clause', () => {
      expect(() => parseSql({ query: 'SELECT name FROM users WHERE SUM(age) > 10' }))
        .toThrow('Aggregate function SUM is not allowed in WHERE clause')
    })
  })

  describe('JOIN errors', () => {
    it('should throw error on missing table name after JOIN', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users JOIN' }))
        .toThrow('Expected identifier after "JOIN" but found end of query at position 24')
    })

    it('should throw error on missing ON keyword in JOIN', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users JOIN orders' }))
        .toThrow('Expected ON after "orders" but found end of query at position 31')
    })

    it('should throw error on missing JOIN condition', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users JOIN orders ON' }))
        .toThrow('Expected expression after "ON" but found end of query at position 34')
    })

    it('should throw error on missing JOIN keyword after LEFT', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users LEFT orders' }))
        .toThrow('Expected JOIN after "LEFT" but found "orders" at position 25')
    })

    it('should throw error on missing JOIN keyword after INNER', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users INNER orders' }))
        .toThrow('Expected JOIN after "INNER" but found "orders" at position 26')
    })

    it('should throw error for aggregate function in JOIN ON', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users JOIN orders ON COUNT(users.id) = orders.user_id' }))
        .toThrow('Aggregate function COUNT is not allowed in JOIN ON clause')
    })
  })

  describe('GROUP BY errors', () => {
    it('should throw error on missing BY after GROUP', () => {
      expect(() => parseSql({ query: 'SELECT COUNT(*) FROM users GROUP' }))
        .toThrow('Expected BY after "GROUP" but found end of query at position 32')
    })

    it('should throw error on missing column after GROUP BY', () => {
      expect(() => parseSql({ query: 'SELECT COUNT(*) FROM users GROUP BY' }))
        .toThrow('Expected expression after "BY" but found end of query at position 35')
    })

    it('should throw error on missing column after comma in GROUP BY', () => {
      expect(() => parseSql({ query: 'SELECT COUNT(*) FROM users GROUP BY age,' }))
        .toThrow('Expected expression after "," but found end of query at position 40')
    })

    it('should throw error for aggregate function in GROUP BY', () => {
      expect(() => parseSql({ query: 'SELECT name FROM users GROUP BY COUNT(name)' }))
        .toThrow('Aggregate function COUNT is not allowed in GROUP BY clause')
    })
  })

  describe('ORDER BY errors', () => {
    it('should throw error on missing BY after ORDER', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users ORDER' }))
        .toThrow('Expected BY after "ORDER" but found end of query at position 25')
    })

    it('should throw error on missing column after ORDER BY', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users ORDER BY' }))
        .toThrow('Expected expression after "BY" but found end of query at position 28')
    })

    it('should throw error on missing column after comma in ORDER BY', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users ORDER BY age,' }))
        .toThrow('Expected expression after "," but found end of query at position 33')
    })

    it('should throw error for aggregate function in ORDER BY without aggregate context', () => {
      expect(() => parseSql({ query: 'SELECT name FROM users ORDER BY COUNT(name)' }))
        .toThrow('Aggregate function COUNT is not allowed in ORDER BY clause')
    })
  })

  describe('LIMIT/OFFSET errors', () => {
    it('should throw error on invalid LIMIT', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users LIMIT abc' }))
        .toThrow('Expected positive integer LIMIT')
    })

    it('should throw error on missing LIMIT value', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users LIMIT' }))
        .toThrow('Expected positive integer LIMIT')
    })

    it('should throw error on invalid OFFSET', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users OFFSET xyz' }))
        .toThrow('Expected positive integer OFFSET')
    })

    it('should throw error on missing OFFSET value', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users OFFSET' }))
        .toThrow('Expected positive integer OFFSET')
    })

    it('should throw error on invalid OFFSET after LIMIT', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users LIMIT 10 OFFSET abc' }))
        .toThrow('Expected positive integer OFFSET')
    })

    it('should throw error on negative LIMIT', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users LIMIT -1' }))
        .toThrow('Expected positive integer LIMIT value')
    })

    it('should throw error on negative OFFSET', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users OFFSET -1' }))
        .toThrow('Expected positive integer OFFSET value')
    })

    it('should throw error on negative OFFSET after LIMIT', () => {
      expect(() => parseSql({ query: 'SELECT * FROM users LIMIT 10 OFFSET -5' }))
        .toThrow('Expected positive integer OFFSET value')
    })
  })
})

describe('ParseError structure', () => {
  it('should throw ParseError with positionStart and positionEnd', () => {
    try {
      parseSql({ query: 'WHERE true' })
      expect.fail('should have thrown')
    } catch (/** @type {any} */ error) {
      expect(error).toBeInstanceOf(ParseError)
      expect(error).toBeInstanceOf(Error)
      expect(error.name).toBe('SyntaxError')
      expect(error.positionStart).toBe(0)
      expect(error.positionEnd).toBe(5) // "WHERE" is 5 chars
      expect(error.message).toBe('Expected SELECT but found "WHERE" at position 0')
    }
  })

  it('should have correct position range for syntax errors', () => {
    try {
      parseSql({ query: 'SELECT * FROM' })
      expect.fail('should have thrown')
    } catch (/** @type {any} */ error) {
      expect(error).toBeInstanceOf(ParseError)
      expect(error.positionStart).toBe(13)
      expect(error.positionEnd).toBe(13) // EOF has same start/end
    }
  })

  it('should throw error for unknown functions without functions parameter', () => {
    expect(() => parseSql({ query: 'SELECT FOOBAR(x) FROM t' })).toThrow('Unknown function "FOOBAR" at position 7')
  })

  it('should allow unknown functions when provided in functions parameter', () => {
    const result = parseSql({
      query: 'SELECT FOOBAR(x) FROM t',
      functions: {
        FOOBAR: {
          apply: (x) => x,
          arguments: { min: 1, max: 1 },
        },
      },
    })
    expect(result).toEqual({
      type: 'select',
      columns: [
        {
          type: 'derived',
          expr: {
            type: 'function',
            funcName: 'FOOBAR',
            args: [
              { type: 'identifier', name: 'x', positionStart: 14, positionEnd: 15 },
            ],
            positionStart: 7,
            positionEnd: 16,
          },
          positionStart: 7,
          positionEnd: 16,
        },
      ],
      distinct: false,
      from: {
        type: 'table',
        table: 't',
        positionStart: 22,
        positionEnd: 23,
      },
      groupBy: [],
      joins: [],
      orderBy: [],
      positionStart: 0,
      positionEnd: 23,
    })
  })

  it('should have correct position range for unexpected character', () => {
    try {
      parseSql({ query: '@' })
      expect.fail('should have thrown')
    } catch (/** @type {any} */ error) {
      expect(error).toBeInstanceOf(ParseError)
      expect(error.positionStart).toBe(0)
      expect(error.positionEnd).toBe(1) // single char
    }
  })
})
