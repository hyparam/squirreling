import { describe, expect, it } from 'vitest'
import { executeSql } from '../../src/execute/execute.js'

/** @type {null} */
const NULL = null

describe('string functions', () => {
  const source = [
    { id: 1, name: 'Alice', email: 'alice@example.com', city: 'NYC' },
    { id: 2, name: 'Bob', email: 'bob@test.com', city: 'LA' },
    { id: 3, name: 'Charlie', email: 'charlie@example.org', city: 'NYC' },
    { id: 4, name: 'diana', email: 'DIANA@EXAMPLE.COM', city: 'LA' },
  ]

  describe('UPPER', () => {
    it('should convert column values to uppercase', () => {
      const result = executeSql({ source, query: 'SELECT UPPER(name) AS upper_name FROM users' })
      expect(result).toEqual([
        { upper_name: 'ALICE' },
        { upper_name: 'BOB' },
        { upper_name: 'CHARLIE' },
        { upper_name: 'DIANA' },
      ])
    })

    it('should work without alias', () => {
      const result = executeSql({ source, query: 'SELECT UPPER(city) FROM users' })
      expect(result[0]).toHaveProperty('upper_city')
      expect(result[0].upper_city).toBe('NYC')
    })

    it('should handle mixed case input', () => {
      const result = executeSql({ source, query: 'SELECT UPPER(email) AS upper_email FROM users WHERE id = 4' })
      expect(result[0].upper_email).toBe('DIANA@EXAMPLE.COM')
    })

    it('should work with WHERE clause', () => {
      const result = executeSql({ source, query: 'SELECT name, UPPER(city) AS upper_city FROM users WHERE city = \'NYC\'' })
      expect(result).toHaveLength(2)
      expect(result.every(r => r.upper_city === 'NYC')).toBe(true)
    })

    it('should work with ORDER BY', () => {
      const result = executeSql({ source, query: 'SELECT UPPER(name) AS upper_name FROM users ORDER BY name' })
      expect(result[0].upper_name).toBe('ALICE')
      expect(result[result.length - 1].upper_name).toBe('DIANA')
    })
  })

  describe('LOWER', () => {
    it('should convert column values to lowercase', () => {
      const result = executeSql({ source, query: 'SELECT LOWER(name) AS lower_name FROM users' })
      expect(result).toEqual([
        { lower_name: 'alice' },
        { lower_name: 'bob' },
        { lower_name: 'charlie' },
        { lower_name: 'diana' },
      ])
    })

    it('should work without alias', () => {
      const result = executeSql({ source, query: 'SELECT LOWER(city) FROM users' })
      expect(result[0]).toHaveProperty('lower_city')
      expect(result[0].lower_city).toBe('nyc')
    })

    it('should handle mixed case input', () => {
      const result = executeSql({ source, query: 'SELECT LOWER(email) AS lower_email FROM users WHERE id = 4' })
      expect(result[0].lower_email).toBe('diana@example.com')
    })

    it('should work with multiple columns', () => {
      const result = executeSql({ source, query: 'SELECT id, LOWER(name) AS lower_name, LOWER(city) AS lower_city FROM users WHERE id = 1' })
      expect(result[0]).toEqual({
        id: 1,
        lower_name: 'alice',
        lower_city: 'nyc',
      })
    })
  })

  describe('CONCAT', () => {
    it('should concatenate two columns', () => {
      const data = [
        { id: 1, first_name: 'Alice', last_name: 'Smith' },
        { id: 2, first_name: 'Bob', last_name: 'Jones' },
      ]
      const result = executeSql({ source: data, query: 'SELECT CONCAT(first_name, last_name) AS full_name FROM users' })
      expect(result).toEqual([
        { full_name: 'AliceSmith' },
        { full_name: 'BobJones' },
      ])
    })

    it('should concatenate columns with string literals', () => {
      const data = [
        { id: 1, first_name: 'Alice', last_name: 'Smith' },
        { id: 2, first_name: 'Bob', last_name: 'Jones' },
      ]
      const result = executeSql({ source: data, query: 'SELECT CONCAT(first_name, \' \', last_name) AS full_name FROM users' })
      expect(result).toEqual([
        { full_name: 'Alice Smith' },
        { full_name: 'Bob Jones' },
      ])
    })

    it('should concatenate multiple columns and literals', () => {
      const result = executeSql({ source, query: 'SELECT CONCAT(name, \' (\', city, \')\') AS name_city FROM users WHERE id = 1' })
      expect(result[0].name_city).toBe('Alice (NYC)')
    })

    it('should work without alias', () => {
      const data = [{ id: 1, a: 'hello', b: 'world' }]
      const result = executeSql({ source: data, query: 'SELECT CONCAT(a, b) FROM users' })
      expect(result[0]).toHaveProperty('concat_a_b')
      expect(result[0].concat_a_b).toBe('helloworld')
    })

    it('should handle empty strings', () => {
      const data = [{ id: 1, a: '', b: 'test' }]
      const result = executeSql({ source: data, query: 'SELECT CONCAT(a, b) AS result FROM users' })
      expect(result[0].result).toBe('test')
    })
  })

  describe('LENGTH', () => {
    it('should return length of string column', () => {
      const result = executeSql({ source, query: 'SELECT LENGTH(name) AS name_length FROM users' })
      expect(result).toEqual([
        { name_length: 5 }, // Alice
        { name_length: 3 }, // Bob
        { name_length: 7 }, // Charlie
        { name_length: 5 }, // diana
      ])
    })

    it('should work without alias', () => {
      const result = executeSql({ source, query: 'SELECT LENGTH(city) FROM users' })
      expect(result[0]).toHaveProperty('length_city')
      expect(result[0].length_city).toBe(3) // NYC
    })

    it('should work with WHERE clause', () => {
      const result = executeSql({ source, query: 'SELECT name, LENGTH(name) AS name_length FROM users WHERE LENGTH(name) > 5' })
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })

    it('should work with ORDER BY', () => {
      const result = executeSql({ source, query: 'SELECT name, LENGTH(name) AS name_length FROM users ORDER BY LENGTH(name) DESC' })
      expect(result[0].name).toBe('Charlie')
      expect(result[result.length - 1].name).toBe('Bob')
    })

    it('should handle empty strings', () => {
      const data = [{ id: 1, value: '' }]
      const result = executeSql({ source: data, query: 'SELECT LENGTH(value) AS len FROM users' })
      expect(result[0].len).toBe(0)
    })
  })

  describe('SUBSTRING', () => {
    it('should extract substring with start position', () => {
      const data = [
        { id: 1, text: 'Hello World' },
        { id: 2, text: 'JavaScript' },
      ]
      const result = executeSql({ source: data, query: 'SELECT SUBSTRING(text, 1, 5) AS sub FROM users' })
      expect(result).toEqual([
        { sub: 'Hello' },
        { sub: 'JavaS' },
      ])
    })

    it('should extract substring from middle', () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = executeSql({ source: data, query: 'SELECT SUBSTRING(text, 7, 5) AS sub FROM users' })
      expect(result[0].sub).toBe('World')
    })

    it('should work without alias', () => {
      const data = [{ id: 1, text: 'Hello' }]
      const result = executeSql({ source: data, query: 'SELECT SUBSTRING(text, 1, 3) FROM users' })
      expect(result[0]).toHaveProperty('substring_text')
      expect(result[0].substring_text).toBe('Hel')
    })

    it('should handle substring beyond string length', () => {
      const data = [{ id: 1, text: 'Hi' }]
      const result = executeSql({ source: data, query: 'SELECT SUBSTRING(text, 1, 10) AS sub FROM users' })
      expect(result[0].sub).toBe('Hi')
    })

    it('should work with column names', () => {
      const result = executeSql({ source, query: 'SELECT name, SUBSTRING(email, 1, 5) AS email_prefix FROM users WHERE id = 1' })
      expect(result[0].email_prefix).toBe('alice')
    })
  })

  describe('SUBSTR', () => {
    it('should work as an alias for SUBSTRING', () => {
      const data = [
        { id: 1, text: 'Hello World' },
        { id: 2, text: 'JavaScript' },
      ]
      const result = executeSql({ source: data, query: 'SELECT SUBSTR(text, 1, 5) AS sub FROM users' })
      expect(result).toEqual([
        { sub: 'Hello' },
        { sub: 'JavaS' },
      ])
    })

    it('should extract substring from middle', () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = executeSql({ source: data, query: 'SELECT SUBSTR(text, 7, 5) AS sub FROM users' })
      expect(result[0].sub).toBe('World')
    })

    it('should work without length parameter', () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = executeSql({ source: data, query: 'SELECT SUBSTR(text, 7) AS sub FROM users' })
      expect(result[0].sub).toBe('World')
    })

    it('should work without alias', () => {
      const data = [{ id: 1, text: 'Hello' }]
      const result = executeSql({ source: data, query: 'SELECT SUBSTR(text, 1, 3) FROM users' })
      expect(result[0]).toHaveProperty('substr_text')
      expect(result[0].substr_text).toBe('Hel')
    })

    it('should handle null values', () => {
      const data = [{ id: 1, text: NULL }]
      const result = executeSql({ source: data, query: 'SELECT SUBSTR(text, 1, 5) AS sub FROM users' })
      expect(result[0].sub).toBeNull()
    })
  })

  describe('TRIM', () => {
    it('should remove leading and trailing whitespace', () => {
      const data = [
        { id: 1, text: '  hello  ' },
        { id: 2, text: '\tworld\t' },
        { id: 3, text: '\n test \n' },
      ]
      const result = executeSql({ source: data, query: 'SELECT TRIM(text) AS trimmed FROM users' })
      expect(result).toEqual([
        { trimmed: 'hello' },
        { trimmed: 'world' },
        { trimmed: 'test' },
      ])
    })

    it('should work without alias', () => {
      const data = [{ id: 1, text: '  hello  ' }]
      const result = executeSql({ source: data, query: 'SELECT TRIM(text) FROM users' })
      expect(result[0]).toHaveProperty('trim_text')
      expect(result[0].trim_text).toBe('hello')
    })

    it('should not affect strings without whitespace', () => {
      const data = [{ id: 1, text: 'hello' }]
      const result = executeSql({ source: data, query: 'SELECT TRIM(text) AS trimmed FROM users' })
      expect(result[0].trimmed).toBe('hello')
    })

    it('should preserve internal whitespace', () => {
      const data = [{ id: 1, text: '  hello world  ' }]
      const result = executeSql({ source: data, query: 'SELECT TRIM(text) AS trimmed FROM users' })
      expect(result[0].trimmed).toBe('hello world')
    })

    it('should work with WHERE clause', () => {
      const data = [
        { id: 1, name: '  Alice  ' },
        { id: 2, name: 'Bob' },
      ]
      const result = executeSql({ source: data, query: 'SELECT id, TRIM(name) AS trimmed FROM users WHERE TRIM(name) = \'Alice\'' })
      expect(result).toHaveLength(1)
      expect(result[0].id).toBe(1)
    })
  })

  describe('REPLACE', () => {
    it('should replace all occurrences of a substring', () => {
      const data = [
        { id: 1, text: 'Hello World' },
        { id: 2, text: 'foo bar foo' },
      ]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(text, \'o\', \'0\') AS replaced FROM users' })
      expect(result).toEqual([
        { replaced: 'Hell0 W0rld' },
        { replaced: 'f00 bar f00' },
      ])
    })

    it('should replace multiple character substrings', () => {
      const data = [{ id: 1, text: 'Hello World Hello' }]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(text, \'Hello\', \'Hi\') AS replaced FROM users' })
      expect(result[0].replaced).toBe('Hi World Hi')
    })

    it('should work without alias', () => {
      const data = [{ id: 1, text: 'test' }]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(text, \'t\', \'T\') FROM users' })
      expect(result[0]).toHaveProperty('replace_text')
      expect(result[0].replace_text).toBe('TesT')
    })

    it('should handle empty replacement string', () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(text, \' \', \'\') AS replaced FROM users' })
      expect(result[0].replaced).toBe('HelloWorld')
    })

    it('should handle search string not found', () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(text, \'xyz\', \'abc\') AS replaced FROM users' })
      expect(result[0].replaced).toBe('Hello World')
    })

    it('should work with column values', () => {
      const data = [
        { id: 1, email: 'alice@example.com', old_domain: 'example.com', new_domain: 'test.org' },
      ]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(email, old_domain, new_domain) AS new_email FROM users' })
      expect(result[0].new_email).toBe('alice@test.org')
    })

    it('should work with WHERE clause', () => {
      const data = [
        { id: 1, text: 'apple banana' },
        { id: 2, text: 'grape orange' },
      ]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(text, \'a\', \'@\') AS replaced FROM users WHERE text LIKE \'%apple%\'' })
      expect(result).toHaveLength(1)
      expect(result[0].replaced).toBe('@pple b@n@n@')
    })

    it('should work with ORDER BY', () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Carol' },
      ]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(name, \'o\', \'0\') AS replaced FROM users ORDER BY name' })
      expect(result[0].replaced).toBe('Alice')
      expect(result[1].replaced).toBe('B0b')
      expect(result[2].replaced).toBe('Car0l')
    })

    it('should be case-sensitive', () => {
      const data = [{ id: 1, text: 'Hello hello HELLO' }]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(text, \'hello\', \'hi\') AS replaced FROM users' })
      expect(result[0].replaced).toBe('Hello hi HELLO')
    })

    it('should handle null values in first argument', () => {
      const data = [{ id: 1, text: NULL }]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(text, \'a\', \'b\') AS replaced FROM users' })
      expect(result[0].replaced).toBeNull()
    })

    it('should handle null values in second argument', () => {
      const data = [{ id: 1, text: 'hello', search: NULL }]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(text, search, \'x\') AS replaced FROM users' })
      expect(result[0].replaced).toBeNull()
    })

    it('should handle null values in third argument', () => {
      const data = [{ id: 1, text: 'hello', replacement: NULL }]
      const result = executeSql({ source: data, query: 'SELECT REPLACE(text, \'l\', replacement) AS replaced FROM users' })
      expect(result[0].replaced).toBeNull()
    })
  })

  describe('combined string functions', () => {
    it('should use multiple different string functions in one query', () => {
      const result = executeSql({ source, query: 'SELECT UPPER(name) AS upper_name, LOWER(city) AS lower_city, LENGTH(email) AS email_len FROM users WHERE id = 1' })
      expect(result[0]).toEqual({
        upper_name: 'ALICE',
        lower_city: 'nyc',
        email_len: 17,
      })
    })

    it('should work with regular columns', () => {
      const result = executeSql({ source, query: 'SELECT id, name, UPPER(city) AS upper_city FROM users ORDER BY id LIMIT 2' })
      expect(result).toHaveLength(2)
      expect(result[0]).toEqual({ id: 1, name: 'Alice', upper_city: 'NYC' })
      expect(result[1]).toEqual({ id: 2, name: 'Bob', upper_city: 'LA' })
    })

    it('should work with DISTINCT', () => {
      const result = executeSql({ source, query: 'SELECT DISTINCT UPPER(city) AS upper_city FROM users' })
      expect(result).toHaveLength(2)
      const cities = result.map(r => r.upper_city).sort()
      expect(cities).toEqual(['LA', 'NYC'])
    })
  })

  describe('string functions with GROUP BY', () => {
    it('should work with GROUP BY and aggregates', () => {
      const result = executeSql({ source, query: 'SELECT UPPER(city) AS upper_city, COUNT(*) AS count FROM users GROUP BY city' })
      expect(result).toHaveLength(2)
      const nycGroup = result.find(r => r.upper_city === 'NYC')
      expect(nycGroup?.count).toBe(2)
    })

    it('should group by string function result', () => {
      const data = [
        { id: 1, name: 'alice', value: 10 },
        { id: 2, name: 'ALICE', value: 20 },
        { id: 3, name: 'bob', value: 30 },
      ]
      const result = executeSql({ source: data, query: 'SELECT UPPER(name) AS upper_name, SUM(value) AS total FROM users GROUP BY UPPER(name)' })
      expect(result).toHaveLength(2)
      const aliceGroup = result.find(r => r.upper_name === 'ALICE')
      expect(aliceGroup?.total).toBe(30)
    })
  })

  describe('null handling', () => {
    it('should handle null values in UPPER', () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: null },
      ]
      const result = executeSql({ source: data, query: 'SELECT UPPER(name) AS upper_name FROM users' })
      expect(result[1].upper_name).toBeNull()
    })

    it('should handle null values in LOWER', () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: null },
      ]
      const result = executeSql({ source: data, query: 'SELECT LOWER(name) AS lower_name FROM users' })
      expect(result[1].lower_name).toBeNull()
    })

    it('should handle null values in CONCAT', () => {
      const data = [{ id: 1, first: 'Alice', last: NULL }]
      const result = executeSql({ source: data, query: 'SELECT CONCAT(first, last) AS full FROM users' })
      expect(result[0].full).toBeNull()
    })

    it('should handle null values in LENGTH', () => {
      const data = [{ id: 1, text: NULL }]
      const result = executeSql({ source: data, query: 'SELECT LENGTH(text) AS len FROM users' })
      expect(result[0].len).toBeNull()
    })

    it('should handle null values in SUBSTRING', () => {
      const data = [{ id: 1, text: NULL }]
      const result = executeSql({ source: data, query: 'SELECT SUBSTRING(text, 1, 5) AS sub FROM users' })
      expect(result[0].sub).toBeNull()
    })

    it('should handle null values in TRIM', () => {
      const data = [{ id: 1, text: NULL }]
      const result = executeSql({ source: data, query: 'SELECT TRIM(text) AS trimmed FROM users' })
      expect(result[0].trimmed).toBeNull()
    })
  })
})
