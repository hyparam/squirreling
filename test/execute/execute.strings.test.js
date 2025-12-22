import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/** @type {null} */
const NULL = null

describe('string functions', () => {
  const users = [
    { id: 1, name: 'Alice', email: 'alice@example.com', city: 'NYC' },
    { id: 2, name: 'Bob', email: 'bob@test.com', city: 'LA' },
    { id: 3, name: 'Charlie', email: 'charlie@example.org', city: 'NYC' },
    { id: 4, name: 'diana', email: 'DIANA@EXAMPLE.COM', city: 'LA' },
  ]

  describe('UPPER', () => {
    it('should convert column values to uppercase', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT UPPER(name) AS upper_name FROM users',
      }))
      expect(result).toEqual([
        { upper_name: 'ALICE' },
        { upper_name: 'BOB' },
        { upper_name: 'CHARLIE' },
        { upper_name: 'DIANA' },
      ])
    })

    it('should work without alias', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT UPPER(city) FROM users',
      }))
      expect(result[0]).toHaveProperty('upper_city')
      expect(result[0].upper_city).toBe('NYC')
    })

    it('should handle mixed case input', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT UPPER(email) AS upper_email FROM users WHERE id = 4',
      }))
      expect(result[0].upper_email).toBe('DIANA@EXAMPLE.COM')
    })

    it('should work with WHERE clause', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, UPPER(city) AS upper_city FROM users WHERE city = \'NYC\'',
      }))
      expect(result).toHaveLength(2)
      expect(result.every(r => r.upper_city === 'NYC')).toBe(true)
    })

    it('should work with ORDER BY', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT UPPER(name) AS upper_name FROM users ORDER BY name',
      }))
      expect(result[0].upper_name).toBe('ALICE')
      expect(result[result.length - 1].upper_name).toBe('DIANA')
    })
  })

  describe('LOWER', () => {
    it('should convert column values to lowercase', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT LOWER(name) AS lower_name FROM users',
      }))
      expect(result).toEqual([
        { lower_name: 'alice' },
        { lower_name: 'bob' },
        { lower_name: 'charlie' },
        { lower_name: 'diana' },
      ])
    })

    it('should work without alias', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT LOWER(city) FROM users',
      }))
      expect(result[0]).toHaveProperty('lower_city')
      expect(result[0].lower_city).toBe('nyc')
    })

    it('should handle mixed case input', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT LOWER(email) AS lower_email FROM users WHERE id = 4',
      }))
      expect(result[0].lower_email).toBe('diana@example.com')
    })

    it('should work with multiple columns', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT id, LOWER(name) AS lower_name, LOWER(city) AS lower_city FROM users WHERE id = 1',
      }))
      expect(result[0]).toEqual({
        id: 1,
        lower_name: 'alice',
        lower_city: 'nyc',
      })
    })
  })

  describe('CONCAT', () => {
    it('should concatenate two columns', async () => {
      const users = [
        { id: 1, first_name: 'Alice', last_name: 'Smith' },
        { id: 2, first_name: 'Bob', last_name: 'Jones' },
      ]
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT CONCAT(first_name, last_name) AS full_name FROM users',
      }))
      expect(result).toEqual([
        { full_name: 'AliceSmith' },
        { full_name: 'BobJones' },
      ])
    })

    it('should concatenate columns with string literals', async () => {
      const users = [
        { id: 1, first_name: 'Alice', last_name: 'Smith' },
        { id: 2, first_name: 'Bob', last_name: 'Jones' },
      ]
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT CONCAT(first_name, \' \', last_name) AS full_name FROM users',
      }))
      expect(result).toEqual([
        { full_name: 'Alice Smith' },
        { full_name: 'Bob Jones' },
      ])
    })

    it('should concatenate multiple columns and literals', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT CONCAT(name, \' (\', city, \')\') AS name_city FROM users WHERE id = 1',
      }))
      expect(result[0].name_city).toBe('Alice (NYC)')
    })

    it('should work without alias', async () => {
      const data = [{ id: 1, a: 'hello', b: 'world' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT CONCAT(a, b) FROM data',
      }))
      expect(result[0]).toHaveProperty('concat_a_b')
      expect(result[0].concat_a_b).toBe('helloworld')
    })

    it('should handle empty strings', async () => {
      const data = [{ id: 1, a: '', b: 'test' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT CONCAT(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe('test')
    })
  })

  describe('LENGTH', () => {
    it('should return length of string column', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT LENGTH(name) AS name_length FROM users',
      }))
      expect(result).toEqual([
        { name_length: 5 }, // Alice
        { name_length: 3 }, // Bob
        { name_length: 7 }, // Charlie
        { name_length: 5 }, // diana
      ])
    })

    it('should work without alias', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT LENGTH(city) FROM users',
      }))
      expect(result[0]).toHaveProperty('length_city')
      expect(result[0].length_city).toBe(3) // NYC
    })

    it('should work with WHERE clause', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, LENGTH(name) AS name_length FROM users WHERE LENGTH(name) > 5',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Charlie')
    })

    it('should work with ORDER BY', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, LENGTH(name) AS name_length FROM users ORDER BY LENGTH(name) DESC',
      }))
      expect(result[0].name).toBe('Charlie')
      expect(result[result.length - 1].name).toBe('Bob')
    })

    it('should handle empty strings', async () => {
      const data = [{ id: 1, value: '' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LENGTH(value) AS len FROM data',
      }))
      expect(result[0].len).toBe(0)
    })
  })

  describe('SUBSTRING', () => {
    it('should extract substring with start position', async () => {
      const data = [
        { id: 1, text: 'Hello World' },
        { id: 2, text: 'JavaScript' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTRING(text, 1, 5) AS sub FROM data',
      }))
      expect(result).toEqual([
        { sub: 'Hello' },
        { sub: 'JavaS' },
      ])
    })

    it('should extract substring from middle', async () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTRING(text, 7, 5) AS sub FROM data',
      }))
      expect(result[0].sub).toBe('World')
    })

    it('should work without alias', async () => {
      const data = [{ id: 1, text: 'Hello' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTRING(text, 1, 3) FROM data',
      }))
      expect(result[0]).toHaveProperty('substring_text_1_3')
      expect(result[0].substring_text_1_3).toBe('Hel')
    })

    it('should handle substring beyond string length', async () => {
      const data = [{ id: 1, text: 'Hi' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTRING(text, 1, 10) AS sub FROM data',
      }))
      expect(result[0].sub).toBe('Hi')
    })

    it('should work with column names', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT name, SUBSTRING(email, 1, 5) AS email_prefix FROM users WHERE id = 1',
      }))
      expect(result[0].email_prefix).toBe('alice')
    })
  })

  describe('SUBSTR', () => {
    it('should work as an alias for SUBSTRING', async () => {
      const data = [
        { id: 1, text: 'Hello World' },
        { id: 2, text: 'JavaScript' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTR(text, 1, 5) AS sub FROM data',
      }))
      expect(result).toEqual([
        { sub: 'Hello' },
        { sub: 'JavaS' },
      ])
    })

    it('should extract substring from middle', async () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTR(text, 7, 5) AS sub FROM data',
      }))
      expect(result[0].sub).toBe('World')
    })

    it('should work without length parameter', async () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTR(text, 7) AS sub FROM data',
      }))
      expect(result[0].sub).toBe('World')
    })

    it('should work without alias', async () => {
      const data = [{ id: 1, text: 'Hello' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTR(text, 1, 3) FROM data',
      }))
      expect(result[0]).toHaveProperty('substr_text_1_3')
      expect(result[0].substr_text_1_3).toBe('Hel')
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, text: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTR(text, 1, 5) AS sub FROM data',
      }))
      expect(result[0].sub).toBeNull()
    })
  })

  describe('TRIM', () => {
    it('should remove leading and trailing whitespace', async () => {
      const data = [
        { id: 1, text: '  hello  ' },
        { id: 2, text: '\tworld\t' },
        { id: 3, text: '\n test \n' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT TRIM(text) AS trimmed FROM data',
      }))
      expect(result).toEqual([
        { trimmed: 'hello' },
        { trimmed: 'world' },
        { trimmed: 'test' },
      ])
    })

    it('should work without alias', async () => {
      const data = [{ id: 1, text: '  hello  ' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT TRIM(text) FROM data',
      }))
      expect(result[0]).toHaveProperty('trim_text')
      expect(result[0].trim_text).toBe('hello')
    })

    it('should not affect strings without whitespace', async () => {
      const data = [{ id: 1, text: 'hello' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT TRIM(text) AS trimmed FROM data',
      }))
      expect(result[0].trimmed).toBe('hello')
    })

    it('should preserve internal whitespace', async () => {
      const data = [{ id: 1, text: '  hello world  ' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT TRIM(text) AS trimmed FROM data',
      }))
      expect(result[0].trimmed).toBe('hello world')
    })

    it('should work with WHERE clause', async () => {
      const users = [
        { id: 1, name: '  Alice  ' },
        { id: 2, name: 'Bob' },
      ]
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT id, TRIM(name) AS trimmed FROM users WHERE TRIM(name) = \'Alice\'',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].id).toBe(1)
    })
  })

  describe('REPLACE', () => {
    it('should replace all occurrences of a substring', async () => {
      const data = [
        { id: 1, text: 'Hello World' },
        { id: 2, text: 'foo bar foo' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(text, \'o\', \'0\') AS replaced FROM data',
      }))
      expect(result).toEqual([
        { replaced: 'Hell0 W0rld' },
        { replaced: 'f00 bar f00' },
      ])
    })

    it('should replace multiple character substrings', async () => {
      const data = [{ id: 1, text: 'Hello World Hello' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(text, \'Hello\', \'Hi\') AS replaced FROM data',
      }))
      expect(result[0].replaced).toBe('Hi World Hi')
    })

    it('should work without alias', async () => {
      const data = [{ id: 1, text: 'test' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(text, \'t\', \'T\') FROM data',
      }))
      expect(result[0]).toHaveProperty('replace_text_t_T')
      expect(result[0].replace_text_t_T).toBe('TesT')
    })

    it('should handle empty replacement string', async () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(text, \' \', \'\') AS replaced FROM data',
      }))
      expect(result[0].replaced).toBe('HelloWorld')
    })

    it('should handle search string not found', async () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(text, \'xyz\', \'abc\') AS replaced FROM data',
      }))
      expect(result[0].replaced).toBe('Hello World')
    })

    it('should work with column values', async () => {
      const data = [
        { id: 1, email: 'alice@example.com', old_domain: 'example.com', new_domain: 'test.org' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(email, old_domain, new_domain) AS new_email FROM data',
      }))
      expect(result[0].new_email).toBe('alice@test.org')
    })

    it('should work with WHERE clause', async () => {
      const data = [
        { id: 1, text: 'apple banana' },
        { id: 2, text: 'grape orange' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(text, \'a\', \'@\') AS replaced FROM data WHERE text LIKE \'%apple%\'',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].replaced).toBe('@pple b@n@n@')
    })

    it('should work with ORDER BY', async () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Carol' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(name, \'o\', \'0\') AS replaced FROM data ORDER BY name',
      }))
      expect(result[0].replaced).toBe('Alice')
      expect(result[1].replaced).toBe('B0b')
      expect(result[2].replaced).toBe('Car0l')
    })

    it('should be case-sensitive', async () => {
      const data = [{ id: 1, text: 'Hello hello HELLO' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(text, \'hello\', \'hi\') AS replaced FROM data',
      }))
      expect(result[0].replaced).toBe('Hello hi HELLO')
    })

    it('should handle null values in first argument', async () => {
      const data = [{ id: 1, text: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(text, \'a\', \'b\') AS replaced FROM data',
      }))
      expect(result[0].replaced).toBeNull()
    })

    it('should handle null values in second argument', async () => {
      const data = [{ id: 1, text: 'hello', search: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(text, search, \'x\') AS replaced FROM data',
      }))
      expect(result[0].replaced).toBeNull()
    })

    it('should handle null values in third argument', async () => {
      const data = [{ id: 1, text: 'hello', replacement: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REPLACE(text, \'l\', replacement) AS replaced FROM data',
      }))
      expect(result[0].replaced).toBeNull()
    })
  })

  describe('LEFT', () => {
    it('should return first n characters', async () => {
      const data = [
        { id: 1, text: 'Hello World' },
        { id: 2, text: 'JavaScript' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LEFT(text, 5) AS left_text FROM data',
      }))
      expect(result).toEqual([
        { left_text: 'Hello' },
        { left_text: 'JavaS' },
      ])
    })

    it('should return entire string if n > length', async () => {
      const data = [{ id: 1, text: 'Hi' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LEFT(text, 10) AS left_text FROM data',
      }))
      expect(result[0].left_text).toBe('Hi')
    })

    it('should return empty string for n = 0', async () => {
      const data = [{ id: 1, text: 'Hello' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LEFT(text, 0) AS left_text FROM data',
      }))
      expect(result[0].left_text).toBe('')
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, text: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LEFT(text, 5) AS left_text FROM data',
      }))
      expect(result[0].left_text).toBeNull()
    })

    it('should throw for negative length', async () => {
      const data = [{ id: 1, text: 'Hello' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT LEFT(text, -1) FROM data',
      }))).rejects.toThrow('LEFT(string, length): length must be a non-negative integer')
    })
  })

  describe('RIGHT', () => {
    it('should return last n characters', async () => {
      const data = [
        { id: 1, text: 'Hello World' },
        { id: 2, text: 'JavaScript' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT RIGHT(text, 5) AS right_text FROM data',
      }))
      expect(result).toEqual([
        { right_text: 'World' },
        { right_text: 'cript' },
      ])
    })

    it('should return entire string if n > length', async () => {
      const data = [{ id: 1, text: 'Hi' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT RIGHT(text, 10) AS right_text FROM data',
      }))
      expect(result[0].right_text).toBe('Hi')
    })

    it('should return empty string for n = 0', async () => {
      const data = [{ id: 1, text: 'Hello' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT RIGHT(text, 0) AS right_text FROM data',
      }))
      expect(result[0].right_text).toBe('')
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, text: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT RIGHT(text, 5) AS right_text FROM data',
      }))
      expect(result[0].right_text).toBeNull()
    })

    it('should throw for negative length', async () => {
      const data = [{ id: 1, text: 'Hello' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT RIGHT(text, -1) FROM data',
      }))).rejects.toThrow('RIGHT(string, length): length must be a non-negative integer')
    })
  })

  describe('INSTR', () => {
    it('should return 1-based position of substring', async () => {
      const data = [
        { id: 1, text: 'Hello World' },
        { id: 2, text: 'Goodbye' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT INSTR(text, \'o\') AS pos FROM data',
      }))
      expect(result).toEqual([
        { pos: 5 }, // 'o' in 'Hello' at position 5
        { pos: 2 }, // 'o' in 'Goodbye' at position 2
      ])
    })

    it('should return 0 when substring not found', async () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT INSTR(text, \'xyz\') AS pos FROM data',
      }))
      expect(result[0].pos).toBe(0)
    })

    it('should find multi-character substrings', async () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT INSTR(text, \'World\') AS pos FROM data',
      }))
      expect(result[0].pos).toBe(7)
    })

    it('should be case-sensitive', async () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT INSTR(text, \'world\') AS pos FROM data',
      }))
      expect(result[0].pos).toBe(0)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, text: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT INSTR(text, \'a\') AS pos FROM data',
      }))
      expect(result[0].pos).toBeNull()
    })

    it('should handle null search string', async () => {
      const data = [{ id: 1, text: 'Hello', search: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT INSTR(text, search) AS pos FROM data',
      }))
      expect(result[0].pos).toBeNull()
    })

    it('should return 1 for empty search string', async () => {
      const data = [{ id: 1, text: 'Hello' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT INSTR(text, \'\') AS pos FROM data',
      }))
      expect(result[0].pos).toBe(1)
    })

    it('should work in WHERE clause', async () => {
      const data = [
        { id: 1, email: 'alice@example.com' },
        { id: 2, email: 'bob@test.org' },
        { id: 3, email: 'charlie@example.com' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT email FROM data WHERE INSTR(email, \'example\') > 0',
      }))
      expect(result).toHaveLength(2)
      expect(result[0].email).toBe('alice@example.com')
      expect(result[1].email).toBe('charlie@example.com')
    })
  })

  describe('combined string functions', () => {
    it('should use multiple different string functions in one query', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT UPPER(name) AS upper_name, LOWER(city) AS lower_city, LENGTH(email) AS email_len FROM users WHERE id = 1',
      }))
      expect(result[0]).toEqual({
        upper_name: 'ALICE',
        lower_city: 'nyc',
        email_len: 17,
      })
    })

    it('should work with regular columns', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT id, name, UPPER(city) AS upper_city FROM users ORDER BY id LIMIT 2',
      }))
      expect(result).toHaveLength(2)
      expect(result[0]).toEqual({ id: 1, name: 'Alice', upper_city: 'NYC' })
      expect(result[1]).toEqual({ id: 2, name: 'Bob', upper_city: 'LA' })
    })

    it('should work with DISTINCT', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT DISTINCT UPPER(city) AS upper_city FROM users',
      }))
      expect(result).toHaveLength(2)
      const cities = result.map(r => r.upper_city).sort()
      expect(cities).toEqual(['LA', 'NYC'])
    })
  })

  describe('string functions with GROUP BY', () => {
    it('should work with GROUP BY and aggregates', async () => {
      const result = await collect(executeSql({
        tables: { users },
        query: 'SELECT UPPER(city) AS upper_city, COUNT(*) AS count FROM users GROUP BY city',
      }))
      expect(result).toHaveLength(2)
      const nycGroup = result.find(r => r.upper_city === 'NYC')
      expect(nycGroup?.count).toBe(2)
    })

    it('should group by string function result', async () => {
      const data = [
        { id: 1, name: 'alice', value: 10 },
        { id: 2, name: 'ALICE', value: 20 },
        { id: 3, name: 'bob', value: 30 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT UPPER(name) AS upper_name, SUM(value) AS total FROM data GROUP BY UPPER(name)',
      }))
      expect(result).toHaveLength(2)
      const aliceGroup = result.find(r => r.upper_name === 'ALICE')
      expect(aliceGroup?.total).toBe(30)
    })
  })

  describe('error handling', () => {
    it('should throw for SUBSTRING with start position 0', async () => {
      const data = [{ id: 1, text: 'Hello' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTRING(text, 0, 3) FROM data' })))
        .rejects.toThrow('SUBSTRING(string, start[, length]): start position must be a positive integer')
    })

    it('should throw for SUBSTRING with negative length', async () => {
      const data = [{ id: 1, text: 'Hello' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTRING(text, 1, -1) FROM data' })))
        .rejects.toThrow('SUBSTRING(string, start[, length]): length must be a non-negative integer')
    })
  })

  describe('null handling', () => {
    it('should handle null values in UPPER', async () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: null },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT UPPER(name) AS upper_name FROM data',
      }))
      expect(result[1].upper_name).toBeNull()
    })

    it('should handle null values in LOWER', async () => {
      const data = [
        { id: 1, name: 'Alice' },
        { id: 2, name: null },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LOWER(name) AS lower_name FROM data',
      }))
      expect(result[1].lower_name).toBeNull()
    })

    it('should handle null values in CONCAT', async () => {
      const data = [{ id: 1, first: 'Alice', last: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT CONCAT(first, last) AS full FROM data',
      }))
      expect(result[0].full).toBeNull()
    })

    it('should handle null values in LENGTH', async () => {
      const data = [{ id: 1, text: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LENGTH(text) AS len FROM data',
      }))
      expect(result[0].len).toBeNull()
    })

    it('should handle null values in SUBSTRING', async () => {
      const data = [{ id: 1, text: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SUBSTRING(text, 1, 5) AS sub FROM data',
      }))
      expect(result[0].sub).toBeNull()
    })

    it('should handle null values in TRIM', async () => {
      const data = [{ id: 1, text: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT TRIM(text) AS trimmed FROM data',
      }))
      expect(result[0].trimmed).toBeNull()
    })
  })
})
