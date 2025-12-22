import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/** @type {null} */
const NULL = null

describe('regex functions', () => {
  describe('REGEXP_SUBSTR', () => {
    it('should extract first match of regex pattern', async () => {
      const data = [
        { id: 1, text: 'Hello World 123' },
        { id: 2, text: 'abc 456 def 789' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[0-9]+\') AS num FROM data',
      }))
      expect(result).toEqual([
        { num: '123' },
        { num: '456' },
      ])
    })

    it('should return null when no match found', async () => {
      const data = [{ id: 1, text: 'Hello World' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[0-9]+\') AS num FROM data',
      }))
      expect(result[0].num).toBeNull()
    })

    it('should extract word patterns', async () => {
      const data = [{ id: 1, text: 'foo bar baz' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[a-z]+\') AS word FROM data',
      }))
      expect(result[0].word).toBe('foo')
    })

    it('should support position parameter (3rd arg)', async () => {
      const data = [{ id: 1, text: 'abc 123 def 456' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[0-9]+\', 6) AS num FROM data',
      }))
      expect(result[0].num).toBe('23')
    })

    it('should support occurrence parameter (4th arg)', async () => {
      const data = [{ id: 1, text: 'abc 123 def 456 ghi 789' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[0-9]+\', 1, 2) AS num FROM data',
      }))
      expect(result[0].num).toBe('456')
    })

    it('should return null when occurrence not found', async () => {
      const data = [{ id: 1, text: 'abc 123 def' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[0-9]+\', 1, 5) AS num FROM data',
      }))
      expect(result[0].num).toBeNull()
    })

    it('should handle null string value', async () => {
      const data = [{ id: 1, text: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[a-z]+\') AS word FROM data',
      }))
      expect(result[0].word).toBeNull()
    })

    it('should handle null pattern value', async () => {
      const data = [{ id: 1, text: 'hello', pattern: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, pattern) AS word FROM data',
      }))
      expect(result[0].word).toBeNull()
    })

    it('should throw for invalid position', async () => {
      const data = [{ id: 1, text: 'hello' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[a-z]+\', 0) FROM data',
      }))).rejects.toThrow('position must be a positive integer')
    })

    it('should throw for invalid occurrence', async () => {
      const data = [{ id: 1, text: 'hello' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[a-z]+\', 1, 0) FROM data',
      }))).rejects.toThrow('occurrence must be a positive integer')
    })

    it('should throw for invalid regex pattern', async () => {
      const data = [{ id: 1, text: 'hello' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[\') FROM data',
      }))).rejects.toThrow('invalid regex pattern')
    })

    it('should work with complex regex patterns', async () => {
      const data = [
        { id: 1, email: 'alice@example.com' },
        { id: 2, email: 'bob.smith@test.org' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(email, \'@[a-z.]+\') AS domain FROM data',
      }))
      expect(result).toEqual([
        { domain: '@example.com' },
        { domain: '@test.org' },
      ])
    })

    it('should work in WHERE clause', async () => {
      const data = [
        { id: 1, code: 'ABC-123' },
        { id: 2, code: 'XYZ-456' },
        { id: 3, code: 'ABC-789' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT code FROM data WHERE REGEXP_SUBSTR(code, \'^ABC\') IS NOT NULL',
      }))
      expect(result).toHaveLength(2)
      expect(result[0].code).toBe('ABC-123')
      expect(result[1].code).toBe('ABC-789')
    })

    it('should work without alias', async () => {
      const data = [{ id: 1, text: 'test123' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_SUBSTR(text, \'[0-9]+\') FROM data',
      }))
      expect(result[0]).toHaveProperty('regexp_substr_text_[0-9]+')
      expect(result[0]['regexp_substr_text_[0-9]+']).toBe('123')
    })
  })

  describe('REGEXP_REPLACE', () => {
    it('should replace all matches by default', async () => {
      const data = [{ id: 1, text: 'abc 123 def 456' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'[0-9]+\', \'X\') AS replaced FROM data',
      }))
      expect(result[0].replaced).toBe('abc X def X')
    })

    it('should replace pattern with empty string', async () => {
      const data = [{ id: 1, text: 'hello123world456' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'[0-9]+\', \'\') AS replaced FROM data',
      }))
      expect(result[0].replaced).toBe('helloworld')
    })

    it('should support position parameter (4th arg)', async () => {
      const data = [{ id: 1, text: 'abc 123 def 456' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'[0-9]+\', \'X\', 6) AS replaced FROM data',
      }))
      // Position 6 starts at '23 def 456', so '123' is partially replaced
      expect(result[0].replaced).toBe('abc 1X def X')
    })

    it('should support occurrence parameter (5th arg)', async () => {
      const data = [{ id: 1, text: 'abc 123 def 456 ghi 789' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'[0-9]+\', \'X\', 1, 2) AS replaced FROM data',
      }))
      // Replace only the 2nd occurrence
      expect(result[0].replaced).toBe('abc 123 def X ghi 789')
    })

    it('should replace only first occurrence when occurrence is 1', async () => {
      const data = [{ id: 1, text: 'aaa bbb aaa' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'aaa\', \'X\', 1, 1) AS replaced FROM data',
      }))
      expect(result[0].replaced).toBe('X bbb aaa')
    })

    it('should return original string when occurrence not found', async () => {
      const data = [{ id: 1, text: 'abc 123 def' }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'[0-9]+\', \'X\', 1, 5) AS replaced FROM data',
      }))
      expect(result[0].replaced).toBe('abc 123 def')
    })

    it('should handle null string value', async () => {
      const data = [{ id: 1, text: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'[a-z]+\', \'X\') AS replaced FROM data',
      }))
      expect(result[0].replaced).toBeNull()
    })

    it('should handle null pattern value', async () => {
      const data = [{ id: 1, text: 'hello', pattern: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, pattern, \'X\') AS replaced FROM data',
      }))
      expect(result[0].replaced).toBeNull()
    })

    it('should handle null replacement value', async () => {
      const data = [{ id: 1, text: 'hello', repl: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'[a-z]+\', repl) AS replaced FROM data',
      }))
      expect(result[0].replaced).toBeNull()
    })

    it('should throw for invalid position', async () => {
      const data = [{ id: 1, text: 'hello' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'[a-z]+\', \'X\', 0) FROM data',
      }))).rejects.toThrow('position must be a positive integer')
    })

    it('should throw for invalid occurrence', async () => {
      const data = [{ id: 1, text: 'hello' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'[a-z]+\', \'X\', 1, -1) FROM data',
      }))).rejects.toThrow('occurrence must be a non-negative integer')
    })

    it('should throw for invalid regex pattern', async () => {
      const data = [{ id: 1, text: 'hello' }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(text, \'[\', \'X\') FROM data',
      }))).rejects.toThrow('invalid regex pattern')
    })

    it('should work with complex regex patterns', async () => {
      const data = [
        { id: 1, email: 'alice@example.com' },
        { id: 2, email: 'bob@test.org' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT REGEXP_REPLACE(email, \'@[a-z.]+\', \'@redacted\') AS masked FROM data',
      }))
      expect(result).toEqual([
        { masked: 'alice@redacted' },
        { masked: 'bob@redacted' },
      ])
    })

    it('should work in WHERE clause', async () => {
      const data = [
        { id: 1, text: 'foo123bar' },
        { id: 2, text: 'foobar' },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT id FROM data WHERE REGEXP_REPLACE(text, \'[0-9]+\', \'\') = \'foobar\'',
      }))
      expect(result).toHaveLength(2)
    })
  })
})
