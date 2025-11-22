import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('parseSql error handling', () => {
  describe('basic syntax errors', () => {
    it('should throw error on missing SELECT keyword', () => {
      expect(() => parseSql('FROM users')).toThrow('Expected SELECT at position 0')
    })

    it('should throw error on missing FROM keyword', () => {
      expect(() => parseSql('SELECT name')).toThrow('Expected FROM after "name" at position 11')
    })

    it('should throw error on missing FROM keyword after wildcard', () => {
      expect(() => parseSql('SELECT *')).toThrow('Expected FROM after "*" at position 8')
    })

    it('should throw error on missing table name after FROM', () => {
      expect(() => parseSql('SELECT * FROM')).toThrow('Expected identifier after "FROM" at position 13')
    })

    it('should throw error on missing table name after lowercase FROM', () => {
      expect(() => parseSql('SELECT * from')).toThrow('Expected identifier after "from" at position 13')
    })

    it('should throw error on invalid table name after FROM', () => {
      expect(() => parseSql('SELECT * FROM 3')).toThrow('Expected identifier after "FROM" at position 14')
    })

    it('should throw error on empty query', () => {
      expect(() => parseSql('')).toThrow('Expected SELECT at position 0')
    })

    it('should throw error on unexpected tokens after query', () => {
      expect(() => parseSql('SELECT * FROM users; SELECT')).toThrow('Expected end of query after ";" at position 21')
    })
  })

  describe('SELECT list errors', () => {
    it('should throw error on invalid column name', () => {
      expect(() => parseSql('SELECT FROM users')).toThrow('Expected column name or function after "SELECT" at position 7')
    })

    it('should throw error on missing column after comma', () => {
      expect(() => parseSql('SELECT name, FROM users')).toThrow('Expected column name or function after "," at position 13')
    })

    it('should throw error on missing alias after AS', () => {
      expect(() => parseSql('SELECT name AS FROM users')).toThrow('Expected alias after "AS" at position 15')
    })

    it('should throw error on missing alias after lowercase as', () => {
      expect(() => parseSql('SELECT name as FROM users')).toThrow('Expected alias after "as" at position 15')
    })
  })

  describe('aggregate function errors', () => {
    it('should throw error on missing opening paren in aggregate', () => {
      expect(() => parseSql('SELECT COUNT name myalias FROM users')).toThrow('Expected FROM after "name" at position 18')
    })

    it('should throw error on missing closing paren in aggregate', () => {
      expect(() => parseSql('SELECT COUNT(name FROM users')).toThrow('Expected ) after "name" at position 18')
    })

    it('should throw error on empty aggregate function', () => {
      expect(() => parseSql('SELECT COUNT() FROM users')).toThrow('Expected identifier after "(" at position 13')
    })

    it('should throw error on missing aggregate argument', () => {
      expect(() => parseSql('SELECT SUM() FROM users')).toThrow('Expected identifier after "(" at position 11')
    })

    it('should throw error when expecting closing paren in aggregate', () => {
      expect(() => parseSql('SELECT COUNT(name( FROM users')).toThrow('Expected ) after "name" at position 17')
    })
  })

  describe('string function errors', () => {
    it('should throw error on missing opening paren in string function', () => {
      expect(() => parseSql('SELECT UPPER name myalias FROM users')).toThrow('Expected FROM after "name" at position 18')
    })

    it('should throw error on missing closing paren in string function', () => {
      expect(() => parseSql('SELECT UPPER(name FROM users')).toThrow('Expected ) after "name" at position 18')
    })
  })

  describe('WHERE clause errors', () => {
    it('should throw error on incomplete WHERE clause', () => {
      expect(() => parseSql('SELECT * FROM users WHERE')).toThrow('Unexpected token in expression at position 25: eof')
    })

    it('should throw error on missing closing paren in WHERE', () => {
      expect(() => parseSql('SELECT * FROM users WHERE (age > 18')).toThrow('Expected ) after "18" at position 35')
    })

    it('should throw error on incomplete comparison in WHERE', () => {
      expect(() => parseSql('SELECT * FROM users WHERE age >')).toThrow('Unexpected token in expression at position 31: eof')
    })

    it('should throw error when expecting closing paren in WHERE', () => {
      expect(() => parseSql('SELECT * FROM users WHERE (age > 18(')).toThrow('Expected ) after "18" at position 35')
    })
  })

  describe('JOIN errors', () => {
    it('should throw error on missing table name after JOIN', () => {
      expect(() => parseSql('SELECT * FROM users JOIN')).toThrow('Expected identifier after "JOIN" at position 24')
    })

    it('should throw error on missing ON keyword in JOIN', () => {
      expect(() => parseSql('SELECT * FROM users JOIN orders')).toThrow('Expected ON after "orders" at position 31')
    })

    it('should throw error on missing JOIN condition', () => {
      expect(() => parseSql('SELECT * FROM users JOIN orders ON')).toThrow('Unexpected token in expression at position 34: eof')
    })

    it('should throw error on missing JOIN keyword after LEFT', () => {
      expect(() => parseSql('SELECT * FROM users LEFT orders')).toThrow('Expected JOIN after "LEFT" at position 25')
    })

    it('should throw error on missing JOIN keyword after INNER', () => {
      expect(() => parseSql('SELECT * FROM users INNER orders')).toThrow('Expected JOIN after "INNER" at position 26')
    })
  })

  describe('GROUP BY errors', () => {
    it('should throw error on missing BY after GROUP', () => {
      expect(() => parseSql('SELECT COUNT(*) FROM users GROUP')).toThrow('Expected BY after "GROUP" at position 32')
    })

    it('should throw error on missing column after GROUP BY', () => {
      expect(() => parseSql('SELECT COUNT(*) FROM users GROUP BY')).toThrow('Unexpected token in expression at position 35: eof')
    })

    it('should throw error on missing column after comma in GROUP BY', () => {
      expect(() => parseSql('SELECT COUNT(*) FROM users GROUP BY age,')).toThrow('Unexpected token in expression at position 40: eof')
    })
  })

  describe('ORDER BY errors', () => {
    it('should throw error on missing BY after ORDER', () => {
      expect(() => parseSql('SELECT * FROM users ORDER')).toThrow('Expected BY after "ORDER" at position 25')
    })

    it('should throw error on missing column after ORDER BY', () => {
      expect(() => parseSql('SELECT * FROM users ORDER BY')).toThrow('Unexpected token in expression at position 28: eof')
    })

    it('should throw error on missing column after comma in ORDER BY', () => {
      expect(() => parseSql('SELECT * FROM users ORDER BY age,')).toThrow('Unexpected token in expression at position 33: eof')
    })
  })

  describe('LIMIT/OFFSET errors', () => {
    it('should throw error on invalid LIMIT', () => {
      expect(() => parseSql('SELECT * FROM users LIMIT abc')).toThrow('Expected numeric LIMIT')
    })

    it('should throw error on missing LIMIT value', () => {
      expect(() => parseSql('SELECT * FROM users LIMIT')).toThrow('Expected numeric LIMIT')
    })

    it('should throw error on invalid OFFSET', () => {
      expect(() => parseSql('SELECT * FROM users OFFSET xyz')).toThrow('Expected numeric OFFSET')
    })

    it('should throw error on missing OFFSET value', () => {
      expect(() => parseSql('SELECT * FROM users OFFSET')).toThrow('Expected numeric OFFSET')
    })

    it('should throw error on invalid OFFSET after LIMIT', () => {
      expect(() => parseSql('SELECT * FROM users LIMIT 10 OFFSET abc')).toThrow('Expected numeric OFFSET')
    })
  })
})
