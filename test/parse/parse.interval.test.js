import { describe, expect, it } from 'vitest'
import { parseSql } from '../../src/parse/parse.js'

describe('INTERVAL parsing', () => {
  describe('MySQL-style syntax (unquoted)', () => {
    it('should parse INTERVAL with DAY', () => {
      const select = parseSql('SELECT INTERVAL 1 DAY FROM dummy')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'interval',
            value: 1,
            unit: 'DAY',
            positionStart: 7,
            positionEnd: 21,
          },
          alias: undefined,
        },
      ])
    })

    it('should parse INTERVAL with MONTH', () => {
      const select = parseSql('SELECT INTERVAL 2 MONTH FROM dummy')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'interval',
            value: 2,
            unit: 'MONTH',
            positionStart: 7,
            positionEnd: 23,
          },
          alias: undefined,
        },
      ])
    })

    it('should parse INTERVAL with YEAR', () => {
      const select = parseSql('SELECT INTERVAL 5 YEAR FROM dummy')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'interval',
            value: 5,
            unit: 'YEAR',
            positionStart: 7,
            positionEnd: 22,
          },
          alias: undefined,
        },
      ])
    })

    it('should parse INTERVAL with HOUR', () => {
      const select = parseSql('SELECT INTERVAL 12 HOUR FROM dummy')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'interval',
            value: 12,
            unit: 'HOUR',
            positionStart: 7,
            positionEnd: 23,
          },
          alias: undefined,
        },
      ])
    })

    it('should parse INTERVAL with MINUTE', () => {
      const select = parseSql('SELECT INTERVAL 30 MINUTE FROM dummy')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'interval',
            value: 30,
            unit: 'MINUTE',
            positionStart: 7,
            positionEnd: 25,
          },
          alias: undefined,
        },
      ])
    })

    it('should parse INTERVAL with SECOND', () => {
      const select = parseSql('SELECT INTERVAL 45 SECOND FROM dummy')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'interval',
            value: 45,
            unit: 'SECOND',
            positionStart: 7,
            positionEnd: 25,
          },
          alias: undefined,
        },
      ])
    })
  })

  describe('Standard SQL syntax (quoted)', () => {
    it('should parse quoted INTERVAL value', () => {
      const select = parseSql('SELECT INTERVAL \'1\' DAY FROM dummy')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'interval',
            value: 1,
            unit: 'DAY',
            positionStart: 7,
            positionEnd: 23,
          },
          alias: undefined,
        },
      ])
    })

    it('should parse quoted decimal INTERVAL', () => {
      const select = parseSql('SELECT INTERVAL \'2.5\' HOUR FROM dummy')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'interval',
            value: 2.5,
            unit: 'HOUR',
            positionStart: 7,
            positionEnd: 26,
          },
          alias: undefined,
        },
      ])
    })
  })

  describe('date arithmetic expressions', () => {
    it('should parse date + INTERVAL', () => {
      const select = parseSql('SELECT order_date + INTERVAL 7 DAY FROM orders')
      expect(select.columns).toEqual([{
        kind: 'derived',
        expr: {
          type: 'binary',
          op: '+',
          left: {
            type: 'identifier',
            name: 'order_date',
            positionStart: 7,
            positionEnd: 17,
          },
          right: {
            type: 'interval',
            value: 7,
            unit: 'DAY',
            positionStart: 20,
            positionEnd: 34,
          },
          positionStart: 7,
          positionEnd: 34,
        },
        alias: undefined,
      }])
    })

    it('should parse date - INTERVAL', () => {
      const select = parseSql('SELECT created_at - INTERVAL 30 MINUTE FROM events')
      expect(select.columns).toEqual([{
        kind: 'derived',
        expr: {
          type: 'binary',
          op: '-',
          left: {
            type: 'identifier',
            name: 'created_at',
            positionStart: 7,
            positionEnd: 17,
          },
          right: {
            type: 'interval',
            value: 30,
            unit: 'MINUTE',
            positionStart: 20,
            positionEnd: 38,
          },
          positionStart: 7,
          positionEnd: 38,
        },
        alias: undefined,
      }])
    })

    it('should parse CURRENT_DATE + INTERVAL', () => {
      const select = parseSql('SELECT CURRENT_DATE + INTERVAL 1 MONTH FROM dummy')
      expect(select.columns).toEqual([{
        kind: 'derived',
        expr: {
          type: 'binary',
          op: '+',
          left: {
            type: 'function',
            name: 'CURRENT_DATE',
            args: [],
            positionStart: 7,
            positionEnd: 19,
          },
          right: {
            type: 'interval',
            value: 1,
            unit: 'MONTH',
            positionStart: 22,
            positionEnd: 38,
          },
          positionStart: 7,
          positionEnd: 38,
        },
        alias: undefined,
      }])
    })
  })

  describe('negative intervals', () => {
    it('should parse negative INTERVAL value', () => {
      const select = parseSql('SELECT INTERVAL -1 DAY FROM dummy')
      expect(select.columns).toEqual([
        {
          kind: 'derived',
          expr: {
            type: 'interval',
            value: -1,
            unit: 'DAY',
            positionStart: 7,
            positionEnd: 22,
          },
          alias: undefined,
        },
      ])
    })

    it('should parse date subtraction with INTERVAL', () => {
      const select = parseSql('SELECT date_col - INTERVAL 1 DAY FROM t')
      expect(select.columns).toEqual([{
        kind: 'derived',
        expr: {
          type: 'binary',
          op: '-',
          left: {
            type: 'identifier',
            name: 'date_col',
            positionStart: 7,
            positionEnd: 15,
          },
          right: {
            type: 'interval',
            value: 1,
            unit: 'DAY',
            positionStart: 18,
            positionEnd: 32,
          },
          positionStart: 7,
          positionEnd: 32,
        },
        alias: undefined,
      }])
    })
  })
})
