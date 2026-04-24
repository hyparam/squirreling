import { describe, expect, it } from 'vitest'
import { parseSelect } from '../helpers.js'

describe('parseSql window functions', () => {
  it('should parse ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)', () => {
    const select = parseSelect('SELECT ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM sales')
    const col = select.columns[0]
    if (col.type !== 'derived' || col.expr.type !== 'window') throw new Error('expected window node')
    expect(col.alias).toBe('rn')
    expect(col.expr.funcName).toBe('ROW_NUMBER')
    expect(col.expr.args).toEqual([])
    expect(col.expr.partitionBy.length).toBe(1)
    expect(col.expr.partitionBy[0].type).toBe('identifier')
    expect(col.expr.orderBy.length).toBe(1)
    expect(col.expr.orderBy[0].direction).toBe('DESC')
  })

  it('should parse ROW_NUMBER() OVER (ORDER BY x)', () => {
    const select = parseSelect('SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t')
    const col = select.columns[0]
    if (col.type !== 'derived' || col.expr.type !== 'window') throw new Error('expected window node')
    expect(col.expr.funcName).toBe('ROW_NUMBER')
    expect(col.expr.partitionBy).toEqual([])
    expect(col.expr.orderBy.length).toBe(1)
  })

  it('should parse ROW_NUMBER() OVER () with empty window', () => {
    const select = parseSelect('SELECT ROW_NUMBER() OVER () FROM t')
    const col = select.columns[0]
    if (col.type !== 'derived' || col.expr.type !== 'window') throw new Error('expected window node')
    expect(col.expr.partitionBy).toEqual([])
    expect(col.expr.orderBy).toEqual([])
  })

  it('should parse multiple PARTITION BY keys', () => {
    const select = parseSelect('SELECT ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY c) FROM t')
    const col = select.columns[0]
    if (col.type !== 'derived' || col.expr.type !== 'window') throw new Error('expected window node')
    expect(col.expr.partitionBy.length).toBe(2)
    expect(col.expr.orderBy.length).toBe(1)
  })
})
