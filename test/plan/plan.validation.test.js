import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { planSql } from '../../src/plan/plan.js'

describe('planSql table validation', () => {
  const tables = {
    users: memorySource({ data: [{ name: 'Alice', age: 30 }] }),
    orders: memorySource({ data: [{ id: 1, user_id: 1, total: 100 }] }),
  }

  it('should accept valid table name', () => {
    const plan = planSql({ query: 'SELECT * FROM users', tables })
    expect(plan.type).toBe('Scan')
  })

  it('should throw for unknown table name', () => {
    expect(() => planSql({ query: 'SELECT * FROM missing', tables }))
      .toThrow('Table "missing" not found. Available tables: users, orders')
  })

  it('should accept valid column names', () => {
    const plan = planSql({ query: 'SELECT name, age FROM users', tables })
    expect(plan.type).toBe('Project')
  })

  it('should throw for unknown column name', () => {
    expect(() => planSql({ query: 'SELECT email FROM users', tables }))
      .toThrow('Column "email" not found. Available columns: name, age')
  })

  it('should validate join table names', () => {
    expect(() => planSql({ query: 'SELECT * FROM users JOIN missing ON users.id = missing.id', tables }))
      .toThrow('Table "missing" not found. Available tables: users, orders')
  })

  it('should validate join column names', () => {
    expect(() => planSql({ query: 'SELECT users.name, orders.bad FROM users JOIN orders ON users.name = orders.user_id', tables }))
      .toThrow('Column "bad" not found. Available columns: id, user_id, total')
  })

  it('should not validate CTE names against tables', () => {
    const plan = planSql({ query: 'WITH active AS (SELECT name FROM users) SELECT * FROM active', tables })
    expect(plan).toBeDefined()
  })

  it('should validate subquery table names', () => {
    expect(() => planSql({ query: 'SELECT * FROM (SELECT * FROM missing) AS sub', tables }))
      .toThrow('Table "missing" not found. Available tables: users, orders')
  })

  it('should throw for column not in subquery', () => {
    expect(() => planSql({ query: 'SELECT name FROM (SELECT name AS full_name FROM users)', tables }))
      .toThrow('Column "name" not found. Available columns: full_name')
  })

  it('should skip validation when tables not provided', () => {
    const plan = planSql({ query: 'SELECT * FROM anything' })
    expect(plan.type).toBe('Scan')
  })

  it('should reject self references in lateral UNNEST arguments', () => {
    expect(() => planSql({ query: 'SELECT * FROM users JOIN UNNEST(u.arr) AS u(x) ON TRUE', tables }))
      .toThrow('Table "u" not found in "u.arr". Available tables: users')
  })

  it('should reject forward references in lateral UNNEST arguments', () => {
    expect(() => planSql({ query: 'SELECT * FROM users JOIN UNNEST(orders.total) AS u(x) ON TRUE JOIN orders ON TRUE', tables }))
      .toThrow('Table "orders" not found in "orders.total". Available tables: users')
  })
})
