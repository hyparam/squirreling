import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('columns with dots in names', () => {
  const data = [
    { 'user.name': 'Alice', 'user.age': 30, id: 1 },
    { 'user.name': 'Bob', 'user.age': 25, id: 2 },
    { 'user.name': 'Charlie', 'user.age': 35, id: 3 },
  ]

  describe('SELECT *', () => {
    it('should select all columns including dotted names', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT * FROM data',
      }))
      expect(result).toEqual(data)
    })
  })

  describe('SELECT specific dotted columns', () => {
    it('should select a dotted column with double quotes', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT "user.name" FROM data',
      }))
      expect(result).toEqual([
        { 'user.name': 'Alice' },
        { 'user.name': 'Bob' },
        { 'user.name': 'Charlie' },
      ])
    })

    it('should select multiple dotted columns with double quotes', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT "user.name", "user.age" FROM data',
      }))
      expect(result).toEqual([
        { 'user.name': 'Alice', 'user.age': 30 },
        { 'user.name': 'Bob', 'user.age': 25 },
        { 'user.name': 'Charlie', 'user.age': 35 },
      ])
    })

    it('should mix dotted and regular columns', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT id, "user.name" FROM data',
      }))
      expect(result).toEqual([
        { id: 1, 'user.name': 'Alice' },
        { id: 2, 'user.name': 'Bob' },
        { id: 3, 'user.name': 'Charlie' },
      ])
    })

    it('should select a dotted column without quotes (ambiguous with table.column)', async () => {
      // Unquoted user.name is parsed as table "user", column "name"
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT user.name FROM data',
      }))).rejects.toThrow('Table "user" not found')
    })
  })

  describe('WHERE with dotted columns', () => {
    it('should filter on a dotted column with double quotes', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT * FROM data WHERE "user.name" = \'Alice\'',
      }))
      expect(result).toEqual([
        { 'user.name': 'Alice', 'user.age': 30, id: 1 },
      ])
    })

    it('should filter on a dotted column with comparison', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT * FROM data WHERE "user.age" > 28',
      }))
      expect(result).toEqual([
        { 'user.name': 'Alice', 'user.age': 30, id: 1 },
        { 'user.name': 'Charlie', 'user.age': 35, id: 3 },
      ])
    })
  })

  describe('ORDER BY with dotted columns', () => {
    it('should order by a dotted column with double quotes', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT "user.name", "user.age" FROM data ORDER BY "user.age"',
      }))
      expect(result).toEqual([
        { 'user.name': 'Bob', 'user.age': 25 },
        { 'user.name': 'Alice', 'user.age': 30 },
        { 'user.name': 'Charlie', 'user.age': 35 },
      ])
    })
  })

  describe('GROUP BY with dotted columns', () => {
    it('should group by a dotted column with double quotes', async () => {
      const rows = [
        { 'dept.name': 'eng', salary: 100 },
        { 'dept.name': 'eng', salary: 200 },
        { 'dept.name': 'sales', salary: 150 },
      ]
      const result = await collect(executeSql({
        tables: { rows },
        query: 'SELECT "dept.name", SUM(salary) AS total FROM rows GROUP BY "dept.name" ORDER BY "dept.name"',
      }))
      expect(result).toEqual([
        { 'dept.name': 'eng', total: 300 },
        { 'dept.name': 'sales', total: 150 },
      ])
    })
  })

  describe('aliases on dotted columns', () => {
    it('should alias a dotted column', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT "user.name" AS name FROM data',
      }))
      expect(result).toEqual([
        { name: 'Alice' },
        { name: 'Bob' },
        { name: 'Charlie' },
      ])
    })
  })

  describe('qualified table.* with dotted column names', () => {
    it('should expand table.* including dotted columns', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT data.* FROM data',
      }))
      expect(result).toEqual(data)
    })
  })

  describe('table-qualified column references', () => {
    it('should resolve table.column for regular columns', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT data.id FROM data',
      }))
      expect(result).toEqual([
        { id: 1 },
        { id: 2 },
        { id: 3 },
      ])
    })

    it('should resolve table."dotted.column" with quoted identifier', async () => {
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT data."user.name" FROM data',
      }))
      expect(result).toEqual([
        { 'user.name': 'Alice' },
        { 'user.name': 'Bob' },
        { 'user.name': 'Charlie' },
      ])
    })

    it('should not substitute SELECT aliases into qualified identifiers', async () => {
      const rows = [
        { a: 1, x: 2 },
        { a: 2, x: 1 },
      ]
      const result = await collect(executeSql({
        tables: { t: rows },
        query: 'SELECT a AS x, t.x AS real_x FROM t ORDER BY t.x',
      }))
      expect(result).toEqual([
        { x: 2, real_x: 1 },
        { x: 1, real_x: 2 },
      ])
    })
  })

  describe('table names with dots', () => {
    const rows = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]

    it('should select all from a dotted table name', async () => {
      const result = await collect(executeSql({
        tables: { 'dataset.parquet': rows },
        query: 'SELECT * FROM "dataset.parquet"',
      }))
      expect(result).toEqual(rows)
    })

    it('should select specific columns from a dotted table name', async () => {
      const result = await collect(executeSql({
        tables: { 'dataset.parquet': rows },
        query: 'SELECT name FROM "dataset.parquet"',
      }))
      expect(result).toEqual([
        { name: 'Alice' },
        { name: 'Bob' },
      ])
    })

    it('should select qualified columns from a dotted table name', async () => {
      const result = await collect(executeSql({
        tables: { 'dataset.parquet': rows },
        query: 'SELECT "dataset.parquet".name FROM "dataset.parquet"',
      }))
      expect(result).toEqual([
        { name: 'Alice' },
        { name: 'Bob' },
      ])
    })
  })
})
