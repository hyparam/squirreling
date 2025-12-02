import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

describe('JOIN queries', () => {
  const users = [
    { id: 1, name: 'Alice', age: 30, city: 'NYC', active: true },
    { id: 2, name: 'Bob', age: 25, city: 'LA', active: true },
    { id: 3, name: 'Charlie', age: 35, city: 'NYC', active: false },
    { id: 4, name: 'Diana', age: 28, city: 'LA', active: true },
    { id: 5, name: 'Eve', age: 30, city: 'NYC', active: true },
  ]

  const orders = [
    { id: 1, user_id: 1, product: 'Laptop', amount: 1000 },
    { id: 2, user_id: 1, product: 'Mouse', amount: 50 },
    { id: 3, user_id: 2, product: 'Keyboard', amount: 100 },
    { id: 4, user_id: 4, product: 'Monitor', amount: 500 },
  ]

  it('should perform INNER JOIN', async () => {
    const result = await collect(executeSql({
      tables: { users, orders },
      query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
    }))
    expect(result).toHaveLength(4)
    expect(result.map(r => r.name)).toContain('Alice')
    expect(result.map(r => r.name)).toContain('Bob')
    expect(result.map(r => r.name)).toContain('Diana')
  })

  it('should perform LEFT JOIN', async () => {
    const result = await collect(executeSql({
      tables: { users, orders },
      query: 'SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id',
    }))
    // 4 user orders + 2 users without orders = 6
    expect(result.length).toBe(6)
    // Charlie and Eve have no orders, so their product should be null
    const charlie = result.find(r => r.name === 'Charlie')
    expect(charlie).toBeTruthy()
    expect(charlie.product).toBeNull()
  })

  it('should perform RIGHT JOIN', async () => {
    const result = await collect(executeSql({
      tables: { users, orders },
      query: 'SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id',
    }))
    expect(result).toHaveLength(4) // all 4 orders
    expect(result.map(r => r.product).sort()).toEqual(['Keyboard', 'Laptop', 'Monitor', 'Mouse'])
  })

  it('should perform FULL JOIN', async () => {
    const result = await collect(executeSql({
      tables: { users, orders },
      query: 'SELECT users.name, orders.product FROM users FULL JOIN orders ON users.id = orders.user_id',
    }))
    // 4 user orders + 2 users without orders = 6
    expect(result.length).toBe(6)
  })

  it('should handle JOIN with WHERE clause', async () => {
    const result = await collect(executeSql({
      tables: { users, orders },
      query: 'SELECT users.name, orders.product, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100',
    }))
    expect(result).toHaveLength(2)
    expect(result.map(r => r.product).sort()).toEqual(['Laptop', 'Monitor'])
  })

  it('should handle multiple JOINs', async () => {
    const products = [
      { name: 'Laptop', category: 'Electronics' },
      { name: 'Mouse', category: 'Electronics' },
      { name: 'Keyboard', category: 'Electronics' },
      { name: 'Monitor', category: 'Electronics' },
    ]
    const result = await collect(executeSql({
      tables: { users, orders, products },
      query: `
        SELECT users.name, orders.product, products.category
        FROM users
        JOIN orders ON users.id = orders.user_id
        JOIN products ON orders.product = products.name
      `,
    }))
    expect(result).toHaveLength(4)
    expect(result.every(r => r.category === 'Electronics')).toBe(true)
  })

  it('should handle JOIN with aggregates', async () => {
    const result = await collect(executeSql({
      tables: { users, orders },
      query: `
        SELECT users.name, SUM(orders.amount) AS total_spent
        FROM users
        JOIN orders ON users.id = orders.user_id
        GROUP BY users.name
        ORDER BY total_spent DESC
      `,
    }))
    expect(result).toHaveLength(3)
    expect(result[0].name).toBe('Alice')
    expect(result[0].total_spent).toBe(1050)
  })

  it('should handle unqualified column names in SELECT after JOIN', async () => {
    const result = await collect(executeSql({
      tables: { users, orders },
      query: 'SELECT name, product FROM users JOIN orders ON users.id = orders.user_id LIMIT 1',
    }))
    expect(result).toHaveLength(1)
    expect(result[0]).toHaveProperty('name')
    expect(result[0]).toHaveProperty('product')
  })

  it('should error when joining non-existent table', async () => {
    await expect(async () => {
      await collect(executeSql({
        tables: { users },
        query: 'SELECT * FROM users JOIN nonexistent ON users.id = nonexistent.user_id',
      }))
    }).rejects.toThrow('Table "nonexistent" not found')
  })

  // Edge case tests

  describe('empty table edge cases', () => {
    it('should return empty result when INNER JOIN with empty left table', async () => {
      const result = await collect(executeSql({
        tables: { users: [], orders },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(0)
    })

    it('should return empty result when INNER JOIN with empty right table', async () => {
      const result = await collect(executeSql({
        tables: { users, orders: [] },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(0)
    })

    it('should return empty result when INNER JOIN with both tables empty', async () => {
      const result = await collect(executeSql({
        tables: { users: [], orders: [] },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(0)
    })

    it('should return 0 rows for LEFT JOIN when left table is empty', async () => {
      const result = await collect(executeSql({
        tables: { users: [], orders },
        query: 'SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(0)
    })

    it('should return all left rows with nulls for LEFT JOIN with empty right table', async () => {
      const result = await collect(executeSql({
        tables: { users, orders: [] },
        query: 'SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(5)
      expect(result.every(r => r.product == null)).toBe(true)
    })

    it('should return all right rows with nulls for RIGHT JOIN with empty left table', async () => {
      const result = await collect(executeSql({
        tables: { users: [], orders },
        query: 'SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(4)
      expect(result.every(r => r.name == null)).toBe(true)
    })

    it('should return 0 rows for RIGHT JOIN when right table is empty', async () => {
      const result = await collect(executeSql({
        tables: { users, orders: [] },
        query: 'SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(0)
    })

    it('should return all rows from both tables for FULL JOIN with empty left table', async () => {
      const result = await collect(executeSql({
        tables: { users: [], orders },
        query: 'SELECT users.name, orders.product FROM users FULL JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(4)
      expect(result.every(r => r.name == null)).toBe(true)
    })

    it('should return all rows from both tables for FULL JOIN with empty right table', async () => {
      const result = await collect(executeSql({
        tables: { users, orders: [] },
        query: 'SELECT users.name, orders.product FROM users FULL JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(5)
      expect(result.every(r => r.product == null)).toBe(true)
    })
  })

  describe('no matching rows edge cases', () => {
    const ordersNoMatch = [
      { id: 1, user_id: 100, product: 'Laptop', amount: 1000 },
      { id: 2, user_id: 101, product: 'Mouse', amount: 50 },
    ]

    it('should return empty result for INNER JOIN with no matches', async () => {
      const result = await collect(executeSql({
        tables: { users, orders: ordersNoMatch },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(0)
    })

    it('should return left rows with nulls for LEFT JOIN with no matches', async () => {
      const result = await collect(executeSql({
        tables: { users, orders: ordersNoMatch },
        query: 'SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(5)
      expect(result.every(r => r.product === null)).toBe(true)
    })

    it('should return right rows with nulls for RIGHT JOIN with no matches', async () => {
      const result = await collect(executeSql({
        tables: { users, orders: ordersNoMatch },
        query: 'SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(2)
      expect(result.every(r => r.name === null)).toBe(true)
    })

    it('should return all rows from both sides for FULL JOIN with no matches', async () => {
      const result = await collect(executeSql({
        tables: { users, orders: ordersNoMatch },
        query: 'SELECT users.name, orders.product FROM users FULL JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(7) // 5 users + 2 orders
      const usersRows = result.filter(r => r.name !== null)
      const ordersRows = result.filter(r => r.product !== null)
      expect(usersRows).toHaveLength(5)
      expect(ordersRows).toHaveLength(2)
    })
  })

  describe('null values in join columns', () => {
    const usersWithNull = [
      { id: 1, name: 'Alice' },
      { id: null, name: 'Bob' },
      { id: 3, name: 'Charlie' },
    ]

    const ordersWithNull = [
      { id: 1, user_id: 1, product: 'Laptop' },
      { id: 2, user_id: null, product: 'Mouse' },
      { id: 3, user_id: 3, product: 'Keyboard' },
    ]

    it('should not match null values in INNER JOIN (SQL semantics)', async () => {
      const result = await collect(executeSql({
        tables: { users: usersWithNull, orders: ordersWithNull },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      // SQL semantics: NULL != NULL, so only Alice and Charlie match
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie'])
    })

    it('should preserve rows with null join columns in LEFT JOIN', async () => {
      const result = await collect(executeSql({
        tables: { users: usersWithNull, orders: ordersWithNull },
        query: 'SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id',
      }))
      // All 3 users should appear; Bob won't match any order (null != null)
      expect(result).toHaveLength(3)
      const bob = result.find(r => r.name === 'Bob')
      expect(bob.product == null).toBe(true)
    })

    it('should preserve rows with null join columns in RIGHT JOIN', async () => {
      const result = await collect(executeSql({
        tables: { users: usersWithNull, orders: ordersWithNull },
        query: 'SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id',
      }))
      // All 3 orders should appear; Mouse won't match any user (null != null)
      expect(result).toHaveLength(3)
      const mouse = result.find(r => r.product === 'Mouse')
      expect(mouse.name == null).toBe(true)
    })
  })

  describe('one-to-many relationships', () => {
    it('should produce multiple rows when left row matches multiple right rows', async () => {
      // Alice has 2 orders in the original data
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE users.name = \'Alice\'',
      }))
      expect(result).toHaveLength(2)
      expect(result.every(r => r.name === 'Alice')).toBe(true)
      expect(result.map(r => r.product).sort()).toEqual(['Laptop', 'Mouse'])
    })

    it('should produce multiple rows when right row matches multiple left rows', async () => {
      const teams = [
        { id: 1, name: 'Team A', city: 'NYC' },
        { id: 2, name: 'Team B', city: 'NYC' },
        { id: 3, name: 'Team C', city: 'LA' },
      ]
      const cities = [
        { name: 'NYC', country: 'USA' },
        { name: 'LA', country: 'USA' },
      ]
      const result = await collect(executeSql({
        tables: { teams, cities },
        query: 'SELECT teams.name AS team, cities.name AS city FROM teams JOIN cities ON teams.city = cities.name',
      }))
      expect(result).toHaveLength(3)
      const nycTeams = result.filter(r => r.city === 'NYC')
      expect(nycTeams).toHaveLength(2)
    })
  })

  describe('self-join', () => {
    it('should handle self-join with table aliases', async () => {
      const employees = [
        { id: 1, name: 'Alice', manager_id: null },
        { id: 2, name: 'Bob', manager_id: 1 },
        { id: 3, name: 'Charlie', manager_id: 1 },
        { id: 4, name: 'Diana', manager_id: 2 },
      ]
      const result = await collect(executeSql({
        tables: { employees, managers: employees },
        query: `
          SELECT employees.name AS employee, managers.name AS manager
          FROM employees
          JOIN managers ON employees.manager_id = managers.id
        `,
      }))
      expect(result).toHaveLength(3)
      expect(result.find(r => r.employee === 'Bob').manager).toBe('Alice')
      expect(result.find(r => r.employee === 'Charlie').manager).toBe('Alice')
      expect(result.find(r => r.employee === 'Diana').manager).toBe('Bob')
    })

    it('should handle self-join using table aliases', async () => {
      const employees = [
        { id: 1, name: 'Alice', manager_id: null },
        { id: 2, name: 'Bob', manager_id: 1 },
        { id: 3, name: 'Charlie', manager_id: 1 },
        { id: 4, name: 'Diana', manager_id: 2 },
      ]

      const result = await collect(executeSql({
        tables: { employees },
        query: `
          SELECT e.name AS employee, m.name AS manager
          FROM employees e
          LEFT JOIN employees m ON e.manager_id = m.id
          ORDER BY e.id
        `,
      }))

      expect(result).toHaveLength(4)

      expect(result[0]).toEqual({ employee: 'Alice', manager: null })
      expect(result[1]).toEqual({ employee: 'Bob', manager: 'Alice' })
      expect(result[2]).toEqual({ employee: 'Charlie', manager: 'Alice' })
      expect(result[3]).toEqual({ employee: 'Diana', manager: 'Bob' })
    })
  })

  describe('complex ON conditions', () => {
    it('should handle multiple conditions with AND in ON clause', async () => {
      const products = [
        { id: 1, name: 'Laptop', category: 'Electronics', price: 1000 },
        { id: 2, name: 'Mouse', category: 'Electronics', price: 50 },
        { id: 3, name: 'Desk', category: 'Furniture', price: 500 },
      ]
      const discounts = [
        { category: 'Electronics', min_price: 100, discount: 0.1 },
        { category: 'Furniture', min_price: 200, discount: 0.15 },
      ]
      const result = await collect(executeSql({
        tables: { products, discounts },
        query: `
          SELECT products.name, discounts.discount
          FROM products
          JOIN discounts ON products.category = discounts.category AND products.price >= discounts.min_price
        `,
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Desk', 'Laptop'])
    })

    it('should handle non-equality join conditions', async () => {
      const ranges = [
        { id: 1, min: 0, max: 25, label: 'young' },
        { id: 2, min: 26, max: 35, label: 'adult' },
        { id: 3, min: 36, max: 100, label: 'senior' },
      ]
      const result = await collect(executeSql({
        tables: { users, ranges },
        query: `
          SELECT users.name, ranges.label
          FROM users
          JOIN ranges ON users.age >= ranges.min AND users.age <= ranges.max
        `,
      }))
      expect(result).toHaveLength(5)
      const bob = result.find(r => r.name === 'Bob')
      expect(bob.label).toBe('young')
      const charlie = result.find(r => r.name === 'Charlie')
      expect(charlie.label).toBe('adult')
    })
  })

  describe('JOIN with DISTINCT', () => {
    it('should return distinct rows after JOIN', async () => {
      const tags = [
        { user_id: 1, tag: 'developer' },
        { user_id: 1, tag: 'manager' },
        { user_id: 2, tag: 'developer' },
      ]
      const result = await collect(executeSql({
        tables: { users, tags },
        query: 'SELECT DISTINCT users.city FROM users JOIN tags ON users.id = tags.user_id',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.city).sort()).toEqual(['LA', 'NYC'])
    })
  })

  describe('mixed JOIN types', () => {
    it('should handle LEFT JOIN followed by INNER JOIN', async () => {
      const categories = [
        { name: 'Electronics', department: 'Tech' },
        { name: 'Furniture', department: 'Home' },
      ]
      const products = [
        { id: 1, name: 'Laptop', category: 'Electronics' },
        { id: 2, name: 'Mouse', category: 'Electronics' },
      ]
      const result = await collect(executeSql({
        tables: { users, orders, products, categories },
        query: `
          SELECT users.name, orders.product, categories.department
          FROM users
          LEFT JOIN orders ON users.id = orders.user_id
          JOIN products ON orders.product = products.name
          JOIN categories ON products.category = categories.name
        `,
      }))
      // Only users with orders that have matching products and categories
      expect(result.every(r => r.department === 'Tech')).toBe(true)
    })

    it('should handle INNER JOIN followed by LEFT JOIN', async () => {
      const profiles = [
        { user_id: 1, bio: 'Hello' },
        { user_id: 2, bio: 'World' },
      ]
      const result = await collect(executeSql({
        tables: { users, orders, profiles },
        query: `
          SELECT users.name, orders.product, profiles.bio
          FROM users
          JOIN orders ON users.id = orders.user_id
          LEFT JOIN profiles ON users.id = profiles.user_id
        `,
      }))
      // All joined user-order pairs, with profile info where available
      expect(result).toHaveLength(4)
      const diana = result.find(r => r.name === 'Diana')
      expect(diana.bio).toBeNull()
    })
  })

  describe('JOIN with ORDER BY', () => {
    it('should order results by column from left table', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT users.name, orders.product
          FROM users
          JOIN orders ON users.id = orders.user_id
          ORDER BY users.name ASC
        `,
      }))
      expect(result[0].name).toBe('Alice')
      expect(result[result.length - 1].name).toBe('Diana')
    })

    it('should order results by column from right table', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT users.name, orders.product
          FROM users
          JOIN orders ON users.id = orders.user_id
          ORDER BY orders.product ASC
        `,
      }))
      expect(result[0].product).toBe('Keyboard')
      expect(result[result.length - 1].product).toBe('Mouse')
    })

    it('should order by multiple columns from different tables', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT users.name, orders.product
          FROM users
          JOIN orders ON users.id = orders.user_id
          ORDER BY users.name ASC, orders.amount DESC
        `,
      }))
      // Alice's orders should be Laptop (1000) then Mouse (50)
      const aliceOrders = result.filter(r => r.name === 'Alice')
      expect(aliceOrders[0].product).toBe('Laptop')
      expect(aliceOrders[1].product).toBe('Mouse')
    })
  })

  describe('JOIN with LIMIT and OFFSET', () => {
    it('should apply LIMIT after JOIN', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT users.name, orders.product
          FROM users
          JOIN orders ON users.id = orders.user_id
          ORDER BY orders.id
          LIMIT 2
        `,
      }))
      expect(result).toHaveLength(2)
    })

    it('should apply OFFSET after JOIN', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT users.name, orders.product
          FROM users
          JOIN orders ON users.id = orders.user_id
          ORDER BY orders.id
          LIMIT 2 OFFSET 1
        `,
      }))
      expect(result).toHaveLength(2)
      // Should skip first row
      expect(result[0].product).toBe('Mouse')
    })
  })

  describe('JOIN on different data types', () => {
    it('should join on string columns', async () => {
      const cities = [
        { name: 'NYC', population: 8000000 },
        { name: 'LA', population: 4000000 },
      ]
      const result = await collect(executeSql({
        tables: { users, cities },
        query: 'SELECT users.name, cities.population FROM users JOIN cities ON users.city = cities.name',
      }))
      expect(result).toHaveLength(5)
      const alice = result.find(r => r.name === 'Alice')
      expect(alice.population).toBe(8000000)
    })

    it('should join on boolean columns', async () => {
      const statusInfo = [
        { active: true, label: 'Active User' },
        { active: false, label: 'Inactive User' },
      ]
      const result = await collect(executeSql({
        tables: { users, statusInfo },
        query: 'SELECT users.name, statusInfo.label FROM users JOIN statusInfo ON users.active = statusInfo.active',
      }))
      expect(result).toHaveLength(5)
      const charlie = result.find(r => r.name === 'Charlie')
      expect(charlie.label).toBe('Inactive User')
    })
  })

  describe('column name conflicts', () => {
    it('should handle both tables having same column name', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT users.id AS user_id, orders.id AS order_id, users.name FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.id',
      }))
      expect(result[0]).toHaveProperty('user_id')
      expect(result[0]).toHaveProperty('order_id')
      // Find a row where user_id differs from order_id (e.g., order.id=2 for user.id=1)
      const secondRow = result[1] // Order id=2, user_id=1 (Alice's second order)
      expect(secondRow.user_id).toBe(1)
      expect(secondRow.order_id).toBe(2)
    })

    it('should select all columns with * preserving both id columns', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT * FROM users JOIN orders ON users.id = orders.user_id LIMIT 1',
      }))
      // Both tables have 'id' column - implementation may prefix or keep last
      expect(result[0]).toHaveProperty('id')
      expect(result[0]).toHaveProperty('user_id')
    })
  })

  describe('JOIN with GROUP BY and HAVING', () => {
    it('should group after JOIN', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT users.name, COUNT(*) AS order_count
          FROM users
          JOIN orders ON users.id = orders.user_id
          GROUP BY users.name
        `,
      }))
      expect(result).toHaveLength(3)
      const alice = result.find(r => r.name === 'Alice')
      expect(alice.order_count).toBe(2)
    })

    it('should apply HAVING after JOIN and GROUP BY', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: `
          SELECT users.name, COUNT(*) AS order_count
          FROM users
          JOIN orders ON users.id = orders.user_id
          GROUP BY users.name
          HAVING COUNT(*) > 1
        `,
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice')
    })
  })

  describe('chained three-way JOINs', () => {
    it('should handle three table JOINs with all matching', async () => {
      const products = [
        { name: 'Laptop', category_id: 1 },
        { name: 'Mouse', category_id: 1 },
        { name: 'Keyboard', category_id: 1 },
        { name: 'Monitor', category_id: 1 },
      ]
      const categories = [
        { id: 1, name: 'Electronics' },
        { id: 2, name: 'Furniture' },
      ]
      const result = await collect(executeSql({
        tables: { users, orders, products, categories },
        query: `
          SELECT users.name, orders.product, categories.name AS category
          FROM users
          JOIN orders ON users.id = orders.user_id
          JOIN products ON orders.product = products.name
          JOIN categories ON products.category_id = categories.id
        `,
      }))
      expect(result).toHaveLength(4)
      expect(result.every(r => r.category === 'Electronics')).toBe(true)
    })

    it('should handle partial matches across three tables', async () => {
      const products = [
        { name: 'Laptop', category_id: 1 },
        { name: 'Mouse', category_id: 1 },
        // Keyboard and Monitor are missing - no category lookup will fail
      ]
      const categories = [
        { id: 1, name: 'Electronics' },
      ]
      const result = await collect(executeSql({
        tables: { users, orders, products, categories },
        query: `
          SELECT users.name, orders.product, categories.name AS category
          FROM users
          JOIN orders ON users.id = orders.user_id
          JOIN products ON orders.product = products.name
          JOIN categories ON products.category_id = categories.id
        `,
      }))
      // Only Alice's Laptop and Mouse orders have matching products
      expect(result).toHaveLength(2)
      expect(result.every(r => r.name === 'Alice')).toBe(true)
    })
  })

  describe('single row tables', () => {
    it('should handle single row in left table', async () => {
      const singleUser = [{ id: 1, name: 'Alice' }]
      const result = await collect(executeSql({
        tables: { users: singleUser, orders },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(2) // Alice has 2 orders
      expect(result.every(r => r.name === 'Alice')).toBe(true)
    })

    it('should handle single row in right table', async () => {
      const singleOrder = [{ id: 1, user_id: 1, product: 'Laptop' }]
      const result = await collect(executeSql({
        tables: { users, orders: singleOrder },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('Alice')
      expect(result[0].product).toBe('Laptop')
    })

    it('should handle single row in both tables with match', async () => {
      const singleUser = [{ id: 1, name: 'Alice' }]
      const singleOrder = [{ id: 1, user_id: 1, product: 'Laptop' }]
      const result = await collect(executeSql({
        tables: { users: singleUser, orders: singleOrder },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(1)
    })

    it('should handle single row in both tables without match', async () => {
      const singleUser = [{ id: 1, name: 'Alice' }]
      const singleOrder = [{ id: 1, user_id: 999, product: 'Laptop' }]
      const result = await collect(executeSql({
        tables: { users: singleUser, orders: singleOrder },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(0)
    })
  })

  describe('duplicate rows in tables', () => {
    it('should handle duplicate rows in left table', async () => {
      const dupeUsers = [
        { id: 1, name: 'Alice' },
        { id: 1, name: 'Alice' }, // duplicate
        { id: 2, name: 'Bob' },
      ]
      const simpleOrders = [{ id: 1, user_id: 1, product: 'Laptop' }]
      const result = await collect(executeSql({
        tables: { users: dupeUsers, orders: simpleOrders },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(2) // Both Alice rows match
    })

    it('should handle duplicate rows in right table', async () => {
      const singleUser = [{ id: 1, name: 'Alice' }]
      const dupeOrders = [
        { id: 1, user_id: 1, product: 'Laptop' },
        { id: 2, user_id: 1, product: 'Laptop' }, // duplicate product
      ]
      const result = await collect(executeSql({
        tables: { users: singleUser, orders: dupeOrders },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(2)
    })
  })

  describe('complex ON conditions with outer joins (nested loop fallback)', () => {
    const products = [
      { id: 1, name: 'Laptop', category: 'Electronics', price: 1000 },
      { id: 2, name: 'Mouse', category: 'Electronics', price: 50 },
      { id: 3, name: 'Desk', category: 'Furniture', price: 500 },
      { id: 4, name: 'Chair', category: 'Furniture', price: 150 },
    ]

    const discounts = [
      { category: 'Electronics', min_price: 100, discount: 0.1 },
      { category: 'Furniture', min_price: 300, discount: 0.15 },
    ]

    it('should handle LEFT JOIN with complex ON where some left rows do not match', async () => {
      // Mouse (50) and Chair (150) don't meet min_price requirements
      const result = await collect(executeSql({
        tables: { products, discounts },
        query: `
          SELECT products.name, discounts.discount
          FROM products
          LEFT JOIN discounts ON products.category = discounts.category AND products.price >= discounts.min_price
        `,
      }))
      expect(result).toHaveLength(4)
      const laptop = result.find(r => r.name === 'Laptop')
      expect(laptop.discount).toBe(0.1)
      const mouse = result.find(r => r.name === 'Mouse')
      expect(mouse.discount == null).toBe(true)
      const desk = result.find(r => r.name === 'Desk')
      expect(desk.discount).toBe(0.15)
      const chair = result.find(r => r.name === 'Chair')
      expect(chair.discount == null).toBe(true)
    })

    it('should handle RIGHT JOIN with complex ON where some right rows do not match', async () => {
      const smallProducts = [
        { id: 1, name: 'Laptop', category: 'Electronics', price: 1000 },
      ]
      // Furniture discount has no matching product
      const result = await collect(executeSql({
        tables: { products: smallProducts, discounts },
        query: `
          SELECT products.name, discounts.category AS discount_category, discounts.discount
          FROM products
          RIGHT JOIN discounts ON products.category = discounts.category AND products.price >= discounts.min_price
        `,
      }))
      expect(result).toHaveLength(2)
      const electronics = result.find(r => r.discount_category === 'Electronics')
      expect(electronics.name).toBe('Laptop')
      const furniture = result.find(r => r.discount_category === 'Furniture')
      expect(furniture.name == null).toBe(true)
    })

    it('should handle FULL JOIN with complex ON where rows on both sides do not match', async () => {
      const someProducts = [
        { id: 1, name: 'Laptop', category: 'Electronics', price: 1000 },
        { id: 2, name: 'Mouse', category: 'Electronics', price: 50 }, // too cheap
      ]
      const someDiscounts = [
        { category: 'Electronics', min_price: 100, discount: 0.1 },
        { category: 'Clothing', min_price: 50, discount: 0.2 }, // no matching product category
      ]
      const result = await collect(executeSql({
        tables: { products: someProducts, discounts: someDiscounts },
        query: `
          SELECT products.name, discounts.category AS discount_category, discounts.discount
          FROM products
          FULL JOIN discounts ON products.category = discounts.category AND products.price >= discounts.min_price
        `,
      }))
      // Laptop matches Electronics, Mouse doesn't match (too cheap), Clothing has no product
      expect(result).toHaveLength(3)
      const laptop = result.find(r => r.name === 'Laptop')
      expect(laptop.discount).toBe(0.1)
      const mouse = result.find(r => r.name === 'Mouse')
      expect(mouse.discount == null).toBe(true)
      const clothing = result.find(r => r.discount_category === 'Clothing')
      expect(clothing.name == null).toBe(true)
      expect(clothing.discount).toBe(0.2)
    })
  })

  describe('JOIN key position in ON clause', () => {
    // Bug: extractJoinKeys assumes ON condition is written as `left_table.col = right_table.col`
    // but SQL allows either order. The code takes onCondition.left as leftKey and
    // onCondition.right as rightKey, then evaluates leftKey on leftRows and rightKey on rightRows.
    // This breaks when the ON clause is written as `right_table.col = left_table.col`.

    const users = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]

    const orders = [
      { id: 1, user_id: 1, product: 'Laptop' },
      { id: 2, user_id: 2, product: 'Mouse' },
    ]

    it('should work with ON left_table.col = right_table.col (standard order)', async () => {
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])
    })

    it('should work with ON right_table.col = left_table.col (swapped order)', async () => {
      // This test demonstrates the bug: swapping the operand order in the ON clause
      // causes the join to fail because extractJoinKeys doesn't detect which
      // expression belongs to which table
      const result = await collect(executeSql({
        tables: { users, orders },
        query: 'SELECT users.name, orders.product FROM users JOIN orders ON orders.user_id = users.id',
      }))
      expect(result).toHaveLength(2)
      expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])
    })
  })
})
