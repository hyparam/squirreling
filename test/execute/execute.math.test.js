import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/** @type {null} */
const NULL = null

describe('math functions', () => {
  // Shared test data
  const nullData = [{ id: 1, value: NULL }]
  const singleRow = [{ id: 1 }]
  const errorData = [{ id: 1, value: 5 }]
  const numbers = [
    { id: 1, value: 5.7, negative: -3.2 },
    { id: 2, value: 2.3, negative: -8.9 },
    { id: 3, value: 10, negative: -10 },
    { id: 4, value: 0, negative: 0 },
  ]

  describe('FLOOR', () => {
    it('should round down positive decimals', async () => {
      const result = await collect(executeSql({
        tables: { numbers },
        query: 'SELECT FLOOR(value) AS floored FROM numbers',
      }))
      expect(result).toEqual([
        { floored: 5 },
        { floored: 2 },
        { floored: 10 },
        { floored: 0 },
      ])
    })

    it('should round down negative decimals', async () => {
      const result = await collect(executeSql({
        tables: { numbers },
        query: 'SELECT FLOOR(negative) AS floored FROM numbers',
      }))
      expect(result).toEqual([
        { floored: -4 },
        { floored: -9 },
        { floored: -10 },
        { floored: 0 },
      ])
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT FLOOR(value) AS floored FROM data',
      }))
      expect(result[0].floored).toBeNull()
    })

    it('should work with literal values', async () => {
      const result = await collect(executeSql({
        tables: { data: singleRow },
        query: 'SELECT FLOOR(3.7) AS floored FROM data',
      }))
      expect(result[0].floored).toBe(3)
    })

    it('should work in WHERE clause', async () => {
      const result = await collect(executeSql({
        tables: { numbers },
        query: 'SELECT id, value FROM numbers WHERE FLOOR(value) = 5',
      }))
      expect(result).toHaveLength(1)
      expect(result[0].id).toBe(1)
    })
  })

  describe('CEIL / CEILING', () => {
    it('should round up positive decimals', async () => {
      const result = await collect(executeSql({
        tables: { numbers },
        query: 'SELECT CEIL(value) AS ceiled FROM numbers',
      }))
      expect(result).toEqual([
        { ceiled: 6 },
        { ceiled: 3 },
        { ceiled: 10 },
        { ceiled: 0 },
      ])
    })

    it('should round up negative decimals', async () => {
      const result = await collect(executeSql({
        tables: { numbers },
        query: 'SELECT CEIL(negative) AS ceiled FROM numbers',
      }))
      expect(result).toEqual([
        { ceiled: -3 },
        { ceiled: -8 },
        { ceiled: -10 },
        { ceiled: 0 },
      ])
    })

    it('should work with CEILING alias', async () => {
      const data = [{ id: 1, value: 2.1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT CEILING(value) AS ceiled FROM data',
      }))
      expect(result[0].ceiled).toBe(3)
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT CEIL(value) AS ceiled FROM data',
      }))
      expect(result[0].ceiled).toBeNull()
    })
  })

  describe('ROUND', () => {
    it('should round to nearest integer by default', async () => {
      const result = await collect(executeSql({
        tables: { numbers },
        query: 'SELECT ROUND(value) AS rounded FROM numbers',
      }))
      expect(result).toEqual([
        { rounded: 6 },
        { rounded: 2 },
        { rounded: 10 },
        { rounded: 0 },
      ])
    })

    it('should round negative numbers', async () => {
      const result = await collect(executeSql({
        tables: { numbers },
        query: 'SELECT ROUND(negative) AS rounded FROM numbers',
      }))
      expect(result).toEqual([
        { rounded: -3 },
        { rounded: -9 },
        { rounded: -10 },
        { rounded: 0 },
      ])
    })

    it('should round to specified decimal places', async () => {
      const data = [
        { id: 1, value: 3.14159 },
        { id: 2, value: 2.71828 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ROUND(value, 2) AS rounded FROM data',
      }))
      expect(result).toEqual([
        { rounded: 3.14 },
        { rounded: 2.72 },
      ])
    })

    it('should round to more decimal places', async () => {
      const data = [{ id: 1, value: 3.14159265 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ROUND(value, 4) AS rounded FROM data',
      }))
      expect(result[0].rounded).toBe(3.1416)
    })

    it('should handle zero decimal places explicitly', async () => {
      const data = [{ id: 1, value: 5.7 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ROUND(value, 0) AS rounded FROM data',
      }))
      expect(result[0].rounded).toBe(6)
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT ROUND(value) AS rounded FROM data',
      }))
      expect(result[0].rounded).toBeNull()
    })
  })

  describe('ABS', () => {
    it('should return absolute value of negative numbers', async () => {
      const result = await collect(executeSql({
        tables: { numbers },
        query: 'SELECT ABS(negative) AS absolute FROM numbers',
      }))
      expect(result).toEqual([
        { absolute: 3.2 },
        { absolute: 8.9 },
        { absolute: 10 },
        { absolute: 0 },
      ])
    })

    it('should return positive numbers unchanged', async () => {
      const result = await collect(executeSql({
        tables: { numbers },
        query: 'SELECT ABS(value) AS absolute FROM numbers',
      }))
      expect(result).toEqual([
        { absolute: 5.7 },
        { absolute: 2.3 },
        { absolute: 10 },
        { absolute: 0 },
      ])
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT ABS(value) AS absolute FROM data',
      }))
      expect(result[0].absolute).toBeNull()
    })

    it('should work in ORDER BY', async () => {
      const result = await collect(executeSql({
        tables: { numbers },
        query: 'SELECT id, negative FROM numbers ORDER BY ABS(negative)',
      }))
      expect(result[0].id).toBe(4) // 0
      expect(result[1].id).toBe(1) // -3.2
    })
  })

  describe('SIGN', () => {
    it('should return sign of numbers', async () => {
      const data = [
        { value: -5 },
        { value: 0 },
        { value: 10 },
        { value: null },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SIGN(value) AS sign FROM data',
      }))
      expect(result).toEqual([
        { sign: -1 },
        { sign: 0 },
        { sign: 1 },
        { sign: null },
      ])
    })
  })

  describe('MOD', () => {
    it('should return remainder of division', async () => {
      const data = [
        { id: 1, a: 10, b: 3 },
        { id: 2, a: 15, b: 4 },
        { id: 3, a: 20, b: 5 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT MOD(a, b) AS remainder FROM data',
      }))
      expect(result).toEqual([
        { remainder: 1 },
        { remainder: 3 },
        { remainder: 0 },
      ])
    })

    it('should handle negative dividend', async () => {
      const data = [{ id: 1, a: -10, b: 3 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT MOD(a, b) AS remainder FROM data',
      }))
      expect(result[0].remainder).toBe(-1)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, a: NULL, b: 3 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT MOD(a, b) AS remainder FROM data',
      }))
      expect(result[0].remainder).toBeNull()
    })
  })

  describe('EXP', () => {
    it('should return e raised to the power', async () => {
      const data = [{ id: 1, value: 0 }, { id: 2, value: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT EXP(value) AS exp_val FROM data',
      }))
      expect(result[0].exp_val).toBe(1) // e^0 = 1
      expect(result[1].exp_val).toBe(Math.E) // e^1 = e
    })

    it('should handle negative exponents', async () => {
      const data = [{ id: 1, value: -1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT EXP(value) AS exp_val FROM data',
      }))
      expect(result[0].exp_val).toBe(1 / Math.E)
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT EXP(value) AS exp_val FROM data',
      }))
      expect(result[0].exp_val).toBeNull()
    })
  })

  describe('LN', () => {
    it('should return natural logarithm', async () => {
      const data = [
        { id: 1, value: 1 },
        { id: 2, value: Math.E },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LN(value) AS ln_val FROM data',
      }))
      expect(result[0].ln_val).toBe(0) // ln(1) = 0
      expect(result[1].ln_val).toBe(1) // ln(e) = 1
    })

    it('should return -Infinity for ln(0)', async () => {
      const data = [{ id: 1, value: 0 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LN(value) AS ln_val FROM data',
      }))
      expect(result[0].ln_val).toBe(-Infinity)
    })

    it('should return NaN for negative numbers', async () => {
      const data = [{ id: 1, value: -1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LN(value) AS ln_val FROM data',
      }))
      expect(result[0].ln_val).toBeNaN()
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT LN(value) AS ln_val FROM data',
      }))
      expect(result[0].ln_val).toBeNull()
    })
  })

  describe('LOG10', () => {
    it('should return base-10 logarithm', async () => {
      const data = [
        { id: 1, value: 1 },
        { id: 2, value: 10 },
        { id: 3, value: 100 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LOG10(value) AS log_val FROM data',
      }))
      expect(result[0].log_val).toBe(0) // log10(1) = 0
      expect(result[1].log_val).toBe(1) // log10(10) = 1
      expect(result[2].log_val).toBe(2) // log10(100) = 2
    })

    it('should return -Infinity for log10(0)', async () => {
      const data = [{ id: 1, value: 0 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT LOG10(value) AS log_val FROM data',
      }))
      expect(result[0].log_val).toBe(-Infinity)
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT LOG10(value) AS log_val FROM data',
      }))
      expect(result[0].log_val).toBeNull()
    })
  })

  describe('POWER', () => {
    it('should raise base to exponent', async () => {
      const data = [
        { id: 1, base: 2, exp: 3 },
        { id: 2, base: 10, exp: 2 },
        { id: 3, base: 5, exp: 0 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT POWER(base, exp) AS power_val FROM data',
      }))
      expect(result).toEqual([
        { power_val: 8 },
        { power_val: 100 },
        { power_val: 1 },
      ])
    })

    it('should handle negative exponents', async () => {
      const data = [{ id: 1, base: 2, exp: -2 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT POWER(base, exp) AS power_val FROM data',
      }))
      expect(result[0].power_val).toBe(0.25)
    })

    it('should handle fractional exponents', async () => {
      const data = [{ id: 1, base: 4, exp: 0.5 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT POWER(base, exp) AS power_val FROM data',
      }))
      expect(result[0].power_val).toBe(2)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, base: NULL, exp: 2 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT POWER(base, exp) AS power_val FROM data',
      }))
      expect(result[0].power_val).toBeNull()
    })

    it('should work with literal values', async () => {
      const result = await collect(executeSql({
        tables: { data: singleRow },
        query: 'SELECT POWER(3, 4) AS power_val FROM data',
      }))
      expect(result[0].power_val).toBe(81)
    })
  })

  describe('SQRT', () => {
    it('should return square root of positive numbers', async () => {
      const data = [
        { id: 1, value: 4 },
        { id: 2, value: 9 },
        { id: 3, value: 2 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SQRT(value) AS sqrt_val FROM data',
      }))
      expect(result[0].sqrt_val).toBe(2)
      expect(result[1].sqrt_val).toBe(3)
      expect(result[2].sqrt_val).toBe(Math.SQRT2)
    })

    it('should return 0 for sqrt(0)', async () => {
      const data = [{ id: 1, value: 0 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SQRT(value) AS sqrt_val FROM data',
      }))
      expect(result[0].sqrt_val).toBe(0)
    })

    it('should return NaN for negative numbers', async () => {
      const data = [{ id: 1, value: -4 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SQRT(value) AS sqrt_val FROM data',
      }))
      expect(result[0].sqrt_val).toBeNaN()
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT SQRT(value) AS sqrt_val FROM data',
      }))
      expect(result[0].sqrt_val).toBeNull()
    })

    it('should work in expressions', async () => {
      const data = [{ id: 1, a: 3, b: 4 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SQRT(a * a + b * b) AS hypotenuse FROM data',
      }))
      expect(result[0].hypotenuse).toBe(5)
    })
  })

  describe('combined math functions', () => {
    it('should work with multiple math functions in one query', async () => {
      const data = [{ id: 1, value: -3.7 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ABS(value) AS abs_val, FLOOR(value) AS floor_val, CEIL(value) AS ceil_val FROM data',
      }))
      expect(result[0]).toEqual({
        abs_val: 3.7,
        floor_val: -4,
        ceil_val: -3,
      })
    })

    it('should work nested', async () => {
      const data = [{ id: 1, value: -2.5 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SQRT(ABS(value)) AS nested FROM data',
      }))
      expect(result[0].nested).toBe(Math.sqrt(2.5))
    })

    it('should work with GROUP BY and aggregates', async () => {
      const data = [
        { id: 1, category: 'A', value: 3.7 },
        { id: 2, category: 'A', value: 2.2 },
        { id: 3, category: 'B', value: 5.9 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT category, SUM(FLOOR(value)) AS sum_floored FROM data GROUP BY category',
      }))
      const catA = result.find(r => r.category === 'A')
      const catB = result.find(r => r.category === 'B')
      expect(catA?.sum_floored).toBe(5) // floor(3.7) + floor(2.2) = 3 + 2
      expect(catB?.sum_floored).toBe(5) // floor(5.9) = 5
    })
  })

  describe('error handling', () => {
    it('should throw for FLOOR with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT FLOOR(value, 2) FROM data',
      }))).rejects.toThrow('FLOOR(number) function requires 1 argument, got 2')
    })

    it('should throw for MOD with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT MOD(value) FROM data',
      }))).rejects.toThrow('MOD(dividend, divisor) function requires 2 arguments, got 1')
    })

    it('should throw for POWER with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT POWER(value) FROM data',
      }))).rejects.toThrow('POWER(base, exponent) function requires 2 arguments, got 1')
    })

    it('should throw for CEIL with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT CEIL(value, 2) FROM data',
      }))).rejects.toThrow('CEIL(number) function requires 1 argument, got 2')
    })

    it('should throw for LN with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT LN(value, 2) FROM data',
      }))).rejects.toThrow('LN(number) function requires 1 argument, got 2')
    })

    it('should throw for ABS with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT ABS(value, 2) FROM data',
      }))).rejects.toThrow('ABS(number) function requires 1 argument, got 2')
    })

    it('should throw for EXP with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT EXP(value, 2) FROM data',
      }))).rejects.toThrow('EXP(number) function requires 1 argument, got 2')
    })

    it('should throw for LOG10 with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT LOG10(value, 2) FROM data',
      }))).rejects.toThrow('LOG10(number) function requires 1 argument, got 2')
    })

    it('should throw for SQRT with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT SQRT(value, 2) FROM data',
      }))).rejects.toThrow('SQRT(number) function requires 1 argument, got 2')
    })

    it('should throw for ROUND with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT ROUND(value, 2, 3) FROM data',
      }))).rejects.toThrow('ROUND(number[, decimals]) function requires 1 or 2 arguments, got 3')
    })
  })
})
