import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/** @type {null} */
const NULL = null

describe('math functions', () => {
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
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT FLOOR(value) AS floored FROM data',
      }))
      expect(result[0].floored).toBeNull()
    })

    it('should work with literal values', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
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
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT CEIL(value) AS ceiled FROM data',
      }))
      expect(result[0].ceiled).toBeNull()
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
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
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

    it('should work with literal values', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT MOD(17, 5) AS remainder FROM data',
      }))
      expect(result[0].remainder).toBe(2)
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
      expect(result[1].exp_val).toBeCloseTo(Math.E, 10) // e^1 = e
    })

    it('should handle negative exponents', async () => {
      const data = [{ id: 1, value: -1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT EXP(value) AS exp_val FROM data',
      }))
      expect(result[0].exp_val).toBeCloseTo(1 / Math.E, 10)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
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
      expect(result[1].ln_val).toBeCloseTo(1, 10) // ln(e) = 1
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
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
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
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
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
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
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
      expect(result[2].sqrt_val).toBeCloseTo(Math.SQRT2, 10)
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
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
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
      expect(result[0].nested).toBeCloseTo(Math.sqrt(2.5), 10)
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

  describe('SIN', () => {
    it('should return sine of angle in radians', async () => {
      const data = [
        { id: 1, value: 0 },
        { id: 2, value: Math.PI / 2 },
        { id: 3, value: Math.PI },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SIN(value) AS sin_val FROM data',
      }))
      expect(result[0].sin_val).toBeCloseTo(0, 10)
      expect(result[1].sin_val).toBeCloseTo(1, 10)
      expect(result[2].sin_val).toBeCloseTo(0, 10)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SIN(value) AS sin_val FROM data',
      }))
      expect(result[0].sin_val).toBeNull()
    })

    it('should work with literal values', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SIN(0) AS sin_val FROM data',
      }))
      expect(result[0].sin_val).toBe(0)
    })
  })

  describe('COS', () => {
    it('should return cosine of angle in radians', async () => {
      const data = [
        { id: 1, value: 0 },
        { id: 2, value: Math.PI / 2 },
        { id: 3, value: Math.PI },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT COS(value) AS cos_val FROM data',
      }))
      expect(result[0].cos_val).toBeCloseTo(1, 10)
      expect(result[1].cos_val).toBeCloseTo(0, 10)
      expect(result[2].cos_val).toBeCloseTo(-1, 10)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT COS(value) AS cos_val FROM data',
      }))
      expect(result[0].cos_val).toBeNull()
    })
  })

  describe('TAN', () => {
    it('should return tangent of angle in radians', async () => {
      const data = [
        { id: 1, value: 0 },
        { id: 2, value: Math.PI / 4 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT TAN(value) AS tan_val FROM data',
      }))
      expect(result[0].tan_val).toBeCloseTo(0, 10)
      expect(result[1].tan_val).toBeCloseTo(1, 10)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT TAN(value) AS tan_val FROM data',
      }))
      expect(result[0].tan_val).toBeNull()
    })
  })

  describe('COT', () => {
    it('should return cotangent of angle in radians', async () => {
      const data = [
        { id: 1, value: Math.PI / 4 },
        { id: 2, value: Math.PI / 2 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT COT(value) AS cot_val FROM data',
      }))
      expect(result[0].cot_val).toBeCloseTo(1, 10)
      expect(result[1].cot_val).toBeCloseTo(0, 10)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT COT(value) AS cot_val FROM data',
      }))
      expect(result[0].cot_val).toBeNull()
    })
  })

  describe('ASIN', () => {
    it('should return arcsine in radians', async () => {
      const data = [
        { id: 1, value: 0 },
        { id: 2, value: 1 },
        { id: 3, value: -1 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ASIN(value) AS asin_val FROM data',
      }))
      expect(result[0].asin_val).toBeCloseTo(0, 10)
      expect(result[1].asin_val).toBeCloseTo(Math.PI / 2, 10)
      expect(result[2].asin_val).toBeCloseTo(-Math.PI / 2, 10)
    })

    it('should return NaN for values outside [-1, 1]', async () => {
      const data = [{ id: 1, value: 2 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ASIN(value) AS asin_val FROM data',
      }))
      expect(result[0].asin_val).toBeNaN()
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ASIN(value) AS asin_val FROM data',
      }))
      expect(result[0].asin_val).toBeNull()
    })
  })

  describe('ACOS', () => {
    it('should return arccosine in radians', async () => {
      const data = [
        { id: 1, value: 1 },
        { id: 2, value: 0 },
        { id: 3, value: -1 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ACOS(value) AS acos_val FROM data',
      }))
      expect(result[0].acos_val).toBeCloseTo(0, 10)
      expect(result[1].acos_val).toBeCloseTo(Math.PI / 2, 10)
      expect(result[2].acos_val).toBeCloseTo(Math.PI, 10)
    })

    it('should return NaN for values outside [-1, 1]', async () => {
      const data = [{ id: 1, value: 2 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ACOS(value) AS acos_val FROM data',
      }))
      expect(result[0].acos_val).toBeNaN()
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ACOS(value) AS acos_val FROM data',
      }))
      expect(result[0].acos_val).toBeNull()
    })
  })

  describe('ATAN', () => {
    it('should return arctangent in radians', async () => {
      const data = [
        { id: 1, value: 0 },
        { id: 2, value: 1 },
        { id: 3, value: -1 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ATAN(value) AS atan_val FROM data',
      }))
      expect(result[0].atan_val).toBeCloseTo(0, 10)
      expect(result[1].atan_val).toBeCloseTo(Math.PI / 4, 10)
      expect(result[2].atan_val).toBeCloseTo(-Math.PI / 4, 10)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ATAN(value) AS atan_val FROM data',
      }))
      expect(result[0].atan_val).toBeNull()
    })

    it('should return two-argument arctangent when given 2 args', async () => {
      // Postgres supports ATAN(y, x)
      const data = [
        { id: 1, y: 0, x: 1 },
        { id: 2, y: 1, x: 1 },
        { id: 3, y: 1, x: 0 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ATAN(y, x) AS atan_val FROM data',
      }))
      expect(result[0].atan_val).toBeCloseTo(0, 10)
      expect(result[1].atan_val).toBeCloseTo(Math.PI / 4, 10)
      expect(result[2].atan_val).toBeCloseTo(Math.PI / 2, 10)
    })
  })

  describe('ATAN2', () => {
    it('should return two-argument arctangent in radians', async () => {
      const data = [
        { id: 1, y: 0, x: 1 },
        { id: 2, y: 1, x: 1 },
        { id: 3, y: 1, x: 0 },
        { id: 4, y: -1, x: -1 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ATAN2(y, x) AS atan2_val FROM data',
      }))
      expect(result[0].atan2_val).toBeCloseTo(0, 10)
      expect(result[1].atan2_val).toBeCloseTo(Math.PI / 4, 10)
      expect(result[2].atan2_val).toBeCloseTo(Math.PI / 2, 10)
      expect(result[3].atan2_val).toBeCloseTo(-3 * Math.PI / 4, 10)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, y: NULL, x: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ATAN2(y, x) AS atan2_val FROM data',
      }))
      expect(result[0].atan2_val).toBeNull()
    })

    it('should work with literal values', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ATAN2(1, 1) AS atan2_val FROM data',
      }))
      expect(result[0].atan2_val).toBeCloseTo(Math.PI / 4, 10)
    })
  })

  describe('DEGREES', () => {
    it('should convert radians to degrees', async () => {
      const data = [
        { id: 1, value: 0 },
        { id: 2, value: Math.PI / 2 },
        { id: 3, value: Math.PI },
        { id: 4, value: 2 * Math.PI },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT DEGREES(value) AS degrees FROM data',
      }))
      expect(result[0].degrees).toBeCloseTo(0, 10)
      expect(result[1].degrees).toBeCloseTo(90, 10)
      expect(result[2].degrees).toBeCloseTo(180, 10)
      expect(result[3].degrees).toBeCloseTo(360, 10)
    })

    it('should handle negative radians', async () => {
      const data = [{ id: 1, value: -Math.PI }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT DEGREES(value) AS degrees FROM data',
      }))
      expect(result[0].degrees).toBeCloseTo(-180, 10)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT DEGREES(value) AS degrees FROM data',
      }))
      expect(result[0].degrees).toBeNull()
    })
  })

  describe('RADIANS', () => {
    it('should convert degrees to radians', async () => {
      const data = [
        { id: 1, value: 0 },
        { id: 2, value: 90 },
        { id: 3, value: 180 },
        { id: 4, value: 360 },
      ]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT RADIANS(value) AS radians FROM data',
      }))
      expect(result[0].radians).toBeCloseTo(0, 10)
      expect(result[1].radians).toBeCloseTo(Math.PI / 2, 10)
      expect(result[2].radians).toBeCloseTo(Math.PI, 10)
      expect(result[3].radians).toBeCloseTo(2 * Math.PI, 10)
    })

    it('should handle negative degrees', async () => {
      const data = [{ id: 1, value: -180 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT RADIANS(value) AS radians FROM data',
      }))
      expect(result[0].radians).toBeCloseTo(-Math.PI, 10)
    })

    it('should handle null values', async () => {
      const data = [{ id: 1, value: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT RADIANS(value) AS radians FROM data',
      }))
      expect(result[0].radians).toBeNull()
    })
  })

  describe('PI', () => {
    it('should return the value of PI', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT PI() AS pi_val FROM data',
      }))
      expect(result[0].pi_val).toBe(Math.PI)
    })

    it('should work in expressions', async () => {
      const data = [{ id: 1, degrees: 180 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT degrees * PI() / 180 AS radians FROM data',
      }))
      expect(result[0].radians).toBeCloseTo(Math.PI, 10)
    })
  })

  describe('combined trigonometric functions', () => {
    it('should work with degree/radian conversion', async () => {
      const data = [{ id: 1, degrees: 45 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SIN(RADIANS(degrees)) AS sin_val FROM data',
      }))
      expect(result[0].sin_val).toBeCloseTo(Math.sqrt(2) / 2, 10)
    })

    it('should verify trigonometric identity', async () => {
      const data = [{ id: 1, angle: Math.PI / 3 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SIN(angle) * SIN(angle) + COS(angle) * COS(angle) AS identity FROM data',
      }))
      expect(result[0].identity).toBeCloseTo(1, 10)
    })

    it('should work with inverse functions', async () => {
      const data = [{ id: 1, value: 0.5 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SIN(ASIN(value)) AS original FROM data',
      }))
      expect(result[0].original).toBeCloseTo(0.5, 10)
    })
  })

  describe('error handling', () => {
    it('should throw for FLOOR with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT FLOOR(value, 2) FROM data',
      }))).rejects.toThrow('FLOOR(number) function requires 1 argument, got 2')
    })

    it('should throw for MOD with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT MOD(value) FROM data',
      }))).rejects.toThrow('MOD(dividend, divisor) function requires 2 arguments, got 1')
    })

    it('should throw for POWER with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT POWER(value) FROM data',
      }))).rejects.toThrow('POWER(base, exponent) function requires 2 arguments, got 1')
    })

    it('should throw for SIN with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT SIN(value, 2) FROM data',
      }))).rejects.toThrow('SIN(radians) function requires 1 argument, got 2')
    })

    it('should throw for ATAN2 with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ATAN2(value) FROM data',
      }))).rejects.toThrow('ATAN2(y, x) function requires 2 arguments, got 1')
    })

    it('should throw for PI with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT PI(value) FROM data',
      }))).rejects.toThrow('PI() function requires no arguments, got 1')
    })

    it('should throw for ATAN with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ATAN(value, 2, 3) FROM data',
      }))).rejects.toThrow('ATAN(number) function requires 1 or 2 arguments, got 3')
    })

    it('should throw for DEGREES with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT DEGREES(value, 2) FROM data',
      }))).rejects.toThrow('DEGREES(radians) function requires 1 argument, got 2')
    })

    it('should throw for RADIANS with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT RADIANS(value, 2) FROM data',
      }))).rejects.toThrow('RADIANS(degrees) function requires 1 argument, got 2')
    })

    it('should throw for CEIL with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT CEIL(value, 2) FROM data',
      }))).rejects.toThrow('CEIL(number) function requires 1 argument, got 2')
    })

    it('should throw for LN with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT LN(value, 2) FROM data',
      }))).rejects.toThrow('LN(number) function requires 1 argument, got 2')
    })

    it('should throw for ASIN with wrong argument count', async () => {
      const data = [{ id: 1, value: 0.5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ASIN(value, 2) FROM data',
      }))).rejects.toThrow('ASIN(number) function requires 1 argument, got 2')
    })

    it('should throw for ACOS with wrong argument count', async () => {
      const data = [{ id: 1, value: 0.5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ACOS(value, 2) FROM data',
      }))).rejects.toThrow('ACOS(number) function requires 1 argument, got 2')
    })

    it('should throw for COS with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT COS(value, 2) FROM data',
      }))).rejects.toThrow('COS(radians) function requires 1 argument, got 2')
    })

    it('should throw for TAN with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT TAN(value, 2) FROM data',
      }))).rejects.toThrow('TAN(radians) function requires 1 argument, got 2')
    })

    it('should throw for COT with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT COT(value, 2) FROM data',
      }))).rejects.toThrow('COT(radians) function requires 1 argument, got 2')
    })

    it('should throw for ABS with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ABS(value, 2) FROM data',
      }))).rejects.toThrow('ABS(number) function requires 1 argument, got 2')
    })

    it('should throw for EXP with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT EXP(value, 2) FROM data',
      }))).rejects.toThrow('EXP(number) function requires 1 argument, got 2')
    })

    it('should throw for LOG10 with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT LOG10(value, 2) FROM data',
      }))).rejects.toThrow('LOG10(number) function requires 1 argument, got 2')
    })

    it('should throw for SQRT with wrong argument count', async () => {
      const data = [{ id: 1, value: 5 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT SQRT(value, 2) FROM data',
      }))).rejects.toThrow('SQRT(number) function requires 1 argument, got 2')
    })
  })
})
