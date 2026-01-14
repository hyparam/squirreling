import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/** @type {null} */
const NULL = null

describe('trigonometric functions', () => {
  // Shared test data
  const nullData = [{ id: 1, value: NULL }]
  const singleRow = [{ id: 1 }]
  const errorData = [{ id: 1, value: 5 }]
  const standardAngles = [
    { id: 1, value: 0 },
    { id: 2, value: Math.PI / 2 },
    { id: 3, value: Math.PI },
  ]
  const unitValues = [
    { id: 1, value: 0 },
    { id: 2, value: 1 },
    { id: 3, value: -1 },
  ]

  describe('SIN', () => {
    it('should return sine of angle in radians', async () => {
      const result = await collect(executeSql({
        tables: { data: standardAngles },
        query: 'SELECT SIN(value) AS sin_val FROM data',
      }))
      expect(result[0].sin_val).toBe(0)
      expect(result[1].sin_val).toBe(1)
      expect(result[2].sin_val).toBeCloseTo(0, 10)
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT SIN(value) AS sin_val FROM data',
      }))
      expect(result[0].sin_val).toBeNull()
    })

    it('should work with literal values', async () => {
      const result = await collect(executeSql({
        tables: { data: singleRow },
        query: 'SELECT SIN(0) AS sin_val FROM data',
      }))
      expect(result[0].sin_val).toBe(0)
    })
  })

  describe('COS', () => {
    it('should return cosine of angle in radians', async () => {
      const result = await collect(executeSql({
        tables: { data: standardAngles },
        query: 'SELECT COS(value) AS cos_val FROM data',
      }))
      expect(result[0].cos_val).toBe(1)
      expect(result[1].cos_val).toBeCloseTo(0, 10)
      expect(result[2].cos_val).toBe(-1)
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
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
      expect(result[0].tan_val).toBe(0)
      expect(result[1].tan_val).toBeCloseTo(1, 10)
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
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
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT COT(value) AS cot_val FROM data',
      }))
      expect(result[0].cot_val).toBeNull()
    })
  })

  describe('ASIN', () => {
    it('should return arcsine in radians', async () => {
      const result = await collect(executeSql({
        tables: { data: unitValues },
        query: 'SELECT ASIN(value) AS asin_val FROM data',
      }))
      expect(result[0].asin_val).toBe(0)
      expect(result[1].asin_val).toBe(Math.PI / 2)
      expect(result[2].asin_val).toBe(-Math.PI / 2)
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
      const result = await collect(executeSql({
        tables: { data: nullData },
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
      expect(result[0].acos_val).toBe(0)
      expect(result[1].acos_val).toBe(Math.PI / 2)
      expect(result[2].acos_val).toBe(Math.PI)
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
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT ACOS(value) AS acos_val FROM data',
      }))
      expect(result[0].acos_val).toBeNull()
    })
  })

  describe('ATAN', () => {
    it('should return arctangent in radians', async () => {
      const result = await collect(executeSql({
        tables: { data: unitValues },
        query: 'SELECT ATAN(value) AS atan_val FROM data',
      }))
      expect(result[0].atan_val).toBe(0)
      expect(result[1].atan_val).toBe(Math.PI / 4)
      expect(result[2].atan_val).toBe(-Math.PI / 4)
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
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
      expect(result[0].atan_val).toBe(0)
      expect(result[1].atan_val).toBe(Math.PI / 4)
      expect(result[2].atan_val).toBe(Math.PI / 2)
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
      expect(result[0].atan2_val).toBe(0)
      expect(result[1].atan2_val).toBe(Math.PI / 4)
      expect(result[2].atan2_val).toBe(Math.PI / 2)
      expect(result[3].atan2_val).toBe(-3 * Math.PI / 4)
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
      const result = await collect(executeSql({
        tables: { data: singleRow },
        query: 'SELECT ATAN2(1, 1) AS atan2_val FROM data',
      }))
      expect(result[0].atan2_val).toBe(Math.PI / 4)
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
      expect(result[0].degrees).toBe(0)
      expect(result[1].degrees).toBe(90)
      expect(result[2].degrees).toBe(180)
      expect(result[3].degrees).toBe(360)
    })

    it('should handle negative radians', async () => {
      const data = [{ id: 1, value: -Math.PI }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT DEGREES(value) AS degrees FROM data',
      }))
      expect(result[0].degrees).toBe(-180)
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
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
      expect(result[0].radians).toBe(0)
      expect(result[1].radians).toBe(Math.PI / 2)
      expect(result[2].radians).toBe(Math.PI)
      expect(result[3].radians).toBe(2 * Math.PI)
    })

    it('should handle negative degrees', async () => {
      const data = [{ id: 1, value: -180 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT RADIANS(value) AS radians FROM data',
      }))
      expect(result[0].radians).toBe(-Math.PI)
    })

    it('should handle null values', async () => {
      const result = await collect(executeSql({
        tables: { data: nullData },
        query: 'SELECT RADIANS(value) AS radians FROM data',
      }))
      expect(result[0].radians).toBeNull()
    })
  })

  describe('PI', () => {
    it('should return the value of PI', async () => {
      const result = await collect(executeSql({
        tables: { data: singleRow },
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
      expect(result[0].radians).toBe(Math.PI)
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
      expect(result[0].identity).toBe(1)
    })

    it('should work with inverse functions', async () => {
      const data = [{ id: 1, value: 0.5 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT SIN(ASIN(value)) AS original FROM data',
      }))
      expect(result[0].original).toBe(0.5)
    })
  })

  describe('error handling', () => {
    it('should throw for SIN with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT SIN(value, 2) FROM data',
      }))).rejects.toThrow('SIN(radians) function requires 1 argument, got 2')
    })

    it('should throw for ATAN2 with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT ATAN2(value) FROM data',
      }))).rejects.toThrow('ATAN2(y, x) function requires 2 arguments, got 1')
    })

    it('should throw for PI with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT PI(value) FROM data',
      }))).rejects.toThrow('PI() function requires no arguments, got 1')
    })

    it('should throw for ATAN with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT ATAN(value, 2, 3) FROM data',
      }))).rejects.toThrow('ATAN(number) function requires 1 or 2 arguments, got 3')
    })

    it('should throw for DEGREES with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT DEGREES(value, 2) FROM data',
      }))).rejects.toThrow('DEGREES(radians) function requires 1 argument, got 2')
    })

    it('should throw for RADIANS with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT RADIANS(value, 2) FROM data',
      }))).rejects.toThrow('RADIANS(degrees) function requires 1 argument, got 2')
    })

    it('should throw for COS with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT COS(value, 2) FROM data',
      }))).rejects.toThrow('COS(radians) function requires 1 argument, got 2')
    })

    it('should throw for TAN with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT TAN(value, 2) FROM data',
      }))).rejects.toThrow('TAN(radians) function requires 1 argument, got 2')
    })

    it('should throw for COT with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT COT(value, 2) FROM data',
      }))).rejects.toThrow('COT(radians) function requires 1 argument, got 2')
    })

    it('should throw for ASIN with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT ASIN(value, 2) FROM data',
      }))).rejects.toThrow('ASIN(number) function requires 1 argument, got 2')
    })

    it('should throw for ACOS with wrong argument count', async () => {
      await expect(collect(executeSql({
        tables: { data: errorData },
        query: 'SELECT ACOS(value, 2) FROM data',
      }))).rejects.toThrow('ACOS(number) function requires 1 argument, got 2')
    })
  })
})
