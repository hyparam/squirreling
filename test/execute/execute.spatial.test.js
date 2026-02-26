import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/** @type {null} */
const NULL = null

// Helper geometry factories
/**
 * @param {number} x
 * @param {number} y
 * @returns {{ type: string, coordinates: number[] }}
 */
function point(x, y) {
  return { type: 'Point', coordinates: [x, y] }
}

/**
 * @param {...number[]} coords
 * @returns {{ type: string, coordinates: number[][] }}
 */
function lineString(...coords) {
  return { type: 'LineString', coordinates: coords }
}

/**
 * @param {number[][][]} rings
 * @returns {{ type: string, coordinates: number[][][] }}
 */
function polygon(...rings) {
  return { type: 'Polygon', coordinates: rings }
}

// A simple square polygon from (0,0) to (10,10)
const square = polygon([[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]])

// A smaller square from (2,2) to (5,5)
const smallSquare = polygon([[2, 2], [5, 2], [5, 5], [2, 5], [2, 2]])

// A square partially overlapping: (5,5) to (15,15)
const offsetSquare = polygon([[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]])

// A square completely outside: (20,20) to (30,30)
const farSquare = polygon([[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]])

// A square sharing one edge: (10,0) to (20,10)
const adjacentSquare = polygon([[10, 0], [20, 0], [20, 10], [10, 10], [10, 0]])

describe('spatial predicates', () => {
  describe('ST_GeomFromText', () => {
    it('should parse POINT', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_GeomFromText(\'POINT (3 4)\') AS geom FROM data',
      }))
      expect(result[0].geom).toEqual({ type: 'Point', coordinates: [3, 4] })
    })

    it('should parse LINESTRING', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_GeomFromText(\'LINESTRING (0 0, 10 10, 20 0)\') AS geom FROM data',
      }))
      expect(result[0].geom).toEqual({ type: 'LineString', coordinates: [[0, 0], [10, 10], [20, 0]] })
    })

    it('should parse POLYGON', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_GeomFromText(\'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\') AS geom FROM data',
      }))
      expect(result[0].geom).toEqual({
        type: 'Polygon',
        coordinates: [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
      })
    })

    it('should parse POLYGON with hole', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_GeomFromText(\'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 3 2, 3 3, 2 3, 2 2))\') AS geom FROM data',
      }))
      expect(result[0].geom).toEqual({
        type: 'Polygon',
        coordinates: [
          [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
          [[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]],
        ],
      })
    })

    it('should parse MULTIPOINT', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_GeomFromText(\'MULTIPOINT ((1 2), (3 4))\') AS geom FROM data',
      }))
      expect(result[0].geom).toEqual({ type: 'MultiPoint', coordinates: [[1, 2], [3, 4]] })
    })

    it('should parse MULTILINESTRING', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_GeomFromText(\'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))\') AS geom FROM data',
      }))
      expect(result[0].geom).toEqual({
        type: 'MultiLineString',
        coordinates: [[[0, 0], [1, 1]], [[2, 2], [3, 3]]],
      })
    })

    it('should parse MULTIPOLYGON', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_GeomFromText(\'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))\') AS geom FROM data',
      }))
      expect(result[0].geom).toEqual({
        type: 'MultiPolygon',
        coordinates: [
          [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
          [[[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]]],
        ],
      })
    })

    it('should return null for null input', async () => {
      const data = [{ id: 1, wkt: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_GeomFromText(wkt) AS geom FROM data',
      }))
      expect(result[0].geom).toBeNull()
    })

    it('should be usable with predicates', async () => {
      const data = [{ id: 1, geom: point(5, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Contains(ST_GeomFromText(\'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\'), geom) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should be case insensitive', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT st_geomfromtext(\'POINT (1 2)\') AS geom FROM data',
      }))
      expect(result[0].geom).toEqual({ type: 'Point', coordinates: [1, 2] })
    })

    it('should throw with wrong argument count', async () => {
      const data = [{ id: 1 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ST_GeomFromText() AS geom FROM data',
      }))).rejects.toThrow()
    })
  })

  describe('ST_MakeEnvelope', () => {
    it('should create a rectangle polygon', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_MakeEnvelope(0, 0, 10, 10) AS geom FROM data',
      }))
      expect(result[0].geom).toEqual({
        type: 'Polygon',
        coordinates: [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
      })
    })

    it('should return null when any arg is null', async () => {
      const data = [{ id: 1, v: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_MakeEnvelope(0, 0, v, 10) AS geom FROM data',
      }))
      expect(result[0].geom).toBeNull()
    })

    it('should work with predicates', async () => {
      const data = [{ id: 1, geom: point(5, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Contains(ST_MakeEnvelope(0, 0, 10, 10), geom) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should throw with wrong argument count', async () => {
      const data = [{ id: 1 }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ST_MakeEnvelope(0, 0, 10) AS geom FROM data',
      }))).rejects.toThrow()
    })
  })

  describe('ST_AsText', () => {
    it('should convert point to WKT', async () => {
      const data = [{ id: 1, geom: point(3, 4) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_AsText(geom) AS wkt FROM data',
      }))
      expect(result[0].wkt).toBe('POINT (3 4)')
    })

    it('should convert linestring to WKT', async () => {
      const data = [{ id: 1, geom: lineString([0, 0], [10, 10]) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_AsText(geom) AS wkt FROM data',
      }))
      expect(result[0].wkt).toBe('LINESTRING (0 0, 10 10)')
    })

    it('should convert polygon to WKT', async () => {
      const data = [{ id: 1, geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_AsText(geom) AS wkt FROM data',
      }))
      expect(result[0].wkt).toBe('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')
    })

    it('should round-trip with ST_GeomFromText', async () => {
      const data = [{ id: 1 }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_AsText(ST_GeomFromText(\'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\')) AS wkt FROM data',
      }))
      expect(result[0].wkt).toBe('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')
    })

    it('should return null for null input', async () => {
      const data = [{ id: 1, geom: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_AsText(geom) AS wkt FROM data',
      }))
      expect(result[0].wkt).toBeNull()
    })

    it('should throw with wrong argument count', async () => {
      const data = [{ id: 1, a: point(1, 2), b: point(3, 4) }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ST_AsText(a, b) AS wkt FROM data',
      }))).rejects.toThrow()
    })
  })

  describe('ST_Intersects', () => {
    it('should return true for overlapping polygons', async () => {
      const data = [{ id: 1, a: square, b: offsetSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Intersects(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false for disjoint polygons', async () => {
      const data = [{ id: 1, a: square, b: farSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Intersects(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true for touching polygons', async () => {
      const data = [{ id: 1, a: square, b: adjacentSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Intersects(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return true for point inside polygon', async () => {
      const data = [{ id: 1, geom: square, pt: point(5, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Intersects(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false for point outside polygon', async () => {
      const data = [{ id: 1, geom: square, pt: point(50, 50) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Intersects(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true for two equal points', async () => {
      const data = [{ id: 1, a: point(3, 4), b: point(3, 4) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Intersects(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false for two different points', async () => {
      const data = [{ id: 1, a: point(3, 4), b: point(6, 7) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Intersects(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true for intersecting lines', async () => {
      const data = [{ id: 1, a: lineString([0, 0], [10, 10]), b: lineString([0, 10], [10, 0]) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Intersects(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, geom: square, other: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Intersects(geom, other) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })

    it('should be case insensitive', async () => {
      const data = [{ id: 1, a: point(3, 4), b: point(3, 4) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT st_intersects(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })
  })

  describe('ST_Contains', () => {
    it('should return true when polygon contains point', async () => {
      const data = [{ id: 1, geom: square, pt: point(5, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Contains(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false when polygon does not contain point', async () => {
      const data = [{ id: 1, geom: square, pt: point(50, 50) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Contains(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true when polygon contains smaller polygon', async () => {
      const data = [{ id: 1, a: square, b: smallSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Contains(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false when polygon does not fully contain other', async () => {
      const data = [{ id: 1, a: square, b: offsetSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Contains(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true when polygon contains linestring', async () => {
      const data = [{ id: 1, geom: square, line: lineString([2, 2], [8, 8]) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Contains(geom, line) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, geom: NULL, pt: point(5, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Contains(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })
  })

  describe('ST_ContainsProperly', () => {
    it('should return true when point is strictly inside polygon', async () => {
      const data = [{ id: 1, geom: square, pt: point(5, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_ContainsProperly(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false when point is on polygon boundary', async () => {
      const data = [{ id: 1, geom: square, pt: point(0, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_ContainsProperly(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return false when point is at polygon vertex', async () => {
      const data = [{ id: 1, geom: square, pt: point(0, 0) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_ContainsProperly(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, geom: square, pt: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_ContainsProperly(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })
  })

  describe('ST_Within', () => {
    it('should return true when point is within polygon', async () => {
      const data = [{ id: 1, pt: point(5, 5), geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Within(pt, geom) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false when point is not within polygon', async () => {
      const data = [{ id: 1, pt: point(50, 50), geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Within(pt, geom) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true when smaller polygon is within larger', async () => {
      const data = [{ id: 1, a: smallSquare, b: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Within(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, pt: point(5, 5), geom: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Within(pt, geom) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })
  })

  describe('ST_Overlaps', () => {
    it('should return true for partially overlapping polygons', async () => {
      const data = [{ id: 1, a: square, b: offsetSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Overlaps(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false for disjoint polygons', async () => {
      const data = [{ id: 1, a: square, b: farSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Overlaps(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return false when one contains the other', async () => {
      const data = [{ id: 1, a: square, b: smallSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Overlaps(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return false for different dimension types', async () => {
      const data = [{ id: 1, a: square, b: point(5, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Overlaps(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, a: NULL, b: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Overlaps(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })
  })

  describe('ST_Touches', () => {
    it('should return true for adjacent polygons sharing an edge', async () => {
      const data = [{ id: 1, a: square, b: adjacentSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Touches(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false for overlapping polygons', async () => {
      const data = [{ id: 1, a: square, b: offsetSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Touches(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return false for disjoint polygons', async () => {
      const data = [{ id: 1, a: square, b: farSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Touches(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true for point on polygon boundary', async () => {
      const data = [{ id: 1, pt: point(0, 5), geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Touches(pt, geom) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false for point inside polygon', async () => {
      const data = [{ id: 1, pt: point(5, 5), geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Touches(pt, geom) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true for point at line endpoint', async () => {
      const data = [{ id: 1, pt: point(0, 0), line: lineString([0, 0], [10, 10]) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Touches(pt, line) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, a: NULL, b: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Touches(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })
  })

  describe('ST_Equals', () => {
    it('should return true for identical polygons', async () => {
      const data = [{ id: 1, a: square, b: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Equals(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false for different polygons', async () => {
      const data = [{ id: 1, a: square, b: smallSquare }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Equals(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true for identical points', async () => {
      const data = [{ id: 1, a: point(3, 4), b: point(3, 4) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Equals(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false for different points', async () => {
      const data = [{ id: 1, a: point(3, 4), b: point(5, 6) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Equals(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, a: square, b: NULL }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Equals(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })
  })

  describe('ST_Crosses', () => {
    it('should return true for line crossing polygon boundary', async () => {
      const data = [{ id: 1, line: lineString([-5, 5], [5, 5]), geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Crosses(line, geom) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false for line entirely inside polygon', async () => {
      const data = [{ id: 1, line: lineString([2, 2], [8, 8]), geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Crosses(line, geom) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true for crossing lines', async () => {
      const data = [{ id: 1, a: lineString([0, 0], [10, 10]), b: lineString([0, 10], [10, 0]) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Crosses(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, a: NULL, b: lineString([0, 0], [10, 10]) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Crosses(a, b) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })
  })

  describe('ST_Covers', () => {
    it('should return true when polygon covers point', async () => {
      const data = [{ id: 1, geom: square, pt: point(5, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Covers(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return true when polygon covers boundary point', async () => {
      const data = [{ id: 1, geom: square, pt: point(0, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Covers(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false when polygon does not cover point', async () => {
      const data = [{ id: 1, geom: square, pt: point(50, 50) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Covers(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, geom: NULL, pt: point(5, 5) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Covers(geom, pt) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })
  })

  describe('ST_CoveredBy', () => {
    it('should return true when point is covered by polygon', async () => {
      const data = [{ id: 1, pt: point(5, 5), geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_CoveredBy(pt, geom) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false when point is not covered by polygon', async () => {
      const data = [{ id: 1, pt: point(50, 50), geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_CoveredBy(pt, geom) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, pt: NULL, geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_CoveredBy(pt, geom) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })
  })

  describe('ST_DWithin', () => {
    it('should return true when points are within distance', async () => {
      const data = [{ id: 1, a: point(0, 0), b: point(3, 4) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_DWithin(a, b, 5) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return true when distance equals threshold', async () => {
      const data = [{ id: 1, a: point(0, 0), b: point(3, 4) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_DWithin(a, b, 5) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false when points are beyond distance', async () => {
      const data = [{ id: 1, a: point(0, 0), b: point(3, 4) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_DWithin(a, b, 4) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return true when point is within distance of polygon', async () => {
      const data = [{ id: 1, pt: point(12, 5), geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_DWithin(pt, geom, 3) AS result FROM data',
      }))
      expect(result[0].result).toBe(true)
    })

    it('should return false when point is far from polygon', async () => {
      const data = [{ id: 1, pt: point(50, 50), geom: square }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_DWithin(pt, geom, 3) AS result FROM data',
      }))
      expect(result[0].result).toBe(false)
    })

    it('should return null when input is null', async () => {
      const data = [{ id: 1, a: NULL, b: point(3, 4) }]
      const result = await collect(executeSql({
        tables: { data },
        query: 'SELECT ST_DWithin(a, b, 5) AS result FROM data',
      }))
      expect(result[0].result).toBeNull()
    })
  })

  describe('wrong argument count', () => {
    it('should throw for ST_Intersects with wrong args', async () => {
      const data = [{ id: 1, geom: square }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ST_Intersects(geom) AS result FROM data',
      }))).rejects.toThrow()
    })

    it('should throw for ST_DWithin with wrong args', async () => {
      const data = [{ id: 1, a: point(0, 0), b: point(1, 1) }]
      await expect(collect(executeSql({
        tables: { data },
        query: 'SELECT ST_DWithin(a, b) AS result FROM data',
      }))).rejects.toThrow()
    })
  })

  describe('use in WHERE clause', () => {
    it('should filter rows using ST_Contains', async () => {
      const locations = [
        { id: 1, name: 'inside', geom: point(5, 5), region: square },
        { id: 2, name: 'outside', geom: point(50, 50), region: square },
        { id: 3, name: 'also inside', geom: point(3, 3), region: square },
      ]
      const result = await collect(executeSql({
        tables: { locations },
        query: 'SELECT name FROM locations WHERE ST_Contains(region, geom)',
      }))
      expect(result).toEqual([
        { name: 'inside' },
        { name: 'also inside' },
      ])
    })
  })
})
