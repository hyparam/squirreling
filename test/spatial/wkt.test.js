import { describe, expect, it } from 'vitest'
import { geomToWkt, parseWkt } from '../../src/spatial/wkt.js'

describe('parseWkt', () => {
  it('should parse POINT', () => {
    expect(parseWkt('POINT (3 4)')).toEqual({ type: 'Point', coordinates: [3, 4] })
  })

  it('should parse POINT without space before parens', () => {
    expect(parseWkt('POINT(3 4)')).toEqual({ type: 'Point', coordinates: [3, 4] })
  })

  it('should be case insensitive', () => {
    expect(parseWkt('point (1 2)')).toEqual({ type: 'Point', coordinates: [1, 2] })
  })

  it('should trim whitespace', () => {
    expect(parseWkt('  POINT (5 6)  ')).toEqual({ type: 'Point', coordinates: [5, 6] })
  })

  it('should parse MULTIPOINT', () => {
    expect(parseWkt('MULTIPOINT ((1 2), (3 4))')).toEqual({
      type: 'MultiPoint', coordinates: [[1, 2], [3, 4]],
    })
  })

  it('should parse LINESTRING', () => {
    expect(parseWkt('LINESTRING (0 0, 10 10, 20 0)')).toEqual({
      type: 'LineString', coordinates: [[0, 0], [10, 10], [20, 0]],
    })
  })

  it('should parse MULTILINESTRING', () => {
    expect(parseWkt('MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))')).toEqual({
      type: 'MultiLineString', coordinates: [[[0, 0], [1, 1]], [[2, 2], [3, 3]]],
    })
  })

  it('should parse POLYGON', () => {
    expect(parseWkt('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')).toEqual({
      type: 'Polygon', coordinates: [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
    })
  })

  it('should parse POLYGON with hole', () => {
    expect(parseWkt('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 3 2, 3 3, 2 3, 2 2))')).toEqual({
      type: 'Polygon',
      coordinates: [
        [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
        [[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]],
      ],
    })
  })

  it('should parse MULTIPOLYGON', () => {
    expect(parseWkt('MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))')).toEqual({
      type: 'MultiPolygon',
      coordinates: [
        [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        [[[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]]],
      ],
    })
  })

  it('should return null for unknown type', () => {
    expect(parseWkt('CIRCLE (1 2 3)')).toBeNull()
  })

  it('should return null for invalid POINT', () => {
    expect(parseWkt('POINT (abc)')).toBeNull()
  })

  it('should return null for LINESTRING with bad parens', () => {
    expect(parseWkt('LINESTRING 0 0, 1 1')).toBeNull()
  })

  it('should return null for MULTIPOINT with bad parens', () => {
    expect(parseWkt('MULTIPOINT 1 2')).toBeNull()
  })

  it('should return null for POLYGON with bad inner', () => {
    expect(parseWkt('POLYGON (0 0, 1 1)')).toBeNull()
  })

  it('should return null for MULTILINESTRING with bad inner', () => {
    expect(parseWkt('MULTILINESTRING (0 0, 1 1)')).toBeNull()
  })

  it('should return null for MULTIPOLYGON with bad inner', () => {
    expect(parseWkt('MULTIPOLYGON ((0 0, 1 1))')).toBeNull()
  })

  it('should return null for empty coordinate list', () => {
    expect(parseWkt('LINESTRING ()')).toBeNull()
  })

  it('should return null for non-finite coordinates', () => {
    expect(parseWkt('POINT (Infinity 0)')).toBeNull()
  })

  it('should return null for single number coordinate', () => {
    expect(parseWkt('POINT (42)')).toBeNull()
  })

  it('should return null for bad coordinate in list', () => {
    expect(parseWkt('LINESTRING (0 0, bad 1)')).toBeNull()
  })

  it('should return null for empty ring list', () => {
    expect(parseWkt('POLYGON (())')).toBeNull()
  })

  it('should return null for bad ring in polygon', () => {
    expect(parseWkt('POLYGON ((0 0, 1 0, 0 0), bad)')).toBeNull()
  })

  it('should return null for bad polygon in multipolygon', () => {
    expect(parseWkt('MULTIPOLYGON (((0 0, 1 0, 0 0)), (bad))')).toBeNull()
  })

  it('should handle GeometryCollection with empty geometries', () => {
    expect(geomToWkt({ type: 'GeometryCollection', geometries: [] })).toBe('GEOMETRYCOLLECTION ()')
  })
})

describe('geomToWkt', () => {
  it('should convert Point', () => {
    expect(geomToWkt({ type: 'Point', coordinates: [3, 4] })).toBe('POINT (3 4)')
  })

  it('should convert MultiPoint', () => {
    expect(geomToWkt({ type: 'MultiPoint', coordinates: [[1, 2], [3, 4]] }))
      .toBe('MULTIPOINT ((1 2), (3 4))')
  })

  it('should convert LineString', () => {
    expect(geomToWkt({ type: 'LineString', coordinates: [[0, 0], [10, 10]] }))
      .toBe('LINESTRING (0 0, 10 10)')
  })

  it('should convert MultiLineString', () => {
    expect(geomToWkt({ type: 'MultiLineString', coordinates: [[[0, 0], [1, 1]], [[2, 2], [3, 3]]] }))
      .toBe('MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))')
  })

  it('should convert Polygon', () => {
    expect(geomToWkt({ type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] }))
      .toBe('POLYGON ((0 0, 1 0, 1 1, 0 0))')
  })

  it('should convert MultiPolygon', () => {
    expect(geomToWkt({
      type: 'MultiPolygon',
      coordinates: [[[[0, 0], [1, 0], [1, 1], [0, 0]]], [[[2, 2], [3, 2], [3, 3], [2, 2]]]],
    })).toBe('MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))')
  })

  it('should convert GeometryCollection', () => {
    expect(geomToWkt({
      type: 'GeometryCollection',
      geometries: [
        { type: 'Point', coordinates: [1, 2] },
        { type: 'Point', coordinates: [3, 4] },
      ],
    })).toBe('GEOMETRYCOLLECTION (POINT (1 2), POINT (3 4))')
  })

  it('should round-trip all types', () => {
    const wkts = [
      'POINT (3 4)',
      'MULTIPOINT ((1 2), (3 4))',
      'LINESTRING (0 0, 10 10, 20 0)',
      'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))',
      'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))',
      'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))',
    ]
    for (const wkt of wkts) {
      expect(geomToWkt(parseWkt(wkt))).toBe(wkt)
    }
  })
})
