import { describe, expect, it } from 'vitest'
import { geometryEqual } from '../../src/spatial/equality.js'

describe('geometryEqual', () => {
  // Points
  it('should compare equal points', () => {
    expect(geometryEqual(
      { type: 'Point', coordinates: [1, 2] },
      { type: 'Point', coordinates: [1, 2] }
    )).toBe(true)
  })

  it('should compare different points', () => {
    expect(geometryEqual(
      { type: 'Point', coordinates: [1, 2] },
      { type: 'Point', coordinates: [3, 4] }
    )).toBe(false)
  })

  it('should return false for mismatched types', () => {
    expect(geometryEqual(
      { type: 'Point', coordinates: [0, 0] },
      { type: 'LineString', coordinates: [[0, 0]] }
    )).toBe(false)
  })

  // LineStrings
  it('should match equal linestrings', () => {
    expect(geometryEqual(
      { type: 'LineString', coordinates: [[0, 0], [1, 1]] },
      { type: 'LineString', coordinates: [[0, 0], [1, 1]] }
    )).toBe(true)
  })

  it('should match reversed linestring', () => {
    expect(geometryEqual(
      { type: 'LineString', coordinates: [[0, 0], [1, 1]] },
      { type: 'LineString', coordinates: [[1, 1], [0, 0]] }
    )).toBe(true)
  })

  it('should reject linestrings with different lengths', () => {
    expect(geometryEqual(
      { type: 'LineString', coordinates: [[0, 0]] },
      { type: 'LineString', coordinates: [[0, 0], [1, 1]] }
    )).toBe(false)
  })

  it('should reject linestrings with different coordinates', () => {
    expect(geometryEqual(
      { type: 'LineString', coordinates: [[0, 0], [1, 1]] },
      { type: 'LineString', coordinates: [[0, 0], [2, 2]] }
    )).toBe(false)
  })

  // Polygons
  it('should match equal polygons', () => {
    expect(geometryEqual(
      { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] },
      { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] }
    )).toBe(true)
  })

  it('should match polygon ring with different start', () => {
    expect(geometryEqual(
      { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] },
      { type: 'Polygon', coordinates: [[[1, 0], [1, 1], [0, 0], [1, 0]]] }
    )).toBe(true)
  })

  it('should match polygon ring reversed', () => {
    expect(geometryEqual(
      { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] },
      { type: 'Polygon', coordinates: [[[0, 0], [1, 1], [1, 0], [0, 0]]] }
    )).toBe(true)
  })

  it('should reject polygons with different rings', () => {
    expect(geometryEqual(
      { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] },
      { type: 'Polygon', coordinates: [[[0, 0], [2, 0], [2, 2], [0, 0]]] }
    )).toBe(false)
  })

  it('should reject polygons with different ring counts', () => {
    expect(geometryEqual(
      { type: 'Polygon', coordinates: [[[0, 0], [4, 0], [4, 4], [0, 0]]] },
      { type: 'Polygon', coordinates: [[[0, 0], [4, 0], [4, 4], [0, 0]], [[1, 1], [2, 1], [2, 2], [1, 1]]] }
    )).toBe(false)
  })

  it('should reject polygons with different ring lengths', () => {
    expect(geometryEqual(
      { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [0, 0]]] },
      { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] }
    )).toBe(false)
  })
})
