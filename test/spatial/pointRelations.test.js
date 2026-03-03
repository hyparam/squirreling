import { describe, expect, it } from 'vitest'
import { pointInPolygon, pointLineRelation, pointOnLine, pointToSegmentDistSq } from '../../src/spatial/pointRelations.js'

const square = [
  [0, 0],
  [10, 0],
  [10, 10],
  [0, 10],
  [0, 0],
]

const squareWithHole = [
  square,
  [
    [3, 3],
    [7, 3],
    [7, 7],
    [3, 7],
    [3, 3],
  ],
]

describe('pointInPolygon', () => {
  it('returns INSIDE for points in polygon interior', () => {
    expect(pointInPolygon([2, 2], [square])).toBe('INSIDE')
  })

  it('returns OUTSIDE for points outside polygon', () => {
    expect(pointInPolygon([12, 5], [square])).toBe('OUTSIDE')
  })

  it('returns BOUNDARY for points on exterior ring', () => {
    expect(pointInPolygon([0, 5], [square])).toBe('BOUNDARY')
  })

  it('returns OUTSIDE for points inside a hole', () => {
    expect(pointInPolygon([5, 5], squareWithHole)).toBe('OUTSIDE')
  })

  it('returns BOUNDARY for points on hole boundary', () => {
    expect(pointInPolygon([3, 5], squareWithHole)).toBe('BOUNDARY')
  })
})

describe('pointOnLine', () => {
  it('returns true when point lies on a segment', () => {
    expect(pointOnLine([5, 0], [[0, 0], [10, 0]])).toBe(true)
  })

  it('returns false when point is off the line', () => {
    expect(pointOnLine([5, 1], [[0, 0], [10, 0]])).toBe(false)
  })
})

describe('pointLineRelation', () => {
  it('returns BOUNDARY for line endpoints', () => {
    expect(pointLineRelation([0, 0], [[0, 0], [10, 0], [10, 10]])).toBe('BOUNDARY')
    expect(pointLineRelation([10, 10], [[0, 0], [10, 0], [10, 10]])).toBe('BOUNDARY')
  })

  it('returns INSIDE for interior line points', () => {
    expect(pointLineRelation([5, 0], [[0, 0], [10, 0], [10, 10]])).toBe('INSIDE')
  })

  it('returns OUTSIDE for points not on any segment', () => {
    expect(pointLineRelation([5, 1], [[0, 0], [10, 0], [10, 10]])).toBe('OUTSIDE')
  })
})

describe('pointToSegmentDistSq', () => {
  it('returns point distance when segment is degenerate', () => {
    expect(pointToSegmentDistSq([5, 8], [2, 4], [2, 4])).toBe(25)
  })

  it('clamps to segment start when projection is before start', () => {
    expect(pointToSegmentDistSq([-1, 2], [0, 0], [4, 0])).toBe(5)
  })

  it('clamps to segment end when projection is past end', () => {
    expect(pointToSegmentDistSq([7, 1], [0, 0], [4, 0])).toBe(10)
  })

  it('uses perpendicular distance when projection falls on segment', () => {
    expect(pointToSegmentDistSq([2, 3], [0, 0], [4, 0])).toBe(9)
  })
})
