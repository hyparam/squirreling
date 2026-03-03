import { describe, expect, it } from 'vitest'
import { segmentIntersectionPoint, segmentIntersectsRing, segmentsIntersect } from '../../src/spatial/segments.js'

describe('segmentsIntersect', () => {
  it('returns true for crossing segments', () => {
    expect(segmentsIntersect([0, 0], [2, 2], [0, 2], [2, 0])).toBe(true)
  })

  it('returns true for collinear overlapping segments', () => {
    expect(segmentsIntersect([0, 0], [2, 0], [1, 0], [3, 0])).toBe(true)
  })

  it('returns false for disjoint segments', () => {
    expect(segmentsIntersect([0, 0], [1, 0], [0, 1], [1, 1])).toBe(false)
  })
})

describe('segmentIntersectsRing', () => {
  const squareRing = [
    [0, 0],
    [10, 0],
    [10, 10],
    [0, 10],
    [0, 0],
  ]

  it('returns true when segment crosses ring boundary', () => {
    expect(segmentIntersectsRing([-1, 5], [1, 5], squareRing)).toBe(true)
  })

  it('returns false when segment stays fully inside ring', () => {
    expect(segmentIntersectsRing([2, 2], [3, 3], squareRing)).toBe(false)
  })
})

describe('segmentIntersectionPoint', () => {
  it('returns the intersection point for non-parallel segments', () => {
    expect(segmentIntersectionPoint([0, 0], [2, 2], [0, 2], [2, 0])).toEqual([1, 1])
  })

  it('returns null for parallel segments', () => {
    expect(segmentIntersectionPoint([0, 0], [1, 0], [0, 1], [1, 1])).toBeNull()
  })
})
