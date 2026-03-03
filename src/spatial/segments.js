import { EPSILON } from './primitives.js'

/**
 * Test whether two line segments [p1,p2] and [p3,p4] intersect.
 * Returns true if they share any point (including endpoints).
 *
 * @param {number[]} p1
 * @param {number[]} p2
 * @param {number[]} p3
 * @param {number[]} p4
 * @returns {boolean}
 */
export function segmentsIntersect(p1, p2, p3, p4) {
  const d1 = cross(p3, p4, p1)
  const d2 = cross(p3, p4, p2)
  const d3 = cross(p1, p2, p3)
  const d4 = cross(p1, p2, p4)

  if ((d1 > 0 && d2 < 0 || d1 < 0 && d2 > 0) &&
      (d3 > 0 && d4 < 0 || d3 < 0 && d4 > 0)) {
    return true
  }

  if (Math.abs(d1) < EPSILON && onSegment(p3, p4, p1)) return true
  if (Math.abs(d2) < EPSILON && onSegment(p3, p4, p2)) return true
  if (Math.abs(d3) < EPSILON && onSegment(p1, p2, p3)) return true
  if (Math.abs(d4) < EPSILON && onSegment(p1, p2, p4)) return true

  return false
}

/**
 * Test if a line segment intersects a ring boundary.
 *
 * @param {number[]} a
 * @param {number[]} b
 * @param {number[][]} ring
 * @returns {boolean}
 */
export function segmentIntersectsRing(a, b, ring) {
  for (let i = 0; i < ring.length - 1; i++) {
    if (segmentsIntersect(a, b, ring[i], ring[i + 1])) return true
  }
  return false
}

/**
 * Compute intersection point of two segments (if they intersect at a single point).
 *
 * @param {number[]} p1
 * @param {number[]} p2
 * @param {number[]} p3
 * @param {number[]} p4
 * @returns {number[] | null}
 */
export function segmentIntersectionPoint(p1, p2, p3, p4) {
  const d1x = p2[0] - p1[0], d1y = p2[1] - p1[1]
  const d2x = p4[0] - p3[0], d2y = p4[1] - p3[1]
  const denom = d1x * d2y - d1y * d2x
  if (Math.abs(denom) < EPSILON) return null // parallel
  const t = ((p3[0] - p1[0]) * d2y - (p3[1] - p1[1]) * d2x) / denom
  return [p1[0] + t * d1x, p1[1] + t * d1y]
}

/**
 * Cross product of vectors (b-a) and (c-a).
 *
 * @param {number[]} a
 * @param {number[]} b
 * @param {number[]} c
 * @returns {number}
 */
function cross(a, b, c) {
  return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
}

/**
 * Check if point c lies on segment [a, b], assuming collinearity.
 *
 * @param {number[]} a
 * @param {number[]} b
 * @param {number[]} c
 * @returns {boolean}
 */
function onSegment(a, b, c) {
  return Math.min(a[0], b[0]) - c[0] <= EPSILON && c[0] - Math.max(a[0], b[0]) <= EPSILON &&
         Math.min(a[1], b[1]) - c[1] <= EPSILON && c[1] - Math.max(a[1], b[1]) <= EPSILON
}
