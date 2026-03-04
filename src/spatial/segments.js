import { EPSILON, EPSILON_SQ, cross, distSq } from './primitives.js'

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
 * Check if point p lies on segment [a, b].
 *
 * @param {number[]} a
 * @param {number[]} b
 * @param {number[]} p
 * @returns {boolean}
 */
export function pointOnSegment(a, b, p) {
  if (Math.abs(cross(a, b, p)) > EPSILON) return false
  return p[0] >= Math.min(a[0], b[0]) - EPSILON &&
    p[0] <= Math.max(a[0], b[0]) + EPSILON &&
    p[1] >= Math.min(a[1], b[1]) - EPSILON &&
    p[1] <= Math.max(a[1], b[1]) + EPSILON
}

/**
 * Returns the single endpoint touch point for two segments, 'INSIDE' when
 * they intersect at a non-endpoint/proper crossing or overlap by length,
 * and 'OUTSIDE' when they do not intersect.
 *
 * @param {number[]} a1
 * @param {number[]} a2
 * @param {number[]} b1
 * @param {number[]} b2
 * @returns {'INSIDE' | 'OUTSIDE' | number[]}
 */
export function segmentTouchPoint(a1, a2, b1, b2) {
  const d1 = cross(b1, b2, a1)
  const d2 = cross(b1, b2, a2)
  const d3 = cross(a1, a2, b1)
  const d4 = cross(a1, a2, b2)

  if ((d1 > 0 && d2 < 0 || d1 < 0 && d2 > 0) &&
      (d3 > 0 && d4 < 0 || d3 < 0 && d4 > 0)) {
    return 'INSIDE'
  }

  /** @type {number[] | undefined} */
  let point
  let hasSecondPoint = false

  /**
   * @param {number[]} candidate
   */
  function addPoint(candidate) {
    if (!point) {
      point = candidate
      return
    }
    if (distSq(point, candidate) >= EPSILON_SQ) hasSecondPoint = true
  }

  if (Math.abs(d1) < EPSILON && onSegment(b1, b2, a1)) addPoint(a1)
  if (Math.abs(d2) < EPSILON && onSegment(b1, b2, a2)) addPoint(a2)
  if (Math.abs(d3) < EPSILON && onSegment(a1, a2, b1)) addPoint(b1)
  if (Math.abs(d4) < EPSILON && onSegment(a1, a2, b2)) addPoint(b2)

  if (!point) return 'OUTSIDE'

  return hasSecondPoint ? 'INSIDE' : point
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
