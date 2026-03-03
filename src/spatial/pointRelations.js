/**
 * @import { Relation } from './geometry.js'
 */

import { EPSILON_SQ, distSq } from './primitives.js'

/**
 * Classify a point relative to a ring: 'OUTSIDE', 'BOUNDARY', or 'INSIDE'.
 * Combines ray casting with boundary distance check in a single pass.
 * ring is an array of [x, y] coords (closed ring, first = last).
 *
 * @param {number[]} point
 * @param {number[][]} ring
 * @returns {Relation}
 */
function pointInRing(point, ring) {
  const [px, py] = point
  let inside = false
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    if (pointToSegmentDistSq(point, ring[j], ring[i]) < EPSILON_SQ) {
      return 'BOUNDARY'
    }
    const [xi, yi] = ring[i]
    const [xj, yj] = ring[j]
    if (yi > py !== yj > py && px < (xj - xi) * (py - yi) / (yj - yi) + xi) {
      inside = !inside
    }
  }
  return inside ? 'INSIDE' : 'OUTSIDE'
}

/**
 * Classify a point relative to a polygon: 'OUTSIDE', 'BOUNDARY', or 'INSIDE'.
 * First ring is exterior, rest are holes.
 *
 * @param {number[]} point
 * @param {number[][][]} rings
 * @returns {Relation}
 */
export function pointInPolygon(point, rings) {
  const rel = pointInRing(point, rings[0])
  if (rel === 'OUTSIDE') return 'OUTSIDE'
  if (rel === 'BOUNDARY') return 'BOUNDARY'
  for (let i = 1; i < rings.length; i++) {
    const holeRel = pointInRing(point, rings[i])
    if (holeRel === 'INSIDE') return 'OUTSIDE'
    if (holeRel === 'BOUNDARY') return 'BOUNDARY'
  }
  return 'INSIDE'
}

/**
 * Test if point is on a linestring.
 *
 * @param {number[]} point
 * @param {number[][]} line
 * @returns {boolean}
 */
export function pointOnLine(point, line) {
  for (let i = 0; i < line.length - 1; i++) {
    if (pointToSegmentDistSq(point, line[i], line[i + 1]) < EPSILON_SQ) return true
  }
  return false
}

/**
 * Classify a point relative to a linestring.
 *
 * @param {number[]} point
 * @param {number[][]} line
 * @returns {Relation}
 */
export function pointLineRelation(point, line) {
  // Check endpoints first
  if (distSq(point, line[0]) < EPSILON_SQ) return 'BOUNDARY'
  if (distSq(point, line[line.length - 1]) < EPSILON_SQ) return 'BOUNDARY'
  // Check if on any segment
  for (let i = 0; i < line.length - 1; i++) {
    if (pointToSegmentDistSq(point, line[i], line[i + 1]) < EPSILON_SQ) return 'INSIDE'
  }
  return 'OUTSIDE'
}

/**
 * Squared minimum distance from point p to line segment [a, b].
 *
 * @param {number[]} p
 * @param {number[]} a
 * @param {number[]} b
 * @returns {number}
 */
export function pointToSegmentDistSq(p, a, b) {
  const dx = b[0] - a[0]
  const dy = b[1] - a[1]
  const lenSq = dx * dx + dy * dy
  if (lenSq === 0) return distSq(p, a)
  let t = ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / lenSq
  t = Math.max(0, Math.min(1, t))
  const ddx = p[0] - a[0] - t * dx
  const ddy = p[1] - a[1] - t * dy
  return ddx * ddx + ddy * ddy
}
