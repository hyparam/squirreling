import { bboxOverlap } from './bbox.js'
import { pointInPolygon, pointLineRelation, pointOnLine } from './pointRelations.js'
import { EPSILON_SQ, distSq } from './primitives.js'
import { segmentIntersectsRing, segmentTouchPoint, segmentsIntersect } from './segments.js'

/**
 * @import { Relation, SimpleGeometry } from './geometry.js'
 */

/**
 * Test if a linestring intersects a polygon.
 *
 * @param {number[][]} line
 * @param {number[][][]} rings
 * @returns {boolean}
 */
function lineIntersectsPolygon(line, rings) {
  // Fast path for common containment queries: if one point is inside or on
  // boundary, the line intersects.
  if (pointInPolygon(line[0], rings) !== 'OUTSIDE') return true

  // Otherwise, detect crossings/touches against polygon boundaries.
  for (let i = 0; i < line.length - 1; i++) {
    for (const ring of rings) {
      if (segmentIntersectsRing(line[i], line[i + 1], ring)) return true
    }
  }
  return false
}

/**
 * Test if two linestrings share any point (intersect).
 *
 * @param {number[][]} line1
 * @param {number[][]} line2
 * @returns {boolean}
 */
function linesIntersect(line1, line2) {
  for (let i = 0; i < line1.length - 1; i++) {
    for (let j = 0; j < line2.length - 1; j++) {
      if (segmentsIntersect(line1[i], line1[i + 1], line2[j], line2[j + 1])) return true
    }
  }
  return false
}

/**
 * @param {number[]} point
 * @param {number[][]} line
 * @returns {boolean}
 */
function isLineEndpoint(point, line) {
  return distSq(point, line[0]) < EPSILON_SQ || distSq(point, line[line.length - 1]) < EPSILON_SQ
}

/**
 * Classify containment of a linestring within a polygon.
 * Returns 'INSIDE' if entirely in interior, 'BOUNDARY' if inside but
 * touches boundary, 'OUTSIDE' if any part is outside.
 *
 * @param {number[][]} line
 * @param {number[][][]} rings
 * @returns {Relation}
 */
function polygonContainsLine(line, rings) {
  /** @type {Relation} */
  let result = 'INSIDE'
  for (const pt of line) {
    const rel = pointInPolygon(pt, rings)
    if (rel === 'OUTSIDE') return 'OUTSIDE'
    if (rel === 'BOUNDARY') result = 'BOUNDARY'
  }
  for (let i = 0; i < line.length - 1; i++) {
    const mid = [(line[i][0] + line[i + 1][0]) / 2, (line[i][1] + line[i + 1][1]) / 2]
    const rel = pointInPolygon(mid, rings)
    if (rel === 'OUTSIDE') return 'OUTSIDE'
    if (rel === 'BOUNDARY') result = 'BOUNDARY'
  }
  return result
}

/**
 * Classify containment of polygon B within polygon A.
 * Returns 'INSIDE' if entirely in interior, 'BOUNDARY' if inside but
 * touches boundary, 'OUTSIDE' if any part is outside.
 *
 * @param {number[][][]} ringsA
 * @param {number[][][]} ringsB
 * @returns {Relation}
 */
function polygonContainsPolygon(ringsA, ringsB) {
  /** @type {Relation} */
  let result = 'INSIDE'
  for (const pt of ringsB[0]) {
    const rel = pointInPolygon(pt, ringsA)
    if (rel === 'OUTSIDE') return 'OUTSIDE'
    if (rel === 'BOUNDARY') result = 'BOUNDARY'
  }
  for (let i = 0; i < ringsB[0].length - 1; i++) {
    const mid = [(ringsB[0][i][0] + ringsB[0][i + 1][0]) / 2, (ringsB[0][i][1] + ringsB[0][i + 1][1]) / 2]
    const rel = pointInPolygon(mid, ringsA)
    if (rel === 'OUTSIDE') return 'OUTSIDE'
    if (rel === 'BOUNDARY') result = 'BOUNDARY'
  }
  return result
}

/**
 * Classify the relationship between two linestrings.
 * Returns INSIDE if interiors share a point, BOUNDARY if they only meet
 * at endpoints, OUTSIDE if disjoint.
 *
 * @param {number[][]} line1
 * @param {number[][]} line2
 * @returns {Relation}
 */
function lineLineRelation(line1, line2) {
  let boundary = false
  for (let i = 0; i < line1.length - 1; i++) {
    for (let j = 0; j < line2.length - 1; j++) {
      const touchPoint = segmentTouchPoint(line1[i], line1[i + 1], line2[j], line2[j + 1])
      if (touchPoint === 'OUTSIDE') continue
      if (touchPoint === 'INSIDE') return 'INSIDE'
      if (!isLineEndpoint(touchPoint, line1) && !isLineEndpoint(touchPoint, line2)) return 'INSIDE'
      boundary = true
    }
  }
  return boundary ? 'BOUNDARY' : 'OUTSIDE'
}

/**
 * Classify the relationship between a linestring and a polygon.
 * Returns INSIDE if line interior enters polygon interior, BOUNDARY if
 * they only share boundary points, OUTSIDE if disjoint.
 *
 * @param {number[][]} line
 * @param {number[][][]} rings
 * @returns {Relation}
 */
function linePolygonRelation(line, rings) {
  let boundary = false
  // Check segment midpoints and interior vertices
  for (let i = 0; i < line.length - 1; i++) {
    const mid = [(line[i][0] + line[i + 1][0]) / 2, (line[i][1] + line[i + 1][1]) / 2]
    const midRel = pointInPolygon(mid, rings)
    if (midRel === 'INSIDE') return 'INSIDE'
    if (midRel === 'BOUNDARY') boundary = true
  }
  // Check interior vertices of the line
  for (let i = 1; i < line.length - 1; i++) {
    const rel = pointInPolygon(line[i], rings)
    if (rel === 'INSIDE') return 'INSIDE'
    if (rel === 'BOUNDARY') boundary = true
  }
  // Check line endpoints
  for (const pt of [line[0], line[line.length - 1]]) {
    const rel = pointInPolygon(pt, rings)
    if (rel === 'INSIDE') return 'INSIDE'
    if (rel === 'BOUNDARY') boundary = true
  }
  // Check if any edge of the polygon rings intersects the line
  if (!boundary) {
    for (let i = 0; i < line.length - 1; i++) {
      for (const ring of rings) {
        if (segmentIntersectsRing(line[i], line[i + 1], ring)) {
          boundary = true
        }
      }
    }
  }
  return boundary ? 'BOUNDARY' : 'OUTSIDE'
}

/**
 * Classify the relationship between two polygons.
 * Returns INSIDE if interiors share area, BOUNDARY if they only share
 * boundary points/edges, OUTSIDE if disjoint.
 *
 * @param {number[][][]} rings1
 * @param {number[][][]} rings2
 * @returns {Relation}
 */
function polygonPolygonRelation(rings1, rings2) {
  let boundary = false
  // Check vertices of polygon1 against polygon2
  for (const pt of rings1[0]) {
    const rel = pointInPolygon(pt, rings2)
    if (rel === 'INSIDE') return 'INSIDE'
    if (rel === 'BOUNDARY') boundary = true
  }
  // Check vertices of polygon2 against polygon1
  for (const pt of rings2[0]) {
    const rel = pointInPolygon(pt, rings1)
    if (rel === 'INSIDE') return 'INSIDE'
    if (rel === 'BOUNDARY') boundary = true
  }
  // Check edge midpoints of polygon1 against polygon2
  for (let i = 0; i < rings1[0].length - 1; i++) {
    const mid = [(rings1[0][i][0] + rings1[0][i + 1][0]) / 2, (rings1[0][i][1] + rings1[0][i + 1][1]) / 2]
    const rel = pointInPolygon(mid, rings2)
    if (rel === 'INSIDE') return 'INSIDE'
    if (rel === 'BOUNDARY') boundary = true
  }
  // Check edge midpoints of polygon2 against polygon1
  for (let i = 0; i < rings2[0].length - 1; i++) {
    const mid = [(rings2[0][i][0] + rings2[0][i + 1][0]) / 2, (rings2[0][i][1] + rings2[0][i + 1][1]) / 2]
    const rel = pointInPolygon(mid, rings1)
    if (rel === 'INSIDE') return 'INSIDE'
    if (rel === 'BOUNDARY') boundary = true
  }
  // Check edge-edge intersections
  if (!boundary) {
    for (let i = 0; i < rings1[0].length - 1; i++) {
      for (let j = 0; j < rings2[0].length - 1; j++) {
        if (segmentsIntersect(rings1[0][i], rings1[0][i + 1], rings2[0][j], rings2[0][j + 1])) {
          boundary = true
        }
      }
    }
  }
  return boundary ? 'BOUNDARY' : 'OUTSIDE'
}

/**
 * @param {SimpleGeometry[]} partsA
 * @param {SimpleGeometry[]} partsB
 * @returns {boolean}
 */
export function intersects(partsA, partsB) {
  for (const pa of partsA) {
    for (const pb of partsB) {
      if (pairIntersects(pa, pb)) return true
    }
  }
  return false
}

/**
 * @param {SimpleGeometry} a
 * @param {SimpleGeometry} b
 * @returns {boolean}
 */
function pairIntersects(a, b) {
  if (!bboxOverlap(a, b)) return false
  const ta = a.type
  const tb = b.type

  if (ta === 'Point' && tb === 'Point') {
    return distSq(a.coordinates, b.coordinates) < EPSILON_SQ
  }
  if (ta === 'Point' && tb === 'LineString') {
    return pointOnLine(a.coordinates, b.coordinates)
  }
  if (ta === 'LineString' && tb === 'Point') {
    return pointOnLine(b.coordinates, a.coordinates)
  }
  if (ta === 'Point' && tb === 'Polygon') {
    return pointInPolygon(a.coordinates, b.coordinates) !== 'OUTSIDE'
  }
  if (ta === 'Polygon' && tb === 'Point') {
    return pointInPolygon(b.coordinates, a.coordinates) !== 'OUTSIDE'
  }
  if (ta === 'LineString' && tb === 'LineString') {
    return linesIntersect(a.coordinates, b.coordinates)
  }
  if (ta === 'LineString' && tb === 'Polygon') {
    return lineIntersectsPolygon(a.coordinates, b.coordinates)
  }
  if (ta === 'Polygon' && tb === 'LineString') {
    return lineIntersectsPolygon(b.coordinates, a.coordinates)
  }
  if (ta === 'Polygon' && tb === 'Polygon') {
    return polygonPolygonRelation(a.coordinates, b.coordinates) !== 'OUTSIDE'
  }
  return false
}

/**
 * Classify the relationship between two simple geometries.
 * Returns 'INSIDE' if interiors intersect, 'BOUNDARY' if they
 * intersect only at boundaries, or 'OUTSIDE' if they don't intersect.
 *
 * @param {SimpleGeometry} a
 * @param {SimpleGeometry} b
 * @returns {Relation}
 */
export function pairRelation(a, b) {
  if (!bboxOverlap(a, b)) return 'OUTSIDE'
  const ta = a.type
  const tb = b.type

  // Point / Point
  if (ta === 'Point' && tb === 'Point') {
    return distSq(a.coordinates, b.coordinates) < EPSILON_SQ ? 'INSIDE' : 'OUTSIDE'
  }

  // Point / LineString
  if (ta === 'Point' && tb === 'LineString') {
    return pointLineRelation(a.coordinates, b.coordinates)
  }
  if (ta === 'LineString' && tb === 'Point') {
    return pointLineRelation(b.coordinates, a.coordinates)
  }

  // Point / Polygon
  if (ta === 'Point' && tb === 'Polygon') {
    return pointInPolygon(a.coordinates, b.coordinates)
  }
  if (ta === 'Polygon' && tb === 'Point') {
    return pointInPolygon(b.coordinates, a.coordinates)
  }

  // LineString / LineString
  if (ta === 'LineString' && tb === 'LineString') {
    return lineLineRelation(a.coordinates, b.coordinates)
  }

  // LineString / Polygon
  if (ta === 'LineString' && tb === 'Polygon') {
    return linePolygonRelation(a.coordinates, b.coordinates)
  }
  if (ta === 'Polygon' && tb === 'LineString') {
    return linePolygonRelation(b.coordinates, a.coordinates)
  }

  // Polygon / Polygon
  if (ta === 'Polygon' && tb === 'Polygon') {
    return polygonPolygonRelation(a.coordinates, b.coordinates)
  }

  return 'OUTSIDE'
}

/**
 * Classify containment of b within a.
 * Returns 'INSIDE' if b is strictly in a's interior, 'BOUNDARY' if b is
 * inside a but touches a's boundary, 'OUTSIDE' if any part of b is outside a.
 *
 * @param {SimpleGeometry} a
 * @param {SimpleGeometry} b
 * @returns {Relation}
 */
export function pairContainment(a, b) {
  if (!bboxOverlap(a, b)) return 'OUTSIDE'
  const ta = a.type
  const tb = b.type

  if (ta === 'Point' && tb === 'Point') {
    return distSq(a.coordinates, b.coordinates) < EPSILON_SQ ? 'BOUNDARY' : 'OUTSIDE'
  }
  if (ta === 'LineString' && tb === 'Point') {
    return pointLineRelation(b.coordinates, a.coordinates)
  }
  if (ta === 'Polygon' && tb === 'Point') {
    return pointInPolygon(b.coordinates, a.coordinates)
  }
  if (ta === 'Polygon' && tb === 'LineString') {
    return polygonContainsLine(b.coordinates, a.coordinates)
  }
  if (ta === 'Polygon' && tb === 'Polygon') {
    return polygonContainsPolygon(a.coordinates, b.coordinates)
  }
  if (ta === 'LineString' && tb === 'LineString') {
    // Line A contains line B if every point of B is on A
    for (const pt of b.coordinates) {
      if (!pointOnLine(pt, a.coordinates)) return 'OUTSIDE'
    }
    return 'BOUNDARY'
  }
  return 'OUTSIDE'
}
