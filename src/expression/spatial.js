import { geomToWkt, parseWkt } from './wkt.js'

/**
 * @import { SpatialFunc, SqlPrimitive } from '../types.js'
 * @import { Geometry, Point, Relation, SimpleGeometry } from './geometry.js'
 */

const EPSILON = 1e-10
const EPSILON_SQ = EPSILON * EPSILON

/**
 * Evaluate a spatial predicate function.
 *
 * @param {Object} options
 * @param {SpatialFunc} options.funcName
 * @param {SqlPrimitive[]} options.args
 * @returns {SqlPrimitive}
 */
export function evaluateSpatialFunc({ funcName, args }) {
  // Constructor / accessor functions (don't require two geometries)
  if (funcName === 'ST_GEOMFROMTEXT') {
    if (args[0] == null) return null
    return parseWkt(String(args[0]))
  }

  if (funcName === 'ST_MAKEENVELOPE') {
    if (args[0] == null || args[1] == null || args[2] == null || args[3] == null) return null
    const xmin = Number(args[0])
    const ymin = Number(args[1])
    const xmax = Number(args[2])
    const ymax = Number(args[3])
    return {
      type: 'Polygon',
      coordinates: [[[xmin, ymin], [xmax, ymin], [xmax, ymax], [xmin, ymax], [xmin, ymin]]],
    }
  }

  if (funcName === 'ST_ASTEXT') {
    const geom = toGeometry(args[0])
    if (geom == null) return null
    return geomToWkt(geom)
  }

  // Predicate functions (require two geometries)
  const a = toGeometry(args[0])
  const b = toGeometry(args[1])

  if (a == null || b == null) return null

  switch (funcName) {
  case 'ST_INTERSECTS': return stIntersects(a, b)
  case 'ST_CONTAINS': return stContains(a, b)
  case 'ST_CONTAINSPROPERLY': return stContainsProperly(a, b)
  case 'ST_WITHIN': return stContains(b, a) // inverse of contains
  case 'ST_OVERLAPS': return stOverlaps(a, b)
  case 'ST_TOUCHES': return stTouches(a, b)
  case 'ST_EQUALS': return stEquals(a, b)
  case 'ST_CROSSES': return stCrosses(a, b)
  case 'ST_COVERS': return stContains(a, b) // TODO: handle boundary
  case 'ST_COVEREDBY': return stContains(b, a) // inverse of covers
  case 'ST_DWITHIN': {
    if (args[2] == null) return null
    const dist = Number(args[2])
    return stDWithin(a, b, dist)
  }
  default:
    return null
  }
}

/**
 * Normalize a geometry value. Accepts GeoJSON objects.
 * Returns null if the value is not a valid geometry.
 *
 * @param {SqlPrimitive} val
 * @returns {Geometry | null}
 */
function toGeometry(val) {
  if (typeof val === 'object' && val != null && 'type' in val) {
    if (val.type === 'GeometryCollection' && Array.isArray(val.geometries)) {
      // eslint-disable-next-line no-extra-parens
      return /** @type {Geometry} */ (val)
    }
    const geometryTypes = ['Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']
    if (geometryTypes.includes(val.type) && Array.isArray(val.coordinates)) {
      // eslint-disable-next-line no-extra-parens
      return /** @type {Geometry} */ (val)
    }
  }
  return null
}

// ============================================================================
// Core geometric algorithms
// ============================================================================

/**
 * Compute the squared distance between two 2D points.
 *
 * @param {number[]} a
 * @param {number[]} b
 * @returns {number}
 */
function distSq(a, b) {
  const dx = a[0] - b[0]
  const dy = a[1] - b[1]
  return dx * dx + dy * dy
}

/**
 * Squared minimum distance from point p to line segment [a, b].
 *
 * @param {number[]} p
 * @param {number[]} a
 * @param {number[]} b
 * @returns {number}
 */
function pointToSegmentDistSq(p, a, b) {
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
function segmentsIntersect(p1, p2, p3, p4) {
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
function pointInPolygon(point, rings) {
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
 * Test if a line segment intersects a ring boundary.
 *
 * @param {number[]} a
 * @param {number[]} b
 * @param {number[][]} ring
 * @returns {boolean}
 */
function segmentIntersectsRing(a, b, ring) {
  for (let i = 0; i < ring.length - 1; i++) {
    if (segmentsIntersect(a, b, ring[i], ring[i + 1])) return true
  }
  return false
}

/**
 * Test if a linestring intersects a polygon.
 *
 * @param {number[][]} line
 * @param {number[][][]} rings
 * @returns {boolean}
 */
function lineIntersectsPolygon(line, rings) {
  // Check if any point of the line is inside the polygon
  for (const pt of line) {
    if (pointInPolygon(pt, rings) !== 'OUTSIDE') return true
  }
  // Check if any segment of the line intersects any ring edge
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
 * Test if two rings are equal (same vertices, possibly different starting point).
 *
 * @param {number[][]} ring1
 * @param {number[][]} ring2
 * @returns {boolean}
 */
function ringsEqual(ring1, ring2) {
  if (ring1.length !== ring2.length) return false
  // Try every rotation
  const n = ring1.length - 1 // closed ring, last = first
  for (let offset = 0; offset < n; offset++) {
    let match = true
    for (let i = 0; i < n; i++) {
      const j = (i + offset) % n
      if (Math.abs(ring1[i][0] - ring2[j][0]) > EPSILON ||
          Math.abs(ring1[i][1] - ring2[j][1]) > EPSILON) {
        match = false
        break
      }
    }
    if (match) return true
  }
  // Try reverse direction
  for (let offset = 0; offset < n; offset++) {
    let match = true
    for (let i = 0; i < n; i++) {
      const j = (n - i + offset) % n
      if (Math.abs(ring1[i][0] - ring2[j][0]) > EPSILON ||
          Math.abs(ring1[i][1] - ring2[j][1]) > EPSILON) {
        match = false
        break
      }
    }
    if (match) return true
  }
  return false
}

// ============================================================================
// Minimum distance between geometries
// ============================================================================

/**
 * Get all coordinates from a geometry as a flat list of [x, y] points.
 *
 * @param {Geometry} geom
 * @returns {number[][]}
 */
function getPoints(geom) {
  switch (geom.type) {
  case 'Point': return [geom.coordinates]
  case 'MultiPoint': return geom.coordinates
  case 'LineString': return geom.coordinates
  case 'MultiLineString': return geom.coordinates.flat()
  case 'Polygon': return geom.coordinates.flat()
  case 'MultiPolygon': return geom.coordinates.flatMap(p => p.flat())
  case 'GeometryCollection': return geom.geometries.flatMap(getPoints)
  default: return []
  }
}

/**
 * Get all line segments from a geometry.
 *
 * @param {Geometry} geom
 * @returns {Array<[number[], number[]]>}
 */
function getSegments(geom) {
  /** @type {Array<[number[], number[]]>} */
  const segments = []
  /**
   * @param {number[][]} coords
   */
  function addLine(coords) {
    for (let i = 0; i < coords.length - 1; i++) {
      segments.push([coords[i], coords[i + 1]])
    }
  }
  switch (geom.type) {
  case 'LineString': addLine(geom.coordinates); break
  case 'MultiLineString': geom.coordinates.forEach(l => addLine(l)); break
  case 'Polygon': geom.coordinates.forEach(r => addLine(r)); break
  case 'MultiPolygon': geom.coordinates.forEach(p => p.forEach(r => addLine(r))); break
  case 'GeometryCollection': geom.geometries.forEach(g => segments.push(...getSegments(g))); break
  }
  return segments
}

/**
 * Minimum distance between two segments.
 *
 * @param {number[]} a1
 * @param {number[]} a2
 * @param {number[]} b1
 * @param {number[]} b2
 * @returns {number}
 */
function segmentToSegmentDistSq(a1, a2, b1, b2) {
  if (segmentsIntersect(a1, a2, b1, b2)) return 0
  return Math.min(
    pointToSegmentDistSq(a1, b1, b2),
    pointToSegmentDistSq(a2, b1, b2),
    pointToSegmentDistSq(b1, a1, a2),
    pointToSegmentDistSq(b2, a1, a2)
  )
}

/**
 * Test whether two geometries are within a given distance of each other.
 * Exits early as soon as any pair of elements is within range.
 *
 * @param {Geometry} a
 * @param {Geometry} b
 * @param {number} distance
 * @returns {boolean}
 */
function stDWithin(a, b, distance) {
  const distanceSq = distance * distance
  // Handle geometries that contain each other
  if (a.type === 'Polygon' || a.type === 'MultiPolygon') {
    const pts = getPoints(b)
    for (const pt of pts) {
      if (a.type === 'Polygon' && pointInPolygon(pt, a.coordinates) !== 'OUTSIDE') return true
      if (a.type === 'MultiPolygon') {
        for (const poly of a.coordinates) {
          if (pointInPolygon(pt, poly) !== 'OUTSIDE') return true
        }
      }
    }
  }
  if (b.type === 'Polygon' || b.type === 'MultiPolygon') {
    const pts = getPoints(a)
    for (const pt of pts) {
      if (b.type === 'Polygon' && pointInPolygon(pt, b.coordinates) !== 'OUTSIDE') return true
      if (b.type === 'MultiPolygon') {
        for (const poly of b.coordinates) {
          if (pointInPolygon(pt, poly) !== 'OUTSIDE') return true
        }
      }
    }
  }

  // Segment-to-segment
  const segsA = getSegments(a)
  const segsB = getSegments(b)
  for (const [a1, a2] of segsA) {
    for (const [b1, b2] of segsB) {
      if (segmentToSegmentDistSq(a1, a2, b1, b2) <= distanceSq) return true
    }
  }

  // Point-to-segment
  const ptsA = getPoints(a)
  const ptsB = getPoints(b)
  if (segsB.length) {
    for (const pt of ptsA) {
      for (const [b1, b2] of segsB) {
        if (pointToSegmentDistSq(pt, b1, b2) <= distanceSq) return true
      }
    }
  }
  if (segsA.length) {
    for (const pt of ptsB) {
      for (const [a1, a2] of segsA) {
        if (pointToSegmentDistSq(pt, a1, a2) <= distanceSq) return true
      }
    }
  }

  // Point-to-point fallback
  if (segsA.length === 0 && segsB.length === 0) {
    for (const pa of ptsA) {
      for (const pb of ptsB) {
        if (distSq(pa, pb) <= distanceSq) return true
      }
    }
  }

  return false
}

// ============================================================================
// Spatial predicate dispatch - decompose to primitive type pairs
// ============================================================================

/**
 * Decompose Multi* and GeometryCollection into simple geometries.
 *
 * @param {Geometry} geom
 * @returns {SimpleGeometry[]}
 */
function decompose(geom) {
  switch (geom.type) {
  case 'MultiPoint':
    return geom.coordinates.map(c => ({ type: 'Point', coordinates: c }))
  case 'MultiLineString':
    return geom.coordinates.map(c => ({ type: 'LineString', coordinates: c }))
  case 'MultiPolygon':
    return geom.coordinates.map(c => ({ type: 'Polygon', coordinates: c }))
  case 'GeometryCollection':
    return geom.geometries.flatMap(decompose)
  default:
    return [geom]
  }
}

// ============================================================================
// ST_Intersects
// ============================================================================

/**
 * @param {Geometry} a
 * @param {Geometry} b
 * @returns {boolean}
 */
function stIntersects(a, b) {
  const partsA = decompose(a)
  const partsB = decompose(b)
  // Some part of A must intersect some part of B
  for (const pa of partsA) {
    for (const pb of partsB) {
      if (simplePairIntersects(pa, pb)) return true
    }
  }
  return false
}

/**
 * @param {SimpleGeometry} a
 * @param {SimpleGeometry} b
 * @returns {boolean}
 */
function simplePairIntersects(a, b) {
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
 * Test if point is on a linestring.
 *
 * @param {number[]} point
 * @param {number[][]} line
 * @returns {boolean}
 */
function pointOnLine(point, line) {
  for (let i = 0; i < line.length - 1; i++) {
    if (pointToSegmentDistSq(point, line[i], line[i + 1]) < EPSILON_SQ) return true
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
function simplePairRelation(a, b) {
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
 * Classify a point relative to a linestring.
 *
 * @param {number[]} point
 * @param {number[][]} line
 * @returns {Relation}
 */
function pointLineRelation(point, line) {
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
      if (!segmentsIntersect(line1[i], line1[i + 1], line2[j], line2[j + 1])) continue
      // Segments intersect, check if the intersection is interior to both lines
      // Check segment midpoints
      const mid1 = [(line1[i][0] + line1[i + 1][0]) / 2, (line1[i][1] + line1[i + 1][1]) / 2]
      if (pointLineRelation(mid1, line1) === 'INSIDE' && pointLineRelation(mid1, line2) === 'INSIDE') {
        return 'INSIDE'
      }
      const mid2 = [(line2[j][0] + line2[j + 1][0]) / 2, (line2[j][1] + line2[j + 1][1]) / 2]
      if (pointLineRelation(mid2, line1) === 'INSIDE' && pointLineRelation(mid2, line2) === 'INSIDE') {
        return 'INSIDE'
      }
      // Check actual intersection point
      const ip = segmentIntersectionPoint(line1[i], line1[i + 1], line2[j], line2[j + 1])
      if (ip) {
        if (pointLineRelation(ip, line1) === 'INSIDE' && pointLineRelation(ip, line2) === 'INSIDE') {
          return 'INSIDE'
        }
      }
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

// ============================================================================
// ST_Contains
// ============================================================================

/**
 * @param {Geometry} a
 * @param {Geometry} b
 * @returns {boolean}
 */
function stContains(a, b) {
  const partsA = decompose(a)
  const partsB = decompose(b)
  // Every part of b must be inside some part of a
  return partsB.every(pb => partsA.some(pa => simplePairContainment(pa, pb) !== 'OUTSIDE'))
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
function simplePairContainment(a, b) {
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

// ============================================================================
// ST_ContainsProperly
// ============================================================================

/**
 * @param {Geometry} a
 * @param {Geometry} b
 * @returns {boolean}
 */
function stContainsProperly(a, b) {
  const partsA = decompose(a)
  const partsB = decompose(b)
  // Every part of b must be strictly inside some part of a
  return partsB.every(pb => partsA.some(pa => simplePairContainment(pa, pb) === 'INSIDE'))
}

// ============================================================================
// ST_Touches
// ============================================================================

/**
 * @param {Geometry} a
 * @param {Geometry} b
 * @returns {boolean}
 */
function stTouches(a, b) {
  const partsA = decompose(a)
  const partsB = decompose(b)
  let intersects = false
  for (const pa of partsA) {
    for (const pb of partsB) {
      const rel = simplePairRelation(pa, pb)
      if (rel === 'INSIDE') return false
      if (rel === 'BOUNDARY') intersects = true
    }
  }
  return intersects
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
function segmentIntersectionPoint(p1, p2, p3, p4) {
  const d1x = p2[0] - p1[0], d1y = p2[1] - p1[1]
  const d2x = p4[0] - p3[0], d2y = p4[1] - p3[1]
  const denom = d1x * d2y - d1y * d2x
  if (Math.abs(denom) < EPSILON) return null // parallel
  const t = ((p3[0] - p1[0]) * d2y - (p3[1] - p1[1]) * d2x) / denom
  return [p1[0] + t * d1x, p1[1] + t * d1y]
}

// ============================================================================
// ST_Overlaps
// ============================================================================

/**
 * @param {Geometry} a
 * @param {Geometry} b
 * @returns {boolean}
 */
function stOverlaps(a, b) {
  // Overlaps requires same dimension, and that each geometry has some part
  // inside the other and some part outside
  const dimA = geometryDimension(a)
  const dimB = geometryDimension(b)
  if (dimA !== dimB) return false
  if (!stIntersects(a, b)) return false
  if (stEquals(a, b)) return false
  // Must not be containment
  if (stContains(a, b) || stContains(b, a)) return false
  return true
}

/**
 * @param {Geometry} geom
 * @returns {number}
 */
function geometryDimension(geom) {
  switch (geom.type) {
  case 'Point':
  case 'MultiPoint':
    return 0
  case 'LineString':
  case 'MultiLineString':
    return 1
  case 'Polygon':
  case 'MultiPolygon':
    return 2
  case 'GeometryCollection': {
    let max = 0
    for (const g of geom.geometries) {
      max = Math.max(max, geometryDimension(g))
    }
    return max
  }
  default:
    return 0
  }
}

// ============================================================================
// ST_Equals
// ============================================================================

/**
 * @param {Geometry} a
 * @param {Geometry} b
 * @returns {boolean}
 */
function stEquals(a, b) {
  const partsA = decompose(a)
  const partsB = decompose(b)

  if (partsA.length !== partsB.length) return false

  // For each simple geometry in A, find a matching one in B
  const used = new Set()
  for (const pa of partsA) {
    let found = false
    for (let i = 0; i < partsB.length; i++) {
      if (used.has(i)) continue
      if (simpleGeomEqual(pa, partsB[i])) {
        used.add(i)
        found = true
        break
      }
    }
    if (!found) return false
  }
  return true
}

/**
 * @param {SimpleGeometry} a
 * @param {SimpleGeometry} b
 * @returns {boolean}
 */
function simpleGeomEqual(a, b) {
  if (a.type === 'Point' && b.type === 'Point') {
    return distSq(a.coordinates, b.coordinates) < EPSILON_SQ
  } else if (a.type === 'LineString' && b.type === 'LineString') {
    return lineEqual(a.coordinates, b.coordinates)
  } else if (a.type === 'Polygon' && b.type === 'Polygon') {
    return polygonEqual(a.coordinates, b.coordinates)
  }
  return false
}

/**
 * @param {number[][]} a
 * @param {number[][]} b
 * @returns {boolean}
 */
function lineEqual(a, b) {
  if (a.length !== b.length) return false
  // Forward
  let forward = true
  for (let i = 0; i < a.length; i++) {
    if (Math.abs(a[i][0] - b[i][0]) > EPSILON || Math.abs(a[i][1] - b[i][1]) > EPSILON) {
      forward = false
      break
    }
  }
  if (forward) return true
  // Reverse
  for (let i = 0; i < a.length; i++) {
    if (Math.abs(a[i][0] - b[a.length - 1 - i][0]) > EPSILON || Math.abs(a[i][1] - b[a.length - 1 - i][1]) > EPSILON) {
      return false
    }
  }
  return true
}

/**
 * @param {number[][][]} a
 * @param {number[][][]} b
 * @returns {boolean}
 */
function polygonEqual(a, b) {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (!ringsEqual(a[i], b[i])) return false
  }
  return true
}

// ============================================================================
// ST_Crosses
// ============================================================================

/**
 * @param {Geometry} a
 * @param {Geometry} b
 * @returns {boolean}
 */
function stCrosses(a, b) {
  // Crosses: interiors intersect, and the intersection has lower dimension
  // than the maximum of the two geometries' dimensions
  const dimA = geometryDimension(a)
  const dimB = geometryDimension(b)

  if (!stIntersects(a, b)) return false

  // Point/Point or Polygon/Polygon cannot cross
  if (dimA === dimB && dimA !== 1) return false

  // Line/Line: they cross if they intersect at a point (not overlap)
  if (dimA === 1 && dimB === 1) {
    // They cross if they intersect but neither contains the other
    // and the intersection is a set of points (not line segments)
    return !stContains(a, b) && !stContains(b, a) && !stTouches(a, b)
  }

  // Point/Line, Point/Polygon: point "in interior"
  if (dimA === 0 && dimB >= 1) {
    // eslint-disable-next-line no-extra-parens
    const partsA = /** @type {Point[]} */ (decompose(a))
    const partsB = decompose(b)
    for (const pa of partsA) {
      for (const pb of partsB) {
        if (pb.type === 'LineString') {
          if (pointLineRelation(pa.coordinates, pb.coordinates) === 'INSIDE') return true
        }
        if (pb.type === 'Polygon') {
          if (pointInPolygon(pa.coordinates, pb.coordinates) === 'INSIDE') return true
        }
      }
    }
    return false
  }

  // Line/Polygon: line crosses polygon if part of line is inside and part is outside
  if (dimA === 1 && dimB === 2) {
    return !stContains(b, a)
  }

  // Symmetric cases
  if (dimA >= 1 && dimB === 0) return stCrosses(b, a)
  if (dimA === 2 && dimB === 1) return stCrosses(b, a)

  return false
}
