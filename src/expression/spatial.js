import { geomToWkt, parseWkt } from './wkt.js'

/**
 * @import { SpatialFunc, SqlPrimitive } from '../types.js'
 * @import { Geometry, Point, SimpleGeometry } from './geometry.js'
 */

const EPSILON = 1e-10

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
    return geometryDistance(a, b) <= dist
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
 * Compute the Euclidean distance between two 2D points.
 *
 * @param {number[]} a
 * @param {number[]} b
 * @returns {number}
 */
function pointDist(a, b) {
  return Math.sqrt(distSq(a, b))
}

/**
 * Minimum distance from point p to line segment [a, b].
 *
 * @param {number[]} p
 * @param {number[]} a
 * @param {number[]} b
 * @returns {number}
 */
function pointToSegmentDist(p, a, b) {
  const dx = b[0] - a[0]
  const dy = b[1] - a[1]
  const lenSq = dx * dx + dy * dy
  if (lenSq === 0) return pointDist(p, a)
  let t = ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / lenSq
  t = Math.max(0, Math.min(1, t))
  return pointDist(p, [a[0] + t * dx, a[1] + t * dy])
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
  return Math.min(a[0], b[0]) - EPSILON <= c[0] && c[0] <= Math.max(a[0], b[0]) + EPSILON &&
         Math.min(a[1], b[1]) - EPSILON <= c[1] && c[1] <= Math.max(a[1], b[1]) + EPSILON
}

/**
 * Point-in-polygon test using ray casting.
 * ring is an array of [x, y] coords (closed ring, first = last).
 *
 * @param {number[]} point
 * @param {number[][]} ring
 * @returns {boolean}
 */
function pointInRing(point, ring) {
  const [px, py] = point
  let inside = false
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i]
    const [xj, yj] = ring[j]
    if (yi > py !== yj > py && px < (xj - xi) * (py - yi) / (yj - yi) + xi) {
      inside = !inside
    }
  }
  return inside
}

/**
 * Test if point is on the boundary of a ring.
 *
 * @param {number[]} point
 * @param {number[][]} ring
 * @returns {boolean}
 */
function pointOnRingBoundary(point, ring) {
  for (let i = 0; i < ring.length - 1; i++) {
    if (pointToSegmentDist(point, ring[i], ring[i + 1]) < EPSILON) {
      return true
    }
  }
  return false
}

/**
 * Test if point is inside a polygon (array of rings).
 * First ring is exterior, rest are holes.
 *
 * @param {number[]} point
 * @param {number[][][]} rings
 * @returns {boolean}
 */
function pointInPolygon(point, rings) {
  if (!pointInRing(point, rings[0]) && !pointOnRingBoundary(point, rings[0])) return false
  for (let i = 1; i < rings.length; i++) {
    // Point must not be inside a hole (but can be on hole boundary)
    if (pointInRing(point, rings[i]) && !pointOnRingBoundary(point, rings[i])) return false
  }
  return true
}

/**
 * Test if point is strictly inside a polygon (not on boundary).
 *
 * @param {number[]} point
 * @param {number[][][]} rings
 * @returns {boolean}
 */
function pointInPolygonInterior(point, rings) {
  if (!pointInRing(point, rings[0])) return false
  if (pointOnRingBoundary(point, rings[0])) return false
  for (let i = 1; i < rings.length; i++) {
    if (pointInRing(point, rings[i])) return false
  }
  return true
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
    if (pointInPolygon(pt, rings)) return true
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
 * Test if two polygons share any space.
 *
 * @param {number[][][]} rings1
 * @param {number[][][]} rings2
 * @returns {boolean}
 */
function polygonsIntersect(rings1, rings2) {
  // Check if any vertex of polygon1 is inside polygon2
  for (const pt of rings1[0]) {
    if (pointInPolygon(pt, rings2)) return true
  }
  // Check if any vertex of polygon2 is inside polygon1
  for (const pt of rings2[0]) {
    if (pointInPolygon(pt, rings1)) return true
  }
  // Check if any edge of polygon1 intersects any edge of polygon2
  for (let i = 0; i < rings1[0].length - 1; i++) {
    for (let j = 0; j < rings2[0].length - 1; j++) {
      if (segmentsIntersect(rings1[0][i], rings1[0][i + 1], rings2[0][j], rings2[0][j + 1])) return true
    }
  }
  return false
}

/**
 * Test if all points of a linestring are inside a polygon.
 *
 * @param {number[][]} line
 * @param {number[][][]} rings
 * @returns {boolean}
 */
function lineInsidePolygon(line, rings) {
  for (const pt of line) {
    if (!pointInPolygon(pt, rings)) return false
  }
  // Also check that no segment crosses a hole boundary from inside to outside
  for (let i = 0; i < line.length - 1; i++) {
    const mid = [(line[i][0] + line[i + 1][0]) / 2, (line[i][1] + line[i + 1][1]) / 2]
    if (!pointInPolygon(mid, rings)) return false
  }
  return true
}

/**
 * Test if all points of a linestring are strictly in the polygon interior.
 *
 * @param {number[][]} line
 * @param {number[][][]} rings
 * @returns {boolean}
 */
function lineInPolygonInterior(line, rings) {
  for (const pt of line) {
    if (!pointInPolygonInterior(pt, rings)) return false
  }
  for (let i = 0; i < line.length - 1; i++) {
    const mid = [(line[i][0] + line[i + 1][0]) / 2, (line[i][1] + line[i + 1][1]) / 2]
    if (!pointInPolygonInterior(mid, rings)) return false
  }
  return true
}

/**
 * Test if polygon A contains polygon B (all of B is inside A).
 *
 * @param {number[][][]} ringsA
 * @param {number[][][]} ringsB
 * @returns {boolean}
 */
function polygonContainsPolygon(ringsA, ringsB) {
  // Every vertex of B's exterior must be inside A
  for (const pt of ringsB[0]) {
    if (!pointInPolygon(pt, ringsA)) return false
  }
  // Check that edges of B don't cross outside A
  for (let i = 0; i < ringsB[0].length - 1; i++) {
    const mid = [(ringsB[0][i][0] + ringsB[0][i + 1][0]) / 2, (ringsB[0][i][1] + ringsB[0][i + 1][1]) / 2]
    if (!pointInPolygon(mid, ringsA)) return false
  }
  return true
}

/**
 * Test if polygon A contains polygon B properly (no boundary contact).
 *
 * @param {number[][][]} ringsA
 * @param {number[][][]} ringsB
 * @returns {boolean}
 */
function polygonContainsPolygonProperly(ringsA, ringsB) {
  for (const pt of ringsB[0]) {
    if (!pointInPolygonInterior(pt, ringsA)) return false
  }
  for (let i = 0; i < ringsB[0].length - 1; i++) {
    const mid = [(ringsB[0][i][0] + ringsB[0][i + 1][0]) / 2, (ringsB[0][i][1] + ringsB[0][i + 1][1]) / 2]
    if (!pointInPolygonInterior(mid, ringsA)) return false
  }
  return true
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
function segmentToSegmentDist(a1, a2, b1, b2) {
  if (segmentsIntersect(a1, a2, b1, b2)) return 0
  return Math.min(
    pointToSegmentDist(a1, b1, b2),
    pointToSegmentDist(a2, b1, b2),
    pointToSegmentDist(b1, a1, a2),
    pointToSegmentDist(b2, a1, a2)
  )
}

/**
 * Compute the minimum distance between two geometries.
 *
 * @param {Geometry} a
 * @param {Geometry} b
 * @returns {number}
 */
function geometryDistance(a, b) {
  // Handle geometries that contain each other
  if (a.type === 'Polygon' || a.type === 'MultiPolygon') {
    const pts = getPoints(b)
    for (const pt of pts) {
      if (a.type === 'Polygon' && pointInPolygon(pt, a.coordinates)) return 0
      if (a.type === 'MultiPolygon') {
        for (const poly of a.coordinates) {
          if (pointInPolygon(pt, poly)) return 0
        }
      }
    }
  }
  if (b.type === 'Polygon' || b.type === 'MultiPolygon') {
    const pts = getPoints(a)
    for (const pt of pts) {
      if (b.type === 'Polygon' && pointInPolygon(pt, b.coordinates)) return 0
      if (b.type === 'MultiPolygon') {
        for (const poly of b.coordinates) {
          if (pointInPolygon(pt, poly)) return 0
        }
      }
    }
  }

  const segsA = getSegments(a)
  const segsB = getSegments(b)
  const ptsA = getPoints(a)
  const ptsB = getPoints(b)

  let min = Infinity

  // Segment-to-segment
  for (const [a1, a2] of segsA) {
    for (const [b1, b2] of segsB) {
      min = Math.min(min, segmentToSegmentDist(a1, a2, b1, b2))
    }
  }

  // Point-to-segment
  if (segsB.length) {
    for (const pt of ptsA) {
      for (const [b1, b2] of segsB) {
        min = Math.min(min, pointToSegmentDist(pt, b1, b2))
      }
    }
  }
  if (segsA.length) {
    for (const pt of ptsB) {
      for (const [a1, a2] of segsA) {
        min = Math.min(min, pointToSegmentDist(pt, a1, a2))
      }
    }
  }

  // Point-to-point fallback
  if (segsA.length === 0 && segsB.length === 0) {
    for (const pa of ptsA) {
      for (const pb of ptsB) {
        min = Math.min(min, pointDist(pa, pb))
      }
    }
  }

  return min
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
    return pointDist(a.coordinates, b.coordinates) < EPSILON
  }
  if (ta === 'Point' && tb === 'LineString') {
    return pointOnLine(a.coordinates, b.coordinates)
  }
  if (ta === 'LineString' && tb === 'Point') {
    return pointOnLine(b.coordinates, a.coordinates)
  }
  if (ta === 'Point' && tb === 'Polygon') {
    return pointInPolygon(a.coordinates, b.coordinates)
  }
  if (ta === 'Polygon' && tb === 'Point') {
    return pointInPolygon(b.coordinates, a.coordinates)
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
    return polygonsIntersect(a.coordinates, b.coordinates)
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
    if (pointToSegmentDist(point, line[i], line[i + 1]) < EPSILON) return true
  }
  return false
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
  return partsB.every(pb => partsA.some(pa => simplePairContains(pa, pb)))
}

/**
 * @param {SimpleGeometry} a
 * @param {SimpleGeometry} b
 * @returns {boolean}
 */
function simplePairContains(a, b) {
  const ta = a.type
  const tb = b.type

  if (ta === 'Point' && tb === 'Point') {
    return pointDist(a.coordinates, b.coordinates) < EPSILON
  }
  if (ta === 'LineString' && tb === 'Point') {
    return pointOnLine(b.coordinates, a.coordinates)
  }
  if (ta === 'Polygon' && tb === 'Point') {
    return pointInPolygon(b.coordinates, a.coordinates)
  }
  if (ta === 'Polygon' && tb === 'LineString') {
    return lineInsidePolygon(b.coordinates, a.coordinates)
  }
  if (ta === 'Polygon' && tb === 'Polygon') {
    return polygonContainsPolygon(a.coordinates, b.coordinates)
  }
  if (ta === 'LineString' && tb === 'LineString') {
    // Line A contains line B if every point of B is on A
    for (const pt of b.coordinates) {
      if (!pointOnLine(pt, a.coordinates)) return false
    }
    return true
  }
  return false
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
  return partsB.every(pb => partsA.some(pa => simplePairContainsProperly(pa, pb)))
}

/**
 * @param {SimpleGeometry} a
 * @param {SimpleGeometry} b
 * @returns {boolean}
 */
function simplePairContainsProperly(a, b) {
  const ta = a.type
  const tb = b.type

  if (ta === 'Polygon' && tb === 'Point') {
    return pointInPolygonInterior(b.coordinates, a.coordinates)
  }
  if (ta === 'Polygon' && tb === 'LineString') {
    return lineInPolygonInterior(b.coordinates, a.coordinates)
  }
  if (ta === 'Polygon' && tb === 'Polygon') {
    return polygonContainsPolygonProperly(a.coordinates, b.coordinates)
  }
  // Points and lines have no interior in the topological sense for "contains properly"
  return false
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
  // Geometries touch if they intersect but their interiors do not
  if (!stIntersects(a, b)) return false

  const partsA = decompose(a)
  const partsB = decompose(b)
  for (const pa of partsA) {
    for (const pb of partsB) {
      if (simplePairInteriorsIntersect(pa, pb)) return false
    }
  }
  return true
}

/**
 * Test if interiors of two simple geometries share any point.
 *
 * @param {SimpleGeometry} a
 * @param {SimpleGeometry} b
 * @returns {boolean}
 */
function simplePairInteriorsIntersect(a, b) {
  const ta = a.type
  const tb = b.type

  if (ta === 'Point' && tb === 'Point') {
    // A point's interior is the point itself, so equal points have
    // intersecting interiors.
    return pointDist(a.coordinates, b.coordinates) < EPSILON
  }
  if (ta === 'Point' && tb === 'LineString') {
    // Interior of a linestring excludes endpoints
    return pointInLineInterior(a.coordinates, b.coordinates)
  }
  if (ta === 'LineString' && tb === 'Point') {
    return pointInLineInterior(b.coordinates, a.coordinates)
  }
  if (ta === 'Point' && tb === 'Polygon') {
    return pointInPolygonInterior(a.coordinates, b.coordinates)
  }
  if (ta === 'Polygon' && tb === 'Point') {
    return pointInPolygonInterior(b.coordinates, a.coordinates)
  }
  if (ta === 'LineString' && tb === 'LineString') {
    // Check if lines share interior points (not just endpoints)
    return linesShareInterior(a.coordinates, b.coordinates)
  }
  if (ta === 'LineString' && tb === 'Polygon') {
    return lineInteriorIntersectsPolygonInterior(a.coordinates, b.coordinates)
  }
  if (ta === 'Polygon' && tb === 'LineString') {
    return lineInteriorIntersectsPolygonInterior(b.coordinates, a.coordinates)
  }
  if (ta === 'Polygon' && tb === 'Polygon') {
    return polygonInteriorsIntersect(a.coordinates, b.coordinates)
  }
  return false
}

/**
 * @param {number[]} point
 * @param {number[][]} line
 * @returns {boolean}
 */
function pointInLineInterior(point, line) {
  // Interior of line excludes endpoints
  if (pointDist(point, line[0]) < EPSILON) return false
  if (pointDist(point, line[line.length - 1]) < EPSILON) return false
  return pointOnLine(point, line)
}

/**
 * @param {number[][]} line1
 * @param {number[][]} line2
 * @returns {boolean}
 */
function linesShareInterior(line1, line2) {
  // Check if any interior point of one line lies on the other line's interior
  for (let i = 0; i < line1.length - 1; i++) {
    for (let j = 0; j < line2.length - 1; j++) {
      if (segmentsIntersect(line1[i], line1[i + 1], line2[j], line2[j + 1])) {
        // Find the intersection point and check if it's interior to both
        // For simplicity, check segment midpoints
        const mid1 = [(line1[i][0] + line1[i + 1][0]) / 2, (line1[i][1] + line1[i + 1][1]) / 2]
        if (pointOnLine(mid1, line2) && pointInLineInterior(mid1, line1) && pointInLineInterior(mid1, line2)) {
          return true
        }
        const mid2 = [(line2[j][0] + line2[j + 1][0]) / 2, (line2[j][1] + line2[j + 1][1]) / 2]
        if (pointOnLine(mid2, line1) && pointInLineInterior(mid2, line1) && pointInLineInterior(mid2, line2)) {
          return true
        }
        // Also check the actual intersection point
        const ip = segmentIntersectionPoint(line1[i], line1[i + 1], line2[j], line2[j + 1])
        if (ip && pointInLineInterior(ip, line1) && pointInLineInterior(ip, line2)) {
          return true
        }
      }
    }
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
function segmentIntersectionPoint(p1, p2, p3, p4) {
  const d1x = p2[0] - p1[0], d1y = p2[1] - p1[1]
  const d2x = p4[0] - p3[0], d2y = p4[1] - p3[1]
  const denom = d1x * d2y - d1y * d2x
  if (Math.abs(denom) < EPSILON) return null // parallel
  const t = ((p3[0] - p1[0]) * d2y - (p3[1] - p1[1]) * d2x) / denom
  return [p1[0] + t * d1x, p1[1] + t * d1y]
}

/**
 * @param {number[][]} line
 * @param {number[][][]} rings
 * @returns {boolean}
 */
function lineInteriorIntersectsPolygonInterior(line, rings) {
  // Check if any interior point of the line is inside the polygon interior
  for (let i = 0; i < line.length - 1; i++) {
    const mid = [(line[i][0] + line[i + 1][0]) / 2, (line[i][1] + line[i + 1][1]) / 2]
    if (pointInPolygonInterior(mid, rings)) return true
  }
  // Also check interior points of the line
  for (let i = 1; i < line.length - 1; i++) {
    if (pointInPolygonInterior(line[i], rings)) return true
  }
  return false
}

/**
 * @param {number[][][]} rings1
 * @param {number[][][]} rings2
 * @returns {boolean}
 */
function polygonInteriorsIntersect(rings1, rings2) {
  // Check if any vertex of polygon1 is inside polygon2's interior
  for (const pt of rings1[0]) {
    if (pointInPolygonInterior(pt, rings2)) return true
  }
  // Check if any vertex of polygon2 is inside polygon1's interior
  for (const pt of rings2[0]) {
    if (pointInPolygonInterior(pt, rings1)) return true
  }
  // Check if any edge midpoints are inside the other polygon's interior
  for (let i = 0; i < rings1[0].length - 1; i++) {
    const mid = [(rings1[0][i][0] + rings1[0][i + 1][0]) / 2, (rings1[0][i][1] + rings1[0][i + 1][1]) / 2]
    if (pointInPolygonInterior(mid, rings2)) return true
  }
  for (let i = 0; i < rings2[0].length - 1; i++) {
    const mid = [(rings2[0][i][0] + rings2[0][i + 1][0]) / 2, (rings2[0][i][1] + rings2[0][i + 1][1]) / 2]
    if (pointInPolygonInterior(mid, rings1)) return true
  }
  return false
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
    return pointDist(a.coordinates, b.coordinates) < EPSILON
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
    for (const pa of partsA) {
      const partsB = decompose(b)
      for (const pb of partsB) {
        if (pb.type === 'LineString') {
          if (pointInLineInterior(pa.coordinates, pb.coordinates)) return true
        }
        if (pb.type === 'Polygon') {
          if (pointInPolygonInterior(pa.coordinates, pb.coordinates)) return true
        }
      }
    }
    return false
  }

  // Line/Polygon: line crosses polygon if part of line is inside and part is outside
  if (dimA === 1 && dimB === 2) {
    return stIntersects(a, b) && !stContains(b, a)
  }

  // Symmetric cases
  if (dimA >= 1 && dimB === 0) return stCrosses(b, a)
  if (dimA === 2 && dimB === 1) return stCrosses(b, a)

  return false
}
