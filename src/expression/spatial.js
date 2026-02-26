import { geomToWkt, parseWkt } from './wkt.js'
import { distSq, pointInPolygon, pointLineRelation, pointToSegmentDistSq, simpleIntersects, simplePairContainment, simplePairRelation } from './spatial.geometry.js'
import { simpleGeomEqual } from './spatial.equality.js'

/**
 * @import { SpatialFunc, SqlPrimitive } from '../types.js'
 * @import { Geometry, Point, SimpleGeometry } from './geometry.js'
 */

/**
 * Evaluate a spatial predicate function.
 *
 * @param {Object} options
 * @param {SpatialFunc} options.funcName
 * @param {SqlPrimitive[]} options.args
 * @returns {SqlPrimitive}
 */
export function evaluateSpatialFunc({ funcName, args }) {
  // Singleton functions
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

  const geomA = toGeometry(args[0])
  if (funcName === 'ST_ASTEXT') {
    if (geomA == null) return null
    return geomToWkt(geomA)
  }

  // Predicate functions (require two geometries)
  const geomB = toGeometry(args[1])
  if (geomA == null || geomB == null) return null
  const a = decompose(geomA)
  const b = decompose(geomB)

  switch (funcName) {
  case 'ST_INTERSECTS': return simpleIntersects(a, b)
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
  default: return null
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
// Minimum distance between geometries
// ============================================================================

/**
 * Get all line segments from a geometry.
 *
 * @param {SimpleGeometry[]} geoms
 * @returns {{ segments: Array<[number[], number[]]>, points: number[][] }}
 */
function getSegments(geoms) {
  /** @type {Array<[number[], number[]]>} */
  const segments = []
  /** @type {number[][]} */
  const points = []

  /**
   * @param {number[][]} coords
   */
  function addLine(coords) {
    for (let i = 0; i < coords.length - 1; i++) {
      segments.push([coords[i], coords[i + 1]])
    }
    points.push(...coords)
  }

  for (const geom of geoms) {
    if (geom.type === 'Point') points.push(geom.coordinates)
    else if (geom.type === 'LineString') addLine(geom.coordinates)
    else if (geom.type === 'Polygon') geom.coordinates.forEach(addLine)
  }

  return { segments, points }
}

/**
 * Test whether two geometries are within a given distance of each other.
 * Intersecting geometries have distance 0. For non-intersecting geometries,
 * the minimum distance is always at an endpoint, so point-to-segment suffices.
 *
 * @param {SimpleGeometry[]} a
 * @param {SimpleGeometry[]} b
 * @param {number} distance
 * @returns {boolean}
 */
function stDWithin(a, b, distance) {
  if (distance < 0) return false
  if (simpleIntersects(a, b)) return true

  const distanceSq = distance * distance
  const { points: ptsA, segments: segsA } = getSegments(a)
  const { points: ptsB, segments: segsB } = getSegments(b)

  // Point-to-point
  for (const pa of ptsA) {
    for (const pb of ptsB) {
      if (distSq(pa, pb) <= distanceSq) return true
    }
  }

  // Point-to-segment
  for (const pt of ptsA) {
    for (const [b1, b2] of segsB) {
      if (pointToSegmentDistSq(pt, b1, b2) <= distanceSq) return true
    }
  }
  for (const pt of ptsB) {
    for (const [a1, a2] of segsA) {
      if (pointToSegmentDistSq(pt, a1, a2) <= distanceSq) return true
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
// ST_Contains
// ============================================================================

/**
 * @param {SimpleGeometry[]} a
 * @param {SimpleGeometry[]} b
 * @returns {boolean}
 */
function stContains(a, b) {
  // Every part of b must be inside some part of a
  return b.every(pb => a.some(pa => simplePairContainment(pa, pb) !== 'OUTSIDE'))
}

// ============================================================================
// ST_ContainsProperly
// ============================================================================

/**
 * @param {SimpleGeometry[]} a
 * @param {SimpleGeometry[]} b
 * @returns {boolean}
 */
function stContainsProperly(a, b) {
  // Every part of b must be strictly inside some part of a
  return b.every(pb => a.some(pa => simplePairContainment(pa, pb) === 'INSIDE'))
}

// ============================================================================
// ST_Touches
// ============================================================================

/**
 * @param {SimpleGeometry[]} a
 * @param {SimpleGeometry[]} b
 * @returns {boolean}
 */
function stTouches(a, b) {
  let intersects = false
  for (const pa of a) {
    for (const pb of b) {
      const rel = simplePairRelation(pa, pb)
      if (rel === 'INSIDE') return false
      if (rel === 'BOUNDARY') intersects = true
    }
  }
  return intersects
}

// ============================================================================
// ST_Overlaps
// ============================================================================

/**
 * @param {SimpleGeometry[]} a
 * @param {SimpleGeometry[]} b
 * @returns {boolean}
 */
function stOverlaps(a, b) {
  // Overlaps requires same dimension, and that each geometry has some part
  // inside the other and some part outside
  const dimA = geometryDimension(a)
  const dimB = geometryDimension(b)
  if (dimA !== dimB) return false
  if (!simpleIntersects(a, b)) return false
  if (stEquals(a, b)) return false
  // Must not be containment
  if (stContains(a, b) || stContains(b, a)) return false
  return true
}

/**
 * @param {SimpleGeometry[]} parts
 * @returns {number}
 */
function geometryDimension(parts) {
  let max = 0
  for (const geom of parts) {
    switch (geom.type) {
    case 'Point':
      break
    case 'LineString':
      if (max < 1) max = 1
      break
    case 'Polygon':
      return 2
    }
  }
  return max
}

// ============================================================================
// ST_Equals
// ============================================================================

/**
 * @param {SimpleGeometry[]} a
 * @param {SimpleGeometry[]} b
 * @returns {boolean}
 */
function stEquals(a, b) {
  if (a.length !== b.length) return false

  // For each simple geometry in a, find a matching one in b
  const used = new Set()
  for (const pa of a) {
    let found = false
    for (let i = 0; i < b.length; i++) {
      if (used.has(i)) continue
      if (simpleGeomEqual(pa, b[i])) {
        used.add(i)
        found = true
        break
      }
    }
    if (!found) return false
  }
  return true
}

// ============================================================================
// ST_Crosses
// ============================================================================

/**
 * @param {SimpleGeometry[]} a
 * @param {SimpleGeometry[]} b
 * @returns {boolean}
 */
function stCrosses(a, b) {
  // Crosses: interiors intersect, and the intersection has lower dimension
  // than the maximum of the two geometries' dimensions
  const dimA = geometryDimension(a)
  const dimB = geometryDimension(b)

  if (!simpleIntersects(a, b)) return false

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
    for (const pa of a) {
      // eslint-disable-next-line no-extra-parens
      const point = /** @type {Point} */ (pa)
      for (const pb of b) {
        if (pb.type === 'LineString') {
          if (pointLineRelation(point.coordinates, pb.coordinates) === 'INSIDE') return true
        }
        if (pb.type === 'Polygon') {
          if (pointInPolygon(point.coordinates, pb.coordinates) === 'INSIDE') return true
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
