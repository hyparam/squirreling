/**
 * @import { Geometry } from './spatial.js'
 */

/**
 * Parse a WKT string into a GeoJSON geometry.
 *
 * @param {string} wkt
 * @returns {Geometry | null}
 */
export function parseWkt(wkt) {
  const s = wkt.trim()
  const upper = s.toUpperCase()

  if (upper.startsWith('POINT')) {
    const coords = parseWktCoordinate(s.slice(5).trim())
    if (!coords) return null
    return { type: 'Point', coordinates: coords }
  }

  if (upper.startsWith('MULTIPOINT')) {
    const inner = extractParens(s.slice(10).trim())
    if (inner == null) return null
    const coords = parseWktCoordinateList(inner)
    if (!coords) return null
    return { type: 'MultiPoint', coordinates: coords }
  }

  if (upper.startsWith('MULTILINESTRING')) {
    const inner = extractParens(s.slice(15).trim())
    if (inner == null) return null
    const rings = parseWktRingList(inner)
    if (!rings) return null
    return { type: 'MultiLineString', coordinates: rings }
  }

  if (upper.startsWith('MULTIPOLYGON')) {
    const inner = extractParens(s.slice(12).trim())
    if (inner == null) return null
    const polys = parseWktPolygonList(inner)
    if (!polys) return null
    return { type: 'MultiPolygon', coordinates: polys }
  }

  if (upper.startsWith('LINESTRING')) {
    const inner = extractParens(s.slice(10).trim())
    if (inner == null) return null
    const coords = parseWktCoordinateList(inner)
    if (!coords) return null
    return { type: 'LineString', coordinates: coords }
  }

  if (upper.startsWith('POLYGON')) {
    const inner = extractParens(s.slice(7).trim())
    if (inner == null) return null
    const rings = parseWktRingList(inner)
    if (!rings) return null
    return { type: 'Polygon', coordinates: rings }
  }

  return null
}

/**
 * Convert a GeoJSON geometry to WKT.
 *
 * @param {Geometry} geom
 * @returns {string}
 */
export function geomToWkt(geom) {
  switch (geom.type) {
  case 'Point':
    return `POINT (${coordToWkt(geom.coordinates)})`
  case 'MultiPoint':
    return `MULTIPOINT (${geom.coordinates.map(c => `(${coordToWkt(c)})`).join(', ')})`
  case 'LineString':
    return `LINESTRING (${coordListToWkt(geom.coordinates)})`
  case 'MultiLineString':
    return `MULTILINESTRING (${geom.coordinates.map(l => `(${coordListToWkt(l)})`).join(', ')})`
  case 'Polygon':
    return `POLYGON (${geom.coordinates.map(r => `(${coordListToWkt(r)})`).join(', ')})`
  case 'MultiPolygon':
    return `MULTIPOLYGON (${geom.coordinates.map(p => `(${p.map((/** @type {number[][]} */ r) => `(${coordListToWkt(r)})`).join(', ')})`).join(', ')})`
  case 'GeometryCollection':
    return `GEOMETRYCOLLECTION (${(geom.geometries || []).map(g => geomToWkt(g)).join(', ')})`
  default:
    return ''
  }
}

/**
 * Extract content inside outer parentheses.
 *
 * @param {string} s
 * @returns {string | null}
 */
function extractParens(s) {
  const trimmed = s.trim()
  if (!trimmed.startsWith('(') || !trimmed.endsWith(')')) return null
  return trimmed.slice(1, -1).trim()
}

/**
 * Parse a single coordinate like "(1 2)" or "1 2".
 *
 * @param {string} s
 * @returns {number[] | null}
 */
function parseWktCoordinate(s) {
  const inner = s.trim().replace(/^\(/, '').replace(/\)$/, '').trim()
  const parts = inner.split(/\s+/)
  if (parts.length < 2) return null
  const x = Number(parts[0])
  const y = Number(parts[1])
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null
  return [x, y]
}

/**
 * Parse a comma-separated list of coordinates like "1 2, 3 4, 5 6".
 *
 * @param {string} s
 * @returns {number[][] | null}
 */
function parseWktCoordinateList(s) {
  const parts = s.split(',')
  /** @type {number[][]} */
  const coords = []
  for (const part of parts) {
    const trimmed = part.trim().replace(/^\(/, '').replace(/\)$/, '').trim()
    const nums = trimmed.split(/\s+/)
    if (nums.length < 2) return null
    const x = Number(nums[0])
    const y = Number(nums[1])
    if (!Number.isFinite(x) || !Number.isFinite(y)) return null
    coords.push([x, y])
  }
  return coords.length ? coords : null
}

/**
 * Parse a list of rings like "(1 2, 3 4), (5 6, 7 8)".
 *
 * @param {string} s
 * @returns {number[][][] | null}
 */
function parseWktRingList(s) {
  /** @type {number[][][]} */
  const rings = []
  const ringStrs = splitTopLevel(s)
  for (const ringStr of ringStrs) {
    const inner = extractParens(ringStr.trim())
    if (inner == null) return null
    const coords = parseWktCoordinateList(inner)
    if (!coords) return null
    rings.push(coords)
  }
  return rings.length ? rings : null
}

/**
 * Parse a list of polygons like "((ring1), (ring2)), ((ring3))".
 *
 * @param {string} s
 * @returns {number[][][][] | null}
 */
function parseWktPolygonList(s) {
  /** @type {number[][][][]} */
  const polys = []
  const polyStrs = splitTopLevel(s)
  for (const polyStr of polyStrs) {
    const inner = extractParens(polyStr.trim())
    if (inner == null) return null
    const rings = parseWktRingList(inner)
    if (!rings) return null
    polys.push(rings)
  }
  return polys.length ? polys : null
}

/**
 * Split a string by commas at the top-level (not inside parentheses).
 *
 * @param {string} s
 * @returns {string[]}
 */
function splitTopLevel(s) {
  /** @type {string[]} */
  const parts = []
  let depth = 0
  let start = 0
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '(') depth++
    else if (s[i] === ')') depth--
    else if (s[i] === ',' && depth === 0) {
      parts.push(s.slice(start, i))
      start = i + 1
    }
  }
  parts.push(s.slice(start))
  return parts
}

/**
 * Format a single coordinate to WKT.
 *
 * @param {number[]} coord
 * @returns {string}
 */
function coordToWkt(coord) {
  return `${coord[0]} ${coord[1]}`
}

/**
 * Format a coordinate list to WKT.
 *
 * @param {number[][]} coords
 * @returns {string}
 */
function coordListToWkt(coords) {
  return coords.map(coordToWkt).join(', ')
}
