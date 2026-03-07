import { bbox } from '../spatial/bbox.js'
import { parseWkt } from '../spatial/wkt.js'

/**
 * @import { RowGroup } from 'hyparquet'
 * @import { BoundingBox, Geometry, SimpleGeometry } from '../spatial/geometry.js'
 * @import { ExprNode } from '../types.js'
 */

/**
 * @typedef {{ column: string, queryBbox: BoundingBox }} SpatialFilter
 */

/**
 * Extract a spatial filter from a WHERE clause AST.
 * Matches patterns like ST_WITHIN(column, ST_GEOMFROMTEXT('...'))
 * where the first arg is a bare column ref and the second is a constant geometry.
 *
 * @param {ExprNode | undefined} where
 * @returns {SpatialFilter | undefined}
 */
export function extractSpatialFilter(where) {
  if (!where || where.type !== 'function') return
  if (where.name !== 'ST_WITHIN' && where.name !== 'ST_INTERSECTS') return
  const [first, second] = where.args
  if (!first || first.type !== 'identifier') return
  // Second arg must be a constant geometry expression
  const geom = evaluateConstantGeom(second)
  if (!geom) return
  const queryBbox = geomBbox(geom)
  if (!queryBbox) return
  return { column: first.name, queryBbox }
}

/**
 * Try to evaluate a constant geometry expression from the AST.
 * Supports ST_GEOMFROMTEXT('...') and ST_MAKEENVELOPE(x1, y1, x2, y2).
 *
 * @param {ExprNode} node
 * @returns {Geometry | undefined}
 */
function evaluateConstantGeom(node) {
  if (node.type !== 'function') return
  if (node.name === 'ST_GEOMFROMTEXT') {
    if (node.args.length !== 1 || node.args[0].type !== 'literal') return
    const wkt = node.args[0].value
    if (typeof wkt !== 'string') return
    return parseWkt(wkt) ?? undefined
  }
  if (node.name === 'ST_MAKEENVELOPE') {
    if (node.args.length < 4) return
    const nums = node.args.slice(0, 4).map(a => a.type === 'literal' ? Number(a.value) : NaN)
    if (nums.some(n => isNaN(n))) return
    const [xmin, ymin, xmax, ymax] = nums
    return {
      type: 'Polygon',
      coordinates: [[[xmin, ymin], [xmax, ymin], [xmax, ymax], [xmin, ymax], [xmin, ymin]]],
    }
  }
}

/**
 * Compute the bounding box of any geometry type.
 *
 * @param {Geometry} geom
 * @returns {BoundingBox | undefined}
 */
function geomBbox(geom) {
  if (geom.type === 'Point' || geom.type === 'LineString' || geom.type === 'Polygon') {
    return bbox(geom)
  }
  // For multi/collection types, compute the union bbox
  /** @type {SimpleGeometry[]} */
  let parts
  if (geom.type === 'MultiPoint') {
    parts = geom.coordinates.map(c => ({ type: 'Point', coordinates: c }))
  } else if (geom.type === 'MultiLineString') {
    parts = geom.coordinates.map(c => ({ type: 'LineString', coordinates: c }))
  } else if (geom.type === 'MultiPolygon') {
    parts = geom.coordinates.map(c => ({ type: 'Polygon', coordinates: c }))
  } else {
    return // GeometryCollection - not worth the complexity
  }
  if (!parts.length) return
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity
  for (const part of parts) {
    const b = bbox(part)
    if (b.minX < minX) minX = b.minX
    if (b.minY < minY) minY = b.minY
    if (b.maxX > maxX) maxX = b.maxX
    if (b.maxY > maxY) maxY = b.maxY
  }
  return { minX, minY, maxX, maxY }
}

/**
 * Check if a row group's geospatial statistics overlap with a query bounding box.
 *
 * @param {RowGroup} rowGroup
 * @param {SpatialFilter} spatialFilter
 * @returns {boolean}
 */
export function rowGroupOverlaps(rowGroup, { column, queryBbox }) {
  for (const col of rowGroup.columns) {
    const pathName = col.meta_data?.path_in_schema?.join('.')
    if (pathName !== column) continue
    const geoBbox = col.meta_data?.geospatial_statistics?.bbox
    if (!geoBbox) return true // no stats, can't skip
    return geoBbox.xmin <= queryBbox.maxX &&
      geoBbox.xmax >= queryBbox.minX &&
      geoBbox.ymin <= queryBbox.maxY &&
      geoBbox.ymax >= queryBbox.minY
  }
  return true // column not found, can't skip
}
