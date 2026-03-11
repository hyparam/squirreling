/**
 * @import { BBox, SimpleGeometry } from './geometry.js'
 */

export const EPSILON = 1e-10
export const EPSILON_SQ = EPSILON * EPSILON

/** @type {WeakMap<SimpleGeometry, BBox>} */
const bboxCache = new WeakMap()

/**
 * Test whether two bounding boxes overlap.
 *
 * @param {SimpleGeometry} a
 * @param {SimpleGeometry} b
 * @returns {boolean}
 */
export function bboxOverlap(a, b) {
  const aBox = bbox(a)
  const bBox = bbox(b)
  return aBox.minX <= bBox.maxX && aBox.maxX >= bBox.minX && aBox.minY <= bBox.maxY && aBox.maxY >= bBox.minY
}

/**
 * Compute the axis-aligned bounding box of a simple geometry.
 * Results are cached per geometry object.
 *
 * @param {SimpleGeometry} geom
 * @returns {BBox}
 */
export function bbox(geom) {
  let b = bboxCache.get(geom)
  if (b) return b
  if (geom.type === 'Point') {
    const [x, y] = geom.coordinates
    b = { minX: x, minY: y, maxX: x, maxY: y }
  } else {
    /** @type {number[][]} */
    const points = geom.type === 'LineString'
      ? geom.coordinates
      : geom.coordinates[0] // outer ring
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity
    for (const p of points) {
      if (p[0] < minX) minX = p[0]
      if (p[1] < minY) minY = p[1]
      if (p[0] > maxX) maxX = p[0]
      if (p[1] > maxY) maxY = p[1]
    }
    b = { minX, minY, maxX, maxY }
  }
  bboxCache.set(geom, b)
  return b
}
