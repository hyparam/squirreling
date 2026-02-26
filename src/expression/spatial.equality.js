/**
 * @import { SimpleGeometry } from './geometry.js'
 */

import { EPSILON, EPSILON_SQ, distSq } from './spatial.geometry.js'

/**
 * @param {SimpleGeometry} a
 * @param {SimpleGeometry} b
 * @returns {boolean}
 */
export function simpleGeomEqual(a, b) {
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
export function lineEqual(a, b) {
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
export function polygonEqual(a, b) {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (!ringsEqual(a[i], b[i])) return false
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
export function ringsEqual(ring1, ring2) {
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
