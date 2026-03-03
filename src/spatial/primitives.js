export const EPSILON = 1e-10
export const EPSILON_SQ = EPSILON * EPSILON

/**
 * Compute the squared distance between two 2D points.
 *
 * @param {number[]} a
 * @param {number[]} b
 * @returns {number}
 */
export function distSq(a, b) {
  const dx = a[0] - b[0]
  const dy = a[1] - b[1]
  return dx * dx + dy * dy
}
