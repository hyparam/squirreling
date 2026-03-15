import type { BoundingBox, Geometry, SimpleGeometry } from './geometry.js'

export function decompose(geom: Geometry): SimpleGeometry[]
export function bbox(geom: SimpleGeometry): BoundingBox
export function bboxOverlap(a: SimpleGeometry, b: SimpleGeometry): boolean
export function parseWkt(wkt: string): Geometry | null
