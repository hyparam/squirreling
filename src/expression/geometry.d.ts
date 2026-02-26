/**
 * Geometry types based on the GeoJSON specification (RFC 7946)
 */
export type Geometry =
  | Point
  | MultiPoint
  | LineString
  | MultiLineString
  | Polygon
  | MultiPolygon
  | GeometryCollection

/**
 * Simple geometries that are not collections.
 */
export type SimpleGeometry = Point | LineString | Polygon

/**
 * Position is an array of at least two numbers.
 * The order should be [longitude, latitude] with optional properties (eg- altitude).
 */
export type Position = number[]

export interface Point {
  type: 'Point'
  coordinates: Position
}

export interface MultiPoint {
  type: 'MultiPoint'
  coordinates: Position[]
}

export interface LineString {
  type: 'LineString'
  coordinates: Position[]
}

/**
 * Each element is one LineString.
 */
export interface MultiLineString {
  type: 'MultiLineString'
  coordinates: Position[][]
}

/**
 * Each element is a linear ring.
 */
export interface Polygon {
  type: 'Polygon'
  coordinates: Position[][]
}

/**
 * Each element is one Polygon.
 */
export interface MultiPolygon {
  type: 'MultiPolygon'
  coordinates: Position[][][]
}

export interface GeometryCollection {
  type: 'GeometryCollection'
  geometries: Geometry[]
}
