import { ParseError, UnknownFunctionError } from '../validation/parseErrors.js'

/**
 * @import { AggregateFunc, BinaryOp, CastType, FunctionSignature, IntervalUnit, MathFunc, RegExpFunction, SpatialFunc, StringFunc, UserDefinedFunction } from '../types.js'
 */

export const niladicFuncs = ['CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP']

/**
 * @param {string} name
 * @returns {name is AggregateFunc}
 */
export function isAggregateFunc(name) {
  return ['COUNT', 'COUNTIF', 'SUM', 'AVG', 'MIN', 'MAX', 'ARRAY_AGG', 'JSON_ARRAYAGG', 'STDDEV_SAMP', 'STDDEV_POP', 'MEDIAN', 'PERCENTILE_CONT', 'APPROX_QUANTILE', 'STRING_AGG'].includes(name)
}

/**
 * @param {string} name
 * @returns {name is MathFunc}
 */
export function isMathFunc(name) {
  return [
    'FLOOR', 'CEIL', 'CEILING', 'ROUND', 'ABS', 'SIGN', 'MOD', 'EXP', 'LN', 'LOG10', 'POW', 'POWER', 'SQRT',
    'SIN', 'COS', 'TAN', 'COT', 'ASIN', 'ACOS', 'ATAN', 'ATAN2', 'DEGREES', 'RADIANS', 'PI',
    'RAND', 'RANDOM',
  ].includes(name)
}

/**
 * @param {string} name
 * @returns {boolean}
 */
export function isWindowFunc(name) {
  return ['ROW_NUMBER'].includes(name)
}

/**
 * @param {string} name
 * @returns {name is RegExpFunction}
 */
export function isRegexpFunc(name) {
  return ['REGEXP_SUBSTR', 'REGEXP_EXTRACT', 'REGEXP_REPLACE', 'REGEXP_MATCHES'].includes(name)
}

/**
 * Table-valued functions produce rows (used in FROM clause), not scalar values.
 *
 * @param {string} name
 * @returns {boolean}
 */
export function isTableFunction(name) {
  return ['UNNEST', 'EXPLODE', 'JSON_EACH'].includes(name)
}

/**
 * @param {string} name
 * @returns {name is SpatialFunc}
 */
export function isSpatialFunc(name) {
  return [
    'ST_INTERSECTS', 'ST_CONTAINS', 'ST_CONTAINSPROPERLY', 'ST_WITHIN',
    'ST_OVERLAPS', 'ST_TOUCHES', 'ST_EQUALS', 'ST_CROSSES',
    'ST_COVERS', 'ST_COVEREDBY', 'ST_DWITHIN',
    'ST_GEOMFROMTEXT', 'ST_MAKEENVELOPE', 'ST_ASTEXT',
  ].includes(name)
}

/**
 * @param {string} name
 * @returns {name is IntervalUnit}
 */
export function isIntervalUnit(name) {
  return ['DAY', 'MONTH', 'YEAR', 'HOUR', 'MINUTE', 'SECOND'].includes(name)
}

/**
 * @param {string} name
 * @returns {boolean}
 */
export function isExtractField(name) {
  return ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'DOW', 'EPOCH'].includes(name)
}

/**
 * @param {string} name
 * @returns {name is CastType}
 */
export function isCastType(name) {
  return ['TEXT', 'STRING', 'VARCHAR', 'INTEGER', 'INT', 'BIGINT', 'FLOAT', 'REAL', 'DOUBLE', 'BOOLEAN', 'BOOL'].includes(name)
}

/**
 * @param {string} name
 * @returns {name is StringFunc}
 */
export function isStringFunc(name) {
  return [
    'UPPER', 'LOWER', 'CONCAT', 'LENGTH', 'SUBSTRING', 'SUBSTR', 'TRIM',
    'REPLACE', 'LEFT', 'RIGHT', 'INSTR', 'POSITION', 'STRPOS',
  ].includes(name)
}

/**
 * @param {string} op
 * @returns {op is BinaryOp}
 */
export function isBinaryOp(op) {
  return ['AND', 'OR', 'LIKE', '=', '==', '!=', '<>', '<', '>', '<=', '>='].includes(op)
}

/**
 * Function signatures: argument counts and human-readable parameter signatures.
 * @type {Record<string, FunctionSignature>}
 */
export const FUNCTION_SIGNATURES = {
  // String functions
  UPPER: { min: 1, max: 1, signature: 'string' },
  LOWER: { min: 1, max: 1, signature: 'string' },
  LENGTH: { min: 1, max: 1, signature: 'string' },
  TRIM: { min: 1, max: 1, signature: 'string' },
  REPLACE: { min: 3, max: 3, signature: 'string, search, replacement' },
  SUBSTRING: { min: 2, max: 3, signature: 'string, start[, length]' },
  SUBSTR: { min: 2, max: 3, signature: 'string, start[, length]' },
  CONCAT: { min: 1, signature: 'value1, value2[, ...]' },
  LEFT: { min: 2, max: 2, signature: 'string, length' },
  RIGHT: { min: 2, max: 2, signature: 'string, length' },
  INSTR: { min: 2, max: 2, signature: 'string, substring' },
  POSITION: { min: 2, max: 2, signature: 'string, substring' },
  STRPOS: { min: 2, max: 2, signature: 'string, substring' },
  REGEXP_SUBSTR: { min: 2, max: 4, signature: 'string, pattern[, position[, occurrence]]' },
  REGEXP_EXTRACT: { min: 2, max: 4, signature: 'string, pattern[, position[, occurrence]]' },
  REGEXP_REPLACE: { min: 3, max: 5, signature: 'string, pattern, replacement[, position[, occurrence]]' },
  REGEXP_MATCHES: { min: 2, max: 2, signature: 'string, pattern' },

  // Date/time functions
  RANDOM: { min: 0, max: 0, signature: '' },
  RAND: { min: 0, max: 0, signature: '' },
  CURRENT_DATE: { min: 0, max: 0, signature: '' },
  CURRENT_TIME: { min: 0, max: 0, signature: '' },
  CURRENT_TIMESTAMP: { min: 0, max: 0, signature: '' },
  DATE_TRUNC: { min: 2, max: 2, signature: 'unit, date' },
  DATE_PART: { min: 2, max: 2, signature: 'field, date' },
  EXTRACT: { min: 2, max: 2, signature: 'field FROM date' },

  // Math functions
  FLOOR: { min: 1, max: 1, signature: 'number' },
  CEIL: { min: 1, max: 1, signature: 'number' },
  CEILING: { min: 1, max: 1, signature: 'number' },
  ROUND: { min: 1, max: 2, signature: 'number[, decimals]' },
  ABS: { min: 1, max: 1, signature: 'number' },
  SIGN: { min: 1, max: 1, signature: 'number' },
  MOD: { min: 2, max: 2, signature: 'dividend, divisor' },
  EXP: { min: 1, max: 1, signature: 'number' },
  LN: { min: 1, max: 1, signature: 'number' },
  LOG10: { min: 1, max: 1, signature: 'number' },
  POWER: { min: 2, max: 2, signature: 'base, exponent' },
  POW: { min: 2, max: 2, signature: 'base, exponent' },
  SQRT: { min: 1, max: 1, signature: 'number' },
  SIN: { min: 1, max: 1, signature: 'radians' },
  COS: { min: 1, max: 1, signature: 'radians' },
  TAN: { min: 1, max: 1, signature: 'radians' },
  COT: { min: 1, max: 1, signature: 'radians' },
  ASIN: { min: 1, max: 1, signature: 'number' },
  ACOS: { min: 1, max: 1, signature: 'number' },
  ATAN: { min: 1, max: 2, signature: 'number' },
  ATAN2: { min: 2, max: 2, signature: 'y, x' },
  DEGREES: { min: 1, max: 1, signature: 'radians' },
  RADIANS: { min: 1, max: 1, signature: 'degrees' },
  PI: { min: 0, max: 0, signature: '' },

  // JSON functions
  JSON_VALUE: { min: 2, max: 2, signature: 'expression, path' },
  JSON_QUERY: { min: 2, max: 2, signature: 'expression, path' },
  JSON_EXTRACT: { min: 2, max: 2, signature: 'expression, path' },
  JSON_OBJECT: { min: 0, signature: 'key1, value1[, ...]' },
  JSON_ARRAY_LENGTH: { min: 1, max: 1, signature: 'array' },
  JSON_VALID: { min: 1, max: 1, signature: 'value' },
  JSON_TYPE: { min: 1, max: 1, signature: 'value' },
  JSON_ARRAYAGG: { min: 1, max: 1, signature: 'expression' },
  ARRAY_AGG: { min: 1, max: 1, signature: 'expression' },

  // Array functions
  ARRAY_LENGTH: { min: 1, max: 2, signature: 'array[, dimension]' },
  ARRAY_POSITION: { min: 2, max: 2, signature: 'array, element' },
  ARRAY_CONTAINS: { min: 2, max: 2, signature: 'array, element' },
  ARRAY_SORT: { min: 1, max: 1, signature: 'array' },
  CARDINALITY: { min: 1, max: 1, signature: 'array' },

  // Table functions (used in FROM clause)
  UNNEST: { min: 1, max: 1, signature: 'array' },
  EXPLODE: { min: 1, max: 1, signature: 'array' },
  JSON_EACH: { min: 1, max: 1, signature: 'value' },

  // Conditional functions
  COALESCE: { min: 1, signature: 'value1, value2[, ...]' },
  NULLIF: { min: 2, max: 2, signature: 'value1, value2' },
  GREATEST: { min: 1, signature: 'value1[, value2, ...]' },
  LEAST: { min: 1, signature: 'value1[, value2, ...]' },

  // Aggregate functions
  COUNT: { min: 1, max: 1, signature: 'expression' },
  COUNTIF: { min: 1, max: 1, signature: 'condition' },
  SUM: { min: 1, max: 1, signature: 'expression' },
  AVG: { min: 1, max: 1, signature: 'expression' },
  MIN: { min: 1, max: 1, signature: 'expression' },
  MAX: { min: 1, max: 1, signature: 'expression' },
  STDDEV_SAMP: { min: 1, max: 1, signature: 'expression' },
  STDDEV_POP: { min: 1, max: 1, signature: 'expression' },
  MEDIAN: { min: 1, max: 1, signature: 'expression' },
  PERCENTILE_CONT: { min: 2, max: 2, signature: 'fraction, expression' },
  APPROX_QUANTILE: { min: 2, max: 2, signature: 'expression, fraction' },
  STRING_AGG: { min: 2, max: 2, signature: 'expression, separator' },

  // Window functions
  ROW_NUMBER: { min: 0, max: 0, signature: '' },

  // Spatial functions
  ST_INTERSECTS: { min: 2, max: 2, signature: 'geometry, geometry' },
  ST_CONTAINS: { min: 2, max: 2, signature: 'geometry, geometry' },
  ST_CONTAINSPROPERLY: { min: 2, max: 2, signature: 'geometry, geometry' },
  ST_WITHIN: { min: 2, max: 2, signature: 'geometry, geometry' },
  ST_OVERLAPS: { min: 2, max: 2, signature: 'geometry, geometry' },
  ST_TOUCHES: { min: 2, max: 2, signature: 'geometry, geometry' },
  ST_EQUALS: { min: 2, max: 2, signature: 'geometry, geometry' },
  ST_CROSSES: { min: 2, max: 2, signature: 'geometry, geometry' },
  ST_COVERS: { min: 2, max: 2, signature: 'geometry, geometry' },
  ST_COVEREDBY: { min: 2, max: 2, signature: 'geometry, geometry' },
  ST_DWITHIN: { min: 3, max: 3, signature: 'geometry, geometry, distance' },
  ST_GEOMFROMTEXT: { min: 1, max: 1, signature: 'wkt' },
  ST_MAKEENVELOPE: { min: 4, max: 4, signature: 'xmin, ymin, xmax, ymax' },
  ST_ASTEXT: { min: 1, max: 1, signature: 'geometry' },
}

/**
 * Validates function argument count, throwing a ParseError if invalid.
 * @param {string} funcName - The function name (uppercase)
 * @param {number} argCount - Number of arguments provided
 * @param {number} positionStart - Start position in query
 * @param {number} positionEnd - End position in query
 * @param {Record<string, UserDefinedFunction>} [functions] - User-defined functions
 * @throws {ParseError}
 */
export function validateFunctionArgs(funcName, argCount, positionStart, positionEnd, functions) {
  // Check built-in functions
  let spec = FUNCTION_SIGNATURES[funcName]

  // Check user-defined functions (case-insensitive)
  if (!spec && functions) {
    const udfName = Object.keys(functions).find(k => k.toUpperCase() === funcName)
    if (udfName) {
      spec = functions[udfName].arguments
    }
  }

  if (!spec) {
    throw new UnknownFunctionError({ funcName, positionStart, positionEnd })
  }
  const { min, max, signature } = spec

  if (argCount < min || max != null && argCount > max) {
    let expected = ''
    if (max == null) expected += `at least ${min} argument`
    else if (min !== max) expected += `${min}-${max} arguments`
    else {
      expected += `${min} argument`
      if (min !== 1) expected += 's'
    }

    throw new ParseError({
      message: `${funcName}(${signature ?? ''}) function requires ${expected}, got ${argCount}`,
      positionStart,
      positionEnd,
    })
  }
}

/**
 * Checks if a function is known (either built-in or user-defined).
 * @param {string} funcName - The function name (uppercase)
 * @param {Record<string, UserDefinedFunction>} [functions] - User-defined functions
 * @returns {boolean}
 */
export function isKnownFunction(funcName, functions) {
  // Check built-in functions
  if (FUNCTION_SIGNATURES[funcName]) return true

  // Check user-defined functions (case-insensitive)
  if (functions) {
    return Object.keys(functions).some(k => k.toUpperCase() === funcName)
  }

  return false
}
