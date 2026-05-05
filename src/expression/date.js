/**
 * @import { SqlPrimitive, IntervalUnit } from '../types.js'
 */

/**
 * Apply an interval to a date
 * @param {SqlPrimitive} dateVal
 * @param {number} value
 * @param {IntervalUnit} unit
 * @param {'+' | '-'} op
 * @returns {Date | string | null}
 */
export function applyIntervalToDate(dateVal, value, unit, op) {
  const date = toDate(dateVal)
  if (date == null) return null

  const multiplier = op === '+' ? 1 : -1
  const adjusted = value * multiplier

  if (unit === 'SECOND') {
    date.setUTCSeconds(date.getUTCSeconds() + adjusted)
  } else if (unit === 'MINUTE') {
    date.setUTCMinutes(date.getUTCMinutes() + adjusted)
  } else if (unit === 'HOUR') {
    date.setUTCHours(date.getUTCHours() + adjusted)
  } else if (unit === 'DAY') {
    date.setUTCDate(date.getUTCDate() + adjusted)
  } else if (unit === 'MONTH') {
    date.setUTCMonth(date.getUTCMonth() + adjusted)
  } else if (unit === 'YEAR') {
    date.setUTCFullYear(date.getUTCFullYear() + adjusted)
  }

  // Return in same format as input
  if (dateVal instanceof Date) return date
  if (String(dateVal).includes('T')) {
    return date.toISOString()
  } else {
    return date.toISOString().split('T')[0]
  }
}

/**
 * Truncate a date to the given precision
 * @param {SqlPrimitive} precision - the unit to truncate to (year, month, day, hour, minute, second)
 * @param {SqlPrimitive} dateVal - the date value to truncate
 * @returns {Date | string | null}
 */
export function dateTrunc(precision, dateVal) {
  if (precision == null || dateVal == null) return null
  const date = toDate(dateVal)
  if (date == null) return null

  const unit = String(precision).toUpperCase()
  if (unit === 'YEAR') {
    date.setUTCMonth(0, 1)
    date.setUTCHours(0, 0, 0, 0)
  } else if (unit === 'MONTH') {
    date.setUTCDate(1)
    date.setUTCHours(0, 0, 0, 0)
  } else if (unit === 'DAY') {
    date.setUTCHours(0, 0, 0, 0)
  } else if (unit === 'HOUR') {
    date.setUTCMinutes(0, 0, 0)
  } else if (unit === 'MINUTE') {
    date.setUTCSeconds(0, 0)
  } else if (unit === 'SECOND') {
    date.setUTCMilliseconds(0)
  }

  // Return in same format as input
  if (dateVal instanceof Date) return date
  if (String(dateVal).includes('T')) {
    return date.toISOString()
  } else {
    return date.toISOString().split('T')[0]
  }
}

/**
 * Extract a field from a date value
 * @param {SqlPrimitive} field - the field to extract (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DOW, EPOCH)
 * @param {SqlPrimitive} dateVal - the date value to extract from
 * @returns {number | null}
 */
export function extractField(field, dateVal) {
  if (field == null || dateVal == null) return null
  const date = toDate(dateVal)
  if (date == null) return null

  const unit = String(field).toUpperCase()
  if (unit === 'YEAR') return date.getUTCFullYear()
  if (unit === 'MONTH') return date.getUTCMonth() + 1
  if (unit === 'DAY') return date.getUTCDate()
  if (unit === 'HOUR') return date.getUTCHours()
  if (unit === 'MINUTE') return date.getUTCMinutes()
  if (unit === 'SECOND') return date.getUTCSeconds()
  if (unit === 'DOW') return date.getUTCDay()
  if (unit === 'EPOCH') return date.getTime() / 1000
  return null
}

/**
 * Compute the number of unit boundaries between two dates (end - start).
 * @param {SqlPrimitive} unit
 * @param {SqlPrimitive} startVal
 * @param {SqlPrimitive} endVal
 * @returns {number | null}
 */
export function dateDiff(unit, startVal, endVal) {
  if (unit == null || startVal == null || endVal == null) return null
  const start = toDate(startVal)
  const end = toDate(endVal)
  if (start == null || end == null) return null

  const u = String(unit).toUpperCase()
  if (u === 'YEAR') return end.getUTCFullYear() - start.getUTCFullYear()
  if (u === 'MONTH') {
    return (end.getUTCFullYear() - start.getUTCFullYear()) * 12 + (end.getUTCMonth() - start.getUTCMonth())
  }
  const ms = end.getTime() - start.getTime()
  if (u === 'DAY') return Math.trunc(ms / 86400000)
  if (u === 'HOUR') return Math.trunc(ms / 3600000)
  if (u === 'MINUTE') return Math.trunc(ms / 60000)
  if (u === 'SECOND') return Math.trunc(ms / 1000)
  return null
}

/**
 * @param {SqlPrimitive} val
 * @returns {Date | null}
 */
function toDate(val) {
  if (val instanceof Date) return val
  const dateOrTime = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?/
  if (typeof val === 'string' && dateOrTime.test(val)) {
    const date = new Date(val)
    if (!isNaN(date.getTime())) {
      return date
    }
  }
  return null
}
