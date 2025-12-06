/**
 * @import { SqlPrimitive, IntervalUnit } from '../types.js'
 */

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

/**
 * Apply an interval to a date
 * @param {SqlPrimitive} dateVal
 * @param {number} value
 * @param {IntervalUnit} unit
 * @param {'+' | '-'} op
 * @returns {string | null}
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
  if (dateVal instanceof Date) return date.toISOString()
  if (String(dateVal).includes('T')) {
    return date.toISOString()
  } else {
    return date.toISOString().split('T')[0]
  }
}
