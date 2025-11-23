
/**
 * Wraps an AsyncBuffer to count the number of fetches made
 *
 * @import {AsyncBuffer} from 'hyparquet'
 * @param {AsyncBuffer} asyncBuffer
 * @returns {AsyncBuffer & {fetches: number, bytes: number}}
 */
export function countingBuffer(asyncBuffer) {
  return {
    ...asyncBuffer,
    fetches: 0,
    bytes: 0,
    slice(start, end) {
      this.fetches++
      this.bytes += (end ?? asyncBuffer.byteLength) - start
      return asyncBuffer.slice(start, end)
    },
  }
}
