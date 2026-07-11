/**
 * Normalizes scanColumn's legacy AsyncIterable return into ScanColumnResults.
 * Legacy implementations are valid for unfiltered scans only: they cannot
 * claim to have applied a WHERE predicate they predate.
 *
 * @param {AsyncIterable<ArrayLike<import('../types.js').SqlPrimitive>> | import('../types.js').ScanColumnResults} result
 * @param {import('../types.js').ScanColumnOptions} options
 * @returns {import('../types.js').ScanColumnResults}
 */
export function normalizeScanColumnResult(result, options) {
  if ('chunks' in result) return result
  return {
    chunks: () => result,
    appliedWhere: !options.where,
    appliedLimitOffset: !options.where,
  }
}
