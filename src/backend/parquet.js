import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { asyncRow } from './dataSource.js'
import { whereToParquetFilter } from './parquetFilter.js'

/**
 * @import { AsyncBuffer, FileMetaData, ParquetQueryFilter } from 'hyparquet'
 * @import { AsyncDataSource, ScanOptions } from '../types.js'
 */

/**
 * Creates a parquet-backed async data source
 *
 * @param {Object} options
 * @param {AsyncBuffer} options.file - path to parquet file
 * @param {FileMetaData} [options.metadata] - optional parquet metadata object
 * @returns {AsyncDataSource} a data source interface
 */
export function createParquetSource({ file, metadata }) {
  return {
    /**
     * @param {ScanOptions} options
     */
    async *scan({ hints, signal }) {
      metadata ??= await parquetMetadataAsync(file)

      // Convert WHERE AST to hyparquet filter format
      const whereFilter = hints?.where && whereToParquetFilter(hints.where)
      /** @type {ParquetQueryFilter | undefined} */
      const filter = hints?.where ? whereFilter : undefined
      const filterApplied = !filter || whereFilter

      // Emit rows by row group
      let groupStart = 0
      let remainingLimit = hints?.limit ?? Infinity
      for (const rowGroup of metadata.row_groups) {
        if (signal?.aborted) break
        const rowCount = Number(rowGroup.num_rows)

        // Skip row groups by offset if where is fully applied
        let safeOffset = 0
        let safeLimit = rowCount
        if (filterApplied) {
          if (hints?.offset !== undefined && groupStart < hints.offset) {
            safeOffset = Math.min(rowCount, hints.offset - groupStart)
          }
          safeLimit = Math.min(rowCount - safeOffset, remainingLimit)
          if (safeLimit <= 0 && safeOffset < rowCount) break
        }
        for (let i = 0; i < safeOffset; i++) {
          // yield empty rows
          yield asyncRow({})
        }
        if (safeOffset === rowCount) {
          groupStart += rowCount
          continue
        }

        // Read objects from this row group
        const data = await parquetReadObjects({
          file,
          metadata,
          rowStart: groupStart + safeOffset,
          rowEnd: groupStart + safeOffset + safeLimit,
          columns: hints?.columns,
          filter,
          filterStrict: false,
          compressors,
          useOffsetIndex: true,
        })

        // Yield each row
        for (const row of data) {
          yield asyncRow(row)
        }

        remainingLimit -= data.length
        groupStart += rowCount
      }
    },
  }
}
