import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { asyncRow } from './dataSource.js'
import { whereToParquetFilter } from './parquetFilter.js'

/**
 * @import { AsyncBuffer, FileMetaData, ParquetQueryFilter } from 'hyparquet'
 * @import { AsyncDataSource, QueryHints } from '../types.js'
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
     * @param {QueryHints} [hints]
     */
    async *getRows(hints) {
      metadata ??= await parquetMetadataAsync(file)

      // Convert WHERE AST to hyparquet filter format
      /** @type {ParquetQueryFilter | undefined} */
      const filter = hints?.where ? whereToParquetFilter(hints.where) : undefined

      // Emit rows by row group
      let groupStart = 0
      for (const rowGroup of metadata.row_groups) {
        const rowCount = Number(rowGroup.num_rows)

        // Read objects from this row group
        const data = await parquetReadObjects({
          file,
          metadata,
          rowStart: groupStart,
          rowEnd: groupStart + rowCount,
          columns: hints?.columns,
          filter,
          compressors,
        })

        // Yield each row
        for (const row of data) {
          yield asyncRow(row)
        }

        groupStart += rowCount
      }
    },
  }
}
