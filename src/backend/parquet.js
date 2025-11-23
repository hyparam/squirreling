import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { createRowAccessor } from './memory.js'

/**
 * @import { AsyncBuffer, FileMetaData } from 'hyparquet'
 * @import { AsyncDataSource } from '../types.js'
 */

/**
 * Creates a parquet-backed async data source
 *
 * @param {Object} options
 * @param {AsyncBuffer} options.file - path to parquet file
 * @param {FileMetaData} [options.metadata] - optional parquet metadata object
 * @param {string[]} [options.columns] - optional column names to read (for projection)
 * @returns {AsyncDataSource} a data source interface
 */
export function createParquetSource({ file, metadata, columns }) {
  return {
    async *getRows() {
      metadata ??= await parquetMetadataAsync(file)

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
          columns,
          // filters, // TODO: filters for predicate pushdown
          compressors,
        })

        // Yield each row
        for (let j = 0; j < rowCount; j++) {
          yield createRowAccessor(data[j])
        }

        groupStart += rowCount
      }
    },
  }
}
