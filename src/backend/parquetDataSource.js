import { parquetMetadataAsync, parquetReadObjects, parquetSchema } from 'hyparquet'
import { whereToParquetFilter } from './parquetFilter.js'
import { asyncRow } from './dataSource.js'

/**
 * @import { AsyncBuffer, Compressors, FileMetaData, ParquetQueryFilter } from 'hyparquet'
 * @import { AsyncCells, AsyncDataSource, AsyncRow, ScanOptions, ScanResults, SqlPrimitive } from '../types.js'
 */

/**
 * Creates a parquet data source for use with squirreling SQL engine.
 *
 * @param {AsyncBuffer} file
 * @param {FileMetaData} metadata
 * @param {Compressors} compressors
 * @returns {AsyncDataSource}
 */
export function parquetDataSource(file, metadata, compressors) {
  const schema = parquetSchema(metadata)
  return {
    numRows: Number(metadata.num_rows),
    columns: schema.children.map(c => c.element.name),
    /**
     * @param {ScanOptions} hints
     * @returns {ScanResults}
     */
    scan(hints) {
      // Convert WHERE AST to hyparquet filter format
      const whereFilter = hints.where && whereToParquetFilter(hints.where)
      /** @type {ParquetQueryFilter | undefined} */
      const filter = hints.where ? whereFilter : undefined
      const appliedWhere = Boolean(filter && whereFilter)
      const appliedLimitOffset = !hints.where || appliedWhere

      return {
        rows: (async function* () {
          metadata ??= await parquetMetadataAsync(file)

          // Emit rows by row group
          let groupStart = 0
          let remainingLimit = hints.limit ?? Infinity
          for (const rowGroup of metadata.row_groups) {
            if (hints.signal?.aborted) throw new DOMException('Aborted', 'AbortError')
            const rowCount = Number(rowGroup.num_rows)

            // Skip row groups by offset if where is fully applied
            let safeOffset = 0
            let safeLimit = rowCount
            if (appliedLimitOffset) {
              if (hints.offset !== undefined && groupStart < hints.offset) {
                safeOffset = Math.min(rowCount, hints.offset - groupStart)
              }
              safeLimit = Math.min(rowCount - safeOffset, remainingLimit)
              if (safeLimit <= 0 && safeOffset < rowCount) break
            }
            // no rows from this group, continue to next
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
              columns: hints.columns,
              filter,
              filterStrict: false,
              compressors,
              useOffsetIndex: true,
            })

            // Yield each row
            for (const row of data) {
              yield asyncRow(row, Object.keys(row))
            }

            remainingLimit -= data.length
            groupStart += rowCount
          }
        })(),
        appliedWhere,
        appliedLimitOffset,
      }
    },
  }
}
