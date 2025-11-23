import { parquetReadObjects, parquetSchema } from 'hyparquet'
import { parquetReadAsync } from 'hyparquet/src/read.js'
import { assembleAsync } from 'hyparquet/src/rowgroup.js'
import { asyncRow } from './dataSource.js'
import { whereToParquetFilter } from './parquetFilter.js'
import { extractSpatialFilter, rowGroupOverlaps } from './parquetSpatial.js'

/**
 * @import { AsyncBuffer, Compressors, FileMetaData, ParquetQueryFilter } from 'hyparquet'
 * @import { AsyncDataSource } from '../types.js'
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
  const numRows = Number(metadata.num_rows)
  return {
    numRows,
    columns: schema.children.map(c => c.element.name),
    scan(hints) {
      // Convert WHERE AST to hyparquet filter format
      /** @type {ParquetQueryFilter | undefined} */
      const filter = whereToParquetFilter(hints.where)
      const appliedWhere = !hints.where || Boolean(filter)
      const appliedLimitOffset = !hints.where || appliedWhere

      // Extract spatial filter for row group pruning
      const spatialFilter = extractSpatialFilter(hints.where)

      /** @type {number | undefined} */
      let scanRows
      if (!hints.where) {
        scanRows = Math.max(0, Math.min(numRows - (hints.offset ?? 0), hints.limit ?? Infinity))
      }

      return {
        numRows: scanRows,
        async *rows() {
          // Emit rows by row group
          let groupStart = 0
          let remainingLimit = hints.limit ?? Infinity
          for (const rowGroup of metadata.row_groups) {
            if (hints.signal?.aborted) throw new DOMException('Aborted', 'AbortError')
            const rowCount = Number(rowGroup.num_rows)

            // Skip row groups using geospatial statistics
            if (spatialFilter && !rowGroupOverlaps(rowGroup, spatialFilter)) {
              groupStart += rowCount
              continue
            }

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
              useOffsetIndex: safeOffset > 0 || safeLimit < rowCount,
            })

            // Yield each row
            for (const row of data) {
              yield asyncRow(row, Object.keys(row))
            }

            remainingLimit -= data.length
            groupStart += rowCount
          }
        },
        appliedWhere,
        appliedLimitOffset,
      }
    },

    async *scanColumn({ column, limit, offset, signal }) {
      const rowStart = offset ?? 0
      const rowEnd = limit !== undefined ? rowStart + limit : undefined
      const asyncGroups = parquetReadAsync({
        file,
        metadata,
        rowStart,
        rowEnd,
        columns: [column],
        compressors,
      })
      // assemble struct columns
      const schemaTree = parquetSchema(metadata)
      const assembled = asyncGroups.map(arg => assembleAsync(arg, schemaTree))

      for (const rg of assembled) {
        if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
        const { skipped, data } = await rg.asyncColumns[0].data
        if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
        let dataStart = rg.groupStart + skipped
        for (const page of data) {
          const pageRows = page.length
          const selectStart = Math.max(rowStart - dataStart, 0)
          const selectEnd = Math.min((rowEnd ?? Infinity) - dataStart, pageRows)
          if (selectEnd > selectStart) {
            yield selectStart > 0 || selectEnd < pageRows
              ? page.slice(selectStart, selectEnd)
              : page
          }
          dataStart += pageRows
        }
      }
    },
  }
}
