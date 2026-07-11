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

            // Yield each row. All rows in a row group share the same schema, so
            // compute the column-name array once and reuse it across every row
            // instead of allocating a fresh Object.keys() array per row (which
            // buffering operators would then retain, one array per row).
            const rowColumns = data.length ? Object.keys(data[0]) : []
            for (const row of data) {
              yield asyncRow(row, rowColumns)
            }

            remainingLimit -= data.length
            groupStart += rowCount
          }
        },
        appliedWhere,
        appliedLimitOffset,
      }
    },

    scanColumn({ column, where, limit, offset, signal }) {
      const filter = whereToParquetFilter(where)
      const appliedWhere = !where || Boolean(filter)
      // Filtered ranges are over matching rows, not physical parquet rows.
      const appliedLimitOffset = !where ||
        appliedWhere && limit === undefined && offset === undefined

      return {
        appliedWhere,
        appliedLimitOffset,
        async *chunks() {
          // The object reader decodes predicate columns, prunes row groups from
          // statistics, and projects filter-only columns away again.
          if (filter) {
            let groupStart = 0
            for (const rowGroup of metadata.row_groups) {
              if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
              const groupEnd = groupStart + Number(rowGroup.num_rows)
              const rows = await parquetReadObjects({
                file,
                metadata,
                rowStart: groupStart,
                rowEnd: groupEnd,
                columns: [column],
                filter,
                filterStrict: false,
                compressors,
              })
              if (rows.length) yield rows.map(row => row[column])
              groupStart = groupEnd
            }
            return
          }

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
    },
  }
}
