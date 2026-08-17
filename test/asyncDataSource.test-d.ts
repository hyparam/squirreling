import type { AsyncDataSource, PrepareScan, ScanResults } from '../src/types.js'
import type { ReadColumn } from '../src/index.js'

declare const prepareScan: PrepareScan
declare const readColumn: ReadColumn
declare const scan: () => ScanResults

const legacySource: AsyncDataSource = { columns: ['id'], scan }
const preparedSource: AsyncDataSource = {
  schema: { fields: [] },
  prepareScan,
}

// @ts-expect-error A data source must implement a usable scan contract.
const emptySource: AsyncDataSource = {}
// @ts-expect-error prepareScan requires a schema.
const prepareOnlySource: AsyncDataSource = { prepareScan }

void [legacySource, preparedSource, emptySource, prepareOnlySource, readColumn]
