import type { AsyncDataSource, QueryResults, ScanOptions, ScanResults } from '../src/index.js'

type Assert<T extends true> = T
type Not<T extends boolean> = T extends true ? false : true
type HasKey<T, K extends PropertyKey> = K extends keyof T ? true : false

type LegacySource = {
  columns: string[]
  scan(options: ScanOptions): ScanResults
}

type QueryResultsHideBatches = Assert<Not<HasKey<QueryResults, 'batches'>>>
type QueryResultsHideSchema = Assert<Not<HasKey<QueryResults, 'schema'>>>
type DataSourcesKeepLegacyContract = Assert<LegacySource extends AsyncDataSource ? true : false>
type DataSourcesHidePreparedScans = Assert<Not<HasKey<AsyncDataSource, 'prepareScan'>>>
