import { asyncBufferFromUrl, parquetMetadataAsync } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { describe, expect, it } from 'vitest'
import { parquetDataSource } from '../../src/backend/parquetDataSource.js'
import { executeSql } from '../../src/execute/execute.js'
import { collect } from '../../src/index.js'
import { countingBuffer } from '../helpers.js'

describe('parquet backend', async () => {
  const file = await asyncBufferFromUrl({ url: 'https://s3.hyperparam.app/squirreling/wiki1k.parquet' })
  const metadata = await parquetMetadataAsync(file)

  it('should read a specific row from wikipedia with minimal networks', async () => {
    const counting = countingBuffer(file)
    const wiki = parquetDataSource(counting, metadata, compressors)
    const result = await collect(executeSql({
      tables: { wiki },
      query: 'SELECT * FROM wiki LIMIT 1 OFFSET 784',
    }))
    expect(result).toEqual([
      {
        id: 2039n,
        title: 'Avionics',
        url: 'https://en.wikipedia.org/wiki/Avionics',
        text: expect.any(String),
      },
    ])
    expect(counting.bytes).toBe(617381) // 1 text page
    expect(counting.fetches).toBe(3) // 1 offset index + 1 run of 3 column chunks + 1 page
  })

  it('should read a specific row from wikipedia with minimal networks', async () => {
    const counting = countingBuffer(file)
    const wiki = parquetDataSource(counting, metadata, compressors)
    const result = await collect(executeSql({
      tables: { wiki },
      query: 'SELECT * FROM wiki WHERE id = 2039',
    }))
    expect(result).toEqual([
      {
        id: 2039n,
        title: 'Avionics',
        url: 'https://en.wikipedia.org/wiki/Avionics',
        text: expect.any(String),
      },
    ])
    // reads entire second row group
    expect(counting.bytes).toBe(6111314)
    // expect(counting.bytes).toBe(617381) // TODO: 1 text page!
    expect(counting.fetches).toBe(3) // 1 offset index + 1 run of 3 column chunks + 1 page
  })

  it('should respect limit across row group boundaries', async () => {
    // wiki1k.parquet has 2 row groups of 500 rows each
    // Test the source directly to verify limit is tracked across row groups
    // LIMIT 2 OFFSET 499 should yield: 499 empty rows + 1 real row from group 0, then 1 real row from group 1
    // Bug: without tracking remaining limit, source yields 499 empty + 1 real + 2 real = 3 real rows
    const wiki = parquetDataSource(file, metadata, compressors)
    const rows = []
    for await (const row of wiki.scan({ limit: 2, offset: 499 }).rows) {
      rows.push(row)
    }
    // Should yield exactly 2 real rows
    expect(rows).toHaveLength(2)
  })
})
