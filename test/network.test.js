import { asyncBufferFromUrl, parquetMetadataAsync } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { createParquetSource } from '../src/backend/parquet.js'
import { executeSql } from '../src/execute/execute.js'
import { collect } from '../src/index.js'
import { countingBuffer } from './helpers.js'

describe('parquet backend', async () => {
  const file = await asyncBufferFromUrl({ url: 'https://s3.hyperparam.app/squirreling/wiki1k.parquet' })
  const metadata = await parquetMetadataAsync(file)

  it('should read a specific row from wikipedia with minimal networks', async () => {
    const counting = countingBuffer(file)
    const wiki = createParquetSource({ file: counting, metadata })
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
    // reads entire second row group
    expect(counting.bytes).toBe(6111314) // 1 row group
    // expect(counting.bytes).toBe(617381) // TODO: 1 text page!
    expect(counting.fetches).toBe(3) // 1 offset index + 1 run of 3 column chunks + 1 page
  })

  it('should read a specific row from wikipedia with minimal networks', async () => {
    const counting = countingBuffer(file)
    const wiki = createParquetSource({ file: counting, metadata })
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
})
