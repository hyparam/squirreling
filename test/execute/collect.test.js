import { describe, expect, it } from 'vitest'
import { collect } from '../../src/index.js'

/**
 * @import { AsyncRow, QueryResults, SqlPrimitive } from '../../src/types.js'
 */

describe('collect', () => {
  it('keeps row order across resolved and lazy rows', async () => {
    /** @type {AsyncRow[]} */
    const source = [
      { columns: ['a'], cells: { a: () => { throw new Error('resolved row read through cells') } }, resolved: { a: 1 } },
      { columns: ['a'], cells: { a: () => Promise.resolve(2) } },
      { columns: ['a'], cells: { a: () => { throw new Error('resolved row read through cells') } }, resolved: { a: 3 } },
    ]
    /** @type {QueryResults} */
    const results = {
      columns: ['a'],
      async *rows() {
        yield* source
      },
    }
    expect(await collect(results)).toEqual([{ a: 1 }, { a: 2 }, { a: 3 }])
  })

  it('materializes a resolved row before the next row is pulled', async () => {
    /** @type {Record<string, number>} */
    const resolved = { a: 1 }
    /** @type {QueryResults} */
    const results = {
      columns: ['a'],
      async *rows() {
        yield { columns: ['a'], cells: { a: () => Promise.resolve(resolved.a) }, resolved }
        // A source reusing its buffer must not change an already collected row
        resolved.a = 2
        yield { columns: ['a'], cells: { a: () => Promise.resolve(resolved.a) }, resolved: { a: 2 } }
      },
    }
    expect(await collect(results)).toEqual([{ a: 1 }, { a: 2 }])
  })

  it('starts lazy cell reads together and preserves order when they finish in reverse', async () => {
    /** @type {(() => void)[]} */
    const finish = []
    /** @type {number[]} */
    const reads = []
    /** @type {QueryResults} */
    const results = {
      columns: ['a', 'b'],
      async *rows() {
        for (let row = 0; row < 3; row++) {
          yield {
            columns: ['a', 'b'],
            cells: Object.fromEntries(['a', 'b'].map((column, index) => [column, () => {
              const value = row * 2 + index
              reads.push(value)
              return new Promise(resolve => {
                finish.push(() => resolve(value))
                if (finish.length === 6) {
                  for (const complete of finish.reverse()) complete()
                }
              })
            }])),
          }
        }
      },
    }
    expect(await collect(results)).toEqual([{ a: 0, b: 1 }, { a: 2, b: 3 }, { a: 4, b: 5 }])
    expect(reads).toEqual([0, 1, 2, 3, 4, 5])
  })

  it('copies only output columns and preserves SQL values on both row paths', async () => {
    /** @type {Record<string, SqlPrimitive>} */
    const values = { a: null, b: 0, c: false, d: '', e: 42n, f: new Date(0), g: [1, null], h: { nested: true } }
    const columns = Object.keys(values)
    /** @type {QueryResults} */
    const results = {
      columns,
      async *rows() {
        yield { columns, cells: {}, resolved: { ...values, hidden: 'filter-only value' } }
        yield { columns, cells: Object.fromEntries(Object.entries(values).map(([key, value]) => [key, () => Promise.resolve(value)])) }
      },
    }
    expect(await collect(results)).toEqual([values, values])
  })

  it('handles empty results and zero-column rows', async () => {
    /** @type {QueryResults} */
    const empty = { columns: [], async *rows() {} }
    expect(await collect(empty)).toEqual([])
    /** @type {QueryResults} */
    const results = {
      columns: [],
      async *rows() {
        yield { columns: [], cells: {}, resolved: {} }
        yield { columns: [], cells: {} }
      },
    }
    expect(await collect(results)).toEqual([{}, {}])
  })

  it.each([false, true])('propagates cell failures (synchronous: %s)', async synchronous => {
    /** @type {QueryResults} */
    const results = {
      columns: ['a'],
      async *rows() {
        yield { columns: ['a'], cells: {}, resolved: { a: 1 } }
        yield {
          columns: ['a'],
          cells: { a: () => {
            if (synchronous) throw new Error('cell failed')
            return Promise.reject(new Error('cell failed'))
          } },
        }
      },
    }
    await expect(collect(results)).rejects.toThrow('cell failed')
  })

  it('propagates source failures and runs source cleanup', async () => {
    let cleaned = false
    let reads = 0
    /** @type {QueryResults} */
    const results = {
      columns: ['a'],
      async *rows() {
        try {
          yield { columns: ['a'], cells: { a: () => { reads++
            return Promise.resolve(1) } } }
          throw new Error('source failed')
        } finally {
          cleaned = true
        }
      },
    }
    await expect(collect(results)).rejects.toThrow('source failed')
    expect(cleaned).toBe(true)
    expect(reads).toBe(0)
  })

  it('prefers batches and respects selected row order', async () => {
    /** @type {QueryResults} */
    const results = {
      columns: ['a'],
      rows() { throw new Error('row fallback used') },
      async *batches() {
        yield {
          columns: [{ type: 'values', values: [10, 20, 30], length: 3 }],
          selection: { type: 'indices', indices: new Uint32Array([2, 0]), length: 3 },
        }
      },
    }
    expect(await collect(results)).toEqual([{ a: 30 }, { a: 10 }])
  })

})
