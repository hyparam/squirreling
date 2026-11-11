import { describe, expect, it } from 'vitest'
import { collect } from '../../src/index.js'

/**
 * @import { AsyncRow, QueryResults } from '../../src/types.js'
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
})
