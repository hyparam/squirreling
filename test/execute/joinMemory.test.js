import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { executeSql } from '../../src/execute/execute.js'
import { collect } from '../../src/index.js'

// A small left table joined to a larger right table exercises the hash join
// build-side swap: the engine builds the hash table on the smaller left side
// and probes with the larger right side.
const small = memorySource({
  data: [
    { id: 1, name: 'a' },
    { id: 2, name: 'b' },
    { id: 3, name: 'c' },
  ],
})
const large = memorySource({
  data: [
    { ref: 3, v: 30 },
    { ref: 1, v: 10 },
    { ref: 9, v: 90 },
    { ref: 1, v: 11 },
    { ref: 8, v: 80 },
  ],
})

/**
 * @param {object[]} rows
 * @returns {object[]}
 */
function sorted(rows) {
  return [...rows].sort((a, b) => JSON.stringify(a) < JSON.stringify(b) ? -1 : 1)
}

describe('hash join build-side swap', () => {
  it('inner join with smaller left side', async () => {
    const result = await collect(executeSql({
      tables: { small, large },
      query: 'SELECT s.name, l.v FROM small s JOIN large l ON s.id = l.ref',
    }))
    expect(sorted(result)).toEqual(sorted([
      { name: 'c', v: 30 },
      { name: 'a', v: 10 },
      { name: 'a', v: 11 },
    ]))
  })

  it('left join keeps unmatched rows from the smaller left side', async () => {
    const result = await collect(executeSql({
      tables: { small, large },
      query: 'SELECT s.name, l.v FROM small s LEFT JOIN large l ON s.id = l.ref',
    }))
    expect(sorted(result)).toEqual(sorted([
      { name: 'a', v: 10 },
      { name: 'a', v: 11 },
      { name: 'b', v: null },
      { name: 'c', v: 30 },
    ]))
  })

  it('right join keeps unmatched rows from the larger right side', async () => {
    const result = await collect(executeSql({
      tables: { small, large },
      query: 'SELECT s.name, l.v FROM small s RIGHT JOIN large l ON s.id = l.ref',
    }))
    expect(sorted(result)).toEqual(sorted([
      { name: 'c', v: 30 },
      { name: 'a', v: 10 },
      { name: 'a', v: 11 },
      { name: null, v: 90 },
      { name: null, v: 80 },
    ]))
  })

  it('full join keeps unmatched rows from both sides', async () => {
    const result = await collect(executeSql({
      tables: { small, large },
      query: 'SELECT s.name, l.v FROM small s FULL JOIN large l ON s.id = l.ref',
    }))
    expect(sorted(result)).toEqual(sorted([
      { name: 'a', v: 10 },
      { name: 'a', v: 11 },
      { name: 'b', v: null },
      { name: 'c', v: 30 },
      { name: null, v: 90 },
      { name: null, v: 80 },
    ]))
  })

  it('handles NULL join keys on both sides', async () => {
    const l = [{ k: null, x: 1 }, { k: 2, x: 2 }]
    const r = [{ k: null, y: 1 }, { k: 2, y: 2 }, { k: 2, y: 3 }]
    const result = await collect(executeSql({
      tables: { l, r },
      query: 'SELECT l.x, r.y FROM l FULL JOIN r ON l.k = r.k',
    }))
    expect(sorted(result)).toEqual(sorted([
      { x: 2, y: 2 },
      { x: 2, y: 3 },
      { x: 1, y: null },
      { x: null, y: 1 },
    ]))
  })

  it('applies residual conditions with the swapped build side', async () => {
    const result = await collect(executeSql({
      tables: { small, large },
      query: 'SELECT s.name, l.v FROM small s JOIN large l ON s.id = l.ref AND l.v > 10',
    }))
    expect(sorted(result)).toEqual(sorted([
      { name: 'c', v: 30 },
      { name: 'a', v: 11 },
    ]))
  })
})

describe('positional join streaming', () => {
  it('zips uneven sides with NULL padding', async () => {
    const a = memorySource({ data: [{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }] })
    const b = memorySource({ data: [{ y: 'p' }, { y: 'q' }] })
    const result = await collect(executeSql({
      tables: { a, b },
      query: 'SELECT a.x, b.y FROM a POSITIONAL JOIN b',
    }))
    expect(result).toEqual([
      { x: 1, y: 'p' },
      { x: 2, y: 'q' },
      { x: 3, y: null },
      { x: 4, y: null },
    ])
  })

  it('does not buffer either side', async () => {
    // Sources that fail if more than a few rows are pulled before the first
    // output row is consumed would require true streaming to pass; here we
    // check the join yields output before either side is exhausted.
    let leftPulled = 0
    /** @type {import('../../src/types.js').AsyncDataSource} */
    const lazy = {
      numRows: 1000000,
      columns: ['x'],
      scan() {
        return {
          async *rows() {
            for (let i = 0; i < 1000000; i++) {
              leftPulled = i
              yield { columns: ['x'], cells: { x: () => Promise.resolve(i) } }
            }
          },
          appliedWhere: false,
          appliedLimitOffset: false,
        }
      },
    }
    const b = memorySource({ data: [{ y: 'p' }] })
    const results = executeSql({
      tables: { lazy, b },
      query: 'SELECT lazy.x, b.y FROM lazy POSITIONAL JOIN b',
    })
    const iter = results.rows()
    const first = await iter.next()
    expect(first.done).toBe(false)
    // streaming: only a handful of left rows pulled to produce the first row
    expect(leftPulled).toBeLessThan(10)
    await iter.return(undefined)
  })
})
