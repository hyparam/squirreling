import { expect, it, vi } from 'vitest'
import { addJoinRow, findJoinRows, joinIndex } from '../../src/execute/joinIndex.js'

/** @import { AsyncRow } from '../../src/types.js' */

it('looks up Date keys by timestamp without revisiting build Dates', () => {
  const index = joinIndex()
  /** @type {AsyncRow} */
  const row = { columns: [], cells: {} }
  const dates = Array.from({ length: 1000 }, (_, i) => new Date(i))
  const reads = dates.map(date => vi.spyOn(date, 'getTime'))
  for (const date of dates) addJoinRow(index, [date], row)
  for (const read of reads) read.mockClear()
  for (let i = 0; i < dates.length; i++) {
    expect([...findJoinRows(index, [new Date(i)])]).toEqual([row])
  }
  for (const read of reads) expect(read).not.toHaveBeenCalled()
})

it('preserves Date equality with strings and rejects invalid Dates and numbers', () => {
  const index = joinIndex()
  const dateRow = { columns: ['date'], cells: {} }
  const stringRow = { columns: ['string'], cells: {} }
  const numberRow = { columns: ['number'], cells: {} }
  const invalidRow = { columns: ['invalid'], cells: {} }
  addJoinRow(index, [new Date(0)], dateRow)
  addJoinRow(index, ['1970-01-01T00:00:00.000Z'], stringRow)
  addJoinRow(index, [0], numberRow)
  addJoinRow(index, [new Date(NaN)], invalidRow)
  expect([...findJoinRows(index, [new Date(0)])]).toEqual([dateRow, stringRow])
  expect([...findJoinRows(index, ['1970-01-01T00:00:00.000Z'])]).toEqual([stringRow, dateRow])
  expect([...findJoinRows(index, [new Date(NaN)])]).toEqual([])
  expect([...findJoinRows(index, [0])]).toEqual([numberRow])
})
