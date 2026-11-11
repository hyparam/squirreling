import { expect, it as test } from 'vitest'
import { collect, executeSql, streamRowBatches } from '../../src/index.js'
import { bindQuerySignal } from '../../src/execute/utils.js'

/**
 * @import { QueryResults } from '../../src/types.js'
 */

test('native selected batches preserve values without invoking row adapters', async () => {
  /** @type {QueryResults} */
  const result = {
    columns: ['a'],
    rows() { throw new Error('row adapter used') },
    async *batches() {
      yield { columns: [{ type: 'values', length: 3, values: [10, 20, null] }], selection: { type: 'indices', length: 3, indices: new Uint32Array([2, 0]) } }
    },
  }
  expect(await Array.fromAsync(streamRowBatches(result, { batchRows: 1 }))).toEqual([[{ a: null }], [{ a: 10 }]])
})

test('slow consumer and early break do not pull the next batch; cleanup runs', async () => {
  let pulled = 0
  let closed = false
  /** @type {QueryResults} */
  const result = {
    columns: ['a'],
    rows() { throw new Error('row adapter used') },
    async *batches() {
      try {
        for (let i = 0; i < 3; i++) {
          pulled++
          yield { columns: [{ type: 'constant', length: 20, value: i }], selection: { type: 'all', length: 20 } }
        }
      } finally { closed = true }
    },
  }
  const iterator = streamRowBatches(result, { batchRows: 2 })
  expect((await iterator.next()).value).toEqual([{ a: 0 }, { a: 0 }])
  await new Promise(resolve => setImmediate(resolve))
  expect(pulled).toEqual(1)
  await iterator.return(undefined)
  expect(closed).toEqual(true)
  expect(pulled).toEqual(1)
})

test('fallback cell reads batch concurrently, preserve order, and bound read-ahead', async () => {
  let pulled = 0
  let reads = 0
  let closed = false
  /** @type {(() => void)[]} */
  const finish = []
  /** @type {QueryResults} */
  const result = {
    columns: ['a'],
    async *rows() {
      try {
        for (let i = 0; i < 10; i++) {
          pulled++
          yield { columns: ['a'], cells: { a: () => new Promise(resolve => {
            reads++
            finish.push(() => resolve(i))
            if (finish.length === 3) for (const done of finish.reverse()) done()
          }) } }
        }
      } finally { closed = true }
    },
  }
  const iterator = streamRowBatches(result, { batchRows: 3 })
  expect((await iterator.next()).value).toEqual([{ a: 0 }, { a: 1 }, { a: 2 }])
  expect(pulled).toEqual(3)
  expect(reads).toEqual(3)
  await iterator.return(undefined)
  expect(closed).toEqual(true)
})

test('copies a reused resolved buffer before pulling more rows', async () => {
  const resolved = { a: 0, hidden: 'not output' }
  /** @type {QueryResults} */
  const result = {
    columns: ['a'],
    async *rows() {
      for (let i = 0; i < 5; i++) {
        resolved.a = i
        yield { columns: ['a'], cells: {}, resolved }
      }
    },
  }
  expect(await Array.fromAsync(streamRowBatches(result, { batchRows: 2 }))).toEqual([[{ a: 0 }, { a: 1 }], [{ a: 2 }, { a: 3 }], [{ a: 4 }]])
})

test('abort while a consumer is paused rejects before another chunk', async () => {
  const controller = new AbortController()
  let closed = false
  /** @type {QueryResults} */
  const result = {
    columns: ['a'],
    async *rows() {
      try {
        for (let a = 0; a < 10; a++) yield { columns: ['a'], cells: {}, resolved: { a } }
      } finally { closed = true }
    },
  }
  const iterator = streamRowBatches(bindQuerySignal(result, controller.signal), { batchRows: 3 })
  await iterator.next()
  controller.abort(new Error('consumer cancelled'))
  await expect(iterator.next()).rejects.toThrow('consumer cancelled')
  expect(closed).toEqual(true)
})

test('a cell error closes its source and rejects without a partial chunk', async () => {
  let closed = false
  /** @type {QueryResults} */
  const result = {
    columns: ['a'],
    async *rows() {
      try {
        yield { columns: ['a'], cells: { a: () => Promise.reject(new Error('cell failed')) } }
        yield { columns: ['a'], cells: {}, resolved: { a: 2 } }
      } finally { closed = true }
    },
  }
  await expect(Array.fromAsync(streamRowBatches(result, { batchRows: 1 }))).rejects.toThrow('cell failed')
  expect(closed).toEqual(true)
})

test('empty results and invalid chunk sizes', async () => {
  /** @type {QueryResults} */
  const result = { columns: [], async *rows() {} }
  expect(await Array.fromAsync(streamRowBatches(result))).toEqual([])
  await expect(Array.fromAsync(streamRowBatches(result, { batchRows: 0 }))).rejects.toThrow('batchRows must be a positive integer')
})


test('native SQL results match collect, including aliases and SQL values', async () => {
  const tables = { t: [{ a: 1, b: null }, { a: 2, b: 'x' }, { a: 3, b: 'y' }] }
  const query = 'SELECT a AS n, b FROM t WHERE a >= 2 ORDER BY a DESC'
  const expected = await collect(executeSql({ tables, query }))
  expect((await Array.fromAsync(streamRowBatches(executeSql({ tables, query }), { batchRows: 1 }))).flat()).toEqual(expected)
})

test('zero-column batches still yield the selected number of rows', async () => {
  /** @type {QueryResults} */
  const result = {
    columns: [],
    async *rows() {},
    async *batches() { yield { columns: [], selection: { type: 'all', length: 3 } } },
  }
  expect(await Array.fromAsync(streamRowBatches(result, { batchRows: 2 }))).toEqual([[{}, {}], [{}]])
})

test('mixed rows preserve order across chunks', async () => {
  /** @type {QueryResults} */
  const result = {
    columns: ['a'],
    async *rows() {
      for (let a = 0; a < 7; a++) {
        if (a % 2) yield { columns: ['a'], cells: { a: () => Promise.resolve(a) } }
        else yield { columns: ['a'], cells: {}, resolved: { a } }
      }
    },
  }
  expect(await Array.fromAsync(streamRowBatches(result, { batchRows: 3 }))).toEqual([[{ a: 0 }, { a: 1 }, { a: 2 }], [{ a: 3 }, { a: 4 }, { a: 5 }], [{ a: 6 }]])
})

test('pre-aborted queries do not open sources', async () => {
  const controller = new AbortController()
  controller.abort(new Error('already cancelled'))
  /** @type {QueryResults} */
  const result = { columns: [], rows() { throw new Error('source opened') } }
  await expect(Array.fromAsync(streamRowBatches(bindQuerySignal(result, controller.signal)))).rejects.toThrow('already cancelled')
})

test.each([1024, 1000000])('timer cancellation interrupts a large native batch with chunk size %s', async batchRows => {
  const controller = new AbortController()
  let closed = false
  /** @type {QueryResults} */
  const result = {
    columns: ['a'],
    async *rows() {},
    async *batches() {
      try {
        yield { columns: [{ type: 'constant', value: 1, length: 1000000 }], selection: { type: 'all', length: 1000000 } }
      } finally { closed = true }
    },
  }
  const timer = setTimeout(() => controller.abort(new Error('timed out')), 0)
  try {
    await expect(Array.fromAsync(streamRowBatches(bindQuerySignal(result, controller.signal), { batchRows }))).rejects.toThrow('timed out')
    expect(closed).toEqual(true)
  } finally { clearTimeout(timer) }
})


test.each(['SELECT a FROM t', 'SELECT COUNT(*) FROM t', 'SHOW TABLES'])('query results expose the execution signal directly: %s', async query => {
  const controller = new AbortController()
  const result = executeSql({ tables: { t: [{ a: 1 }] }, query, signal: controller.signal })
  expect(result.signal).toBe(controller.signal)
  controller.abort(new Error('query cancelled'))
  await expect(Array.fromAsync(streamRowBatches(result))).rejects.toThrow('query cancelled')
})

test('collectors honor a signal on caller-created results without binding', async () => {
  for (const consume of [collect, (/** @type {QueryResults} */ result) => Array.fromAsync(streamRowBatches(result))]) {
    const controller = new AbortController()
    /** @type {QueryResults} */
    const result = {
      columns: ['a'],
      signal: controller.signal,
      async *rows() {
        yield { columns: ['a'], cells: { a: () => {
          controller.abort(new Error('cancelled during cell read'))
          return Promise.resolve(1)
        } } }
      },
    }
    await expect(consume(result)).rejects.toThrow('cancelled during cell read')
  }
})


test('count cancellation is observed before handing output to a consumer', async () => {
  const controller = new AbortController()
  const result = executeSql({
    query: 'SELECT COUNT(*) FROM t',
    signal: controller.signal,
    tables: {
      t: {
        columns: ['a'],
        scan() {
          return {
            appliedWhere: false,
            appliedLimitOffset: false,
            async *rows() {
              yield { columns: ['a'], cells: { a: () => Promise.resolve(1) } }
              controller.abort(new Error('count cancelled'))
            },
          }
        },
      },
    },
  })
  await expect(Array.fromAsync(streamRowBatches(result))).rejects.toThrow('count cancelled')
})
