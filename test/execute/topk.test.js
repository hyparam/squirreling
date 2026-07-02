import { describe, expect, it } from 'vitest'
import { memorySource } from '../../src/backend/dataSource.js'
import { executeSql } from '../../src/execute/execute.js'
import { collect } from '../../src/index.js'
import { planSql } from '../../src/plan/plan.js'
import { parseSql } from '../../src/parse/parse.js'

// 5000 rows forces multiple truncation rounds through the 1024-entry buffer
const N = 5000
const id = new Array(N)
const v = new Array(N)
for (let i = 0; i < N; i++) {
  id[i] = i
  v[i] = i * 7919 % N // deterministic shuffle of 0..N-1
}
const big = memorySource({ data: id.map(i => ({ id: i, v: v[i] })) })

describe('top-k sort', () => {
  it('pushes LIMIT into the Sort node through Project', () => {
    const query = parseSql({ query: 'SELECT id FROM big ORDER BY v LIMIT 5 OFFSET 2' })
    const plan = planSql({ query, tables: { big } })
    expect(plan.type).toBe('Limit')
    if (plan.type !== 'Limit') throw new Error('expected Limit')
    expect(plan.child.type).toBe('Project')
    if (plan.child.type !== 'Project') throw new Error('expected Project')
    expect(plan.child.child).toEqual({
      type: 'Sort',
      orderBy: [expect.any(Object)],
      topK: 7,
      child: expect.any(Object),
    })
  })

  it('does not push LIMIT into Sort through Distinct', () => {
    const query = parseSql({ query: 'SELECT DISTINCT id FROM big ORDER BY id LIMIT 5' })
    const plan = planSql({ query, tables: { big } })
    expect(JSON.stringify(plan)).not.toContain('"topK"')
  })

  it('sorts with LIMIT across truncation rounds', async () => {
    const result = await collect(executeSql({
      tables: { big },
      query: 'SELECT id, v FROM big ORDER BY v LIMIT 5',
    }))
    const expected = id
      .map(i => ({ id: i, v: v[i] }))
      .sort((a, b) => a.v - b.v)
      .slice(0, 5)
    expect(result).toEqual(expected)
  })

  it('sorts descending with LIMIT and OFFSET', async () => {
    const result = await collect(executeSql({
      tables: { big },
      query: 'SELECT v FROM big ORDER BY v DESC LIMIT 3 OFFSET 4',
    }))
    expect(result).toEqual([{ v: N - 5 }, { v: N - 6 }, { v: N - 7 }])
  })

  it('breaks ties by input order like a full sort', async () => {
    const ties = memorySource({
      data: Array.from({ length: 3000 }, (_, i) => ({ id: i, k: i % 2 })),
    })
    const result = await collect(executeSql({
      tables: { ties },
      query: 'SELECT id FROM ties ORDER BY k LIMIT 3',
    }))
    // stable sort keeps the earliest even ids first
    expect(result).toEqual([{ id: 0 }, { id: 2 }, { id: 4 }])
  })

  it('applies DISTINCT correctly when LIMIT is not pushed into Sort', async () => {
    const dupes = memorySource({
      data: Array.from({ length: 3000 }, (_, i) => ({ x: 2999 - i % 10 - i })),
    })
    const result = await collect(executeSql({
      tables: { dupes },
      query: 'SELECT DISTINCT x FROM dupes ORDER BY x LIMIT 3',
    }))
    const expected = [...new Set(dupes.numRows ? Array.from({ length: 3000 }, (_, i) => 2999 - i % 10 - i) : [])]
      .sort((a, b) => a - b)
      .slice(0, 3)
      .map(x => ({ x }))
    expect(result).toEqual(expected)
  })

  it('sorts a UNION with LIMIT', async () => {
    const result = await collect(executeSql({
      tables: { big },
      query: 'SELECT v FROM big UNION ALL SELECT v FROM big ORDER BY v LIMIT 4',
    }))
    expect(result).toEqual([{ v: 0 }, { v: 0 }, { v: 1 }, { v: 1 }])
  })
})
