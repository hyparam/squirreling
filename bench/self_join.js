// Self-join scaling benchmark.
//
// Focused on join shapes the planner CAN'T reduce to a hash join — those are
// where scaling actually varies. The post-3aa3c28 hash join makes any
// AND-of-equi self-join near-linear in output pairs, so benchmarking that
// shape is just measuring hash-join constant factors. The cases below all
// fall back to NestedLoopJoin and are genuinely O(n^2).
//
// Run from the squirreling repo root:
//   node bench/self_join.js
//   node bench/self_join.js --max=3200
//
// Expected: each case roughly 4x time per 2x N (O(n^2)).
//
// Cases:
//   range     equi key + range predicate — hash join, control case
//   ineq      pure inequality on id — NestedLoopJoin
//   or_equi   OR of two equi conditions — NestedLoopJoin
//   triple    three-way self-join on equi key — hash joins chained

import { performance } from 'node:perf_hooks'
import { collect, executeSql } from '../src/index.js'

function parseArgs(argv) {
  const out = { max: 1600, sessions: 8 }
  for (const a of argv.slice(2)) {
    const m = a.match(/^--(\w+)=(\d+)$/)
    if (m) out[m[1]] = Number(m[2])
  }
  return out
}

function makeRows(n, sessions) {
  const rows = new Array(n)
  for (let i = 0; i < n; i++) {
    rows[i] = { id: i, session_id: 's' + i % sessions, ts: i, k2: i % 7 }
  }
  return rows
}

async function timeQuery(traces, query) {
  const t0 = performance.now()
  const r = await collect(executeSql({ tables: { traces }, query }))
  const ms = performance.now() - t0
  return { ms, count: r[0]?.n ?? null }
}

const cases = [
  {
    name: 'range',
    query: 'SELECT COUNT(*) AS n FROM traces a JOIN traces b ON a.session_id = b.session_id AND b.ts > a.ts',
  },
  {
    name: 'ineq',
    query: 'SELECT COUNT(*) AS n FROM traces a JOIN traces b ON a.id < b.id',
  },
  {
    name: 'or_equi',
    query: 'SELECT COUNT(*) AS n FROM traces a JOIN traces b ON a.session_id = b.session_id OR a.k2 = b.k2',
  },
  {
    name: 'triple',
    query: 'SELECT COUNT(*) AS n FROM traces a JOIN traces b ON a.session_id = b.session_id JOIN traces c ON b.k2 = c.k2',
  },
]

async function main() {
  const { max, sessions } = parseArgs(process.argv)

  const sizes = []
  for (let n = 100; n <= max; n *= 2) sizes.push(n)

  for (const c of cases) {
    console.log(`\n# ${c.name}: ${c.query}`)
    console.log('n,ms,pairs,ms_per_pair_ns,factor')
    let prev = null
    for (const n of sizes) {
      const traces = makeRows(n, sessions)
      const { ms, count } = await timeQuery(traces, c.query)
      const perPair = ms * 1e6 / Math.max(1, count)
      const factor = prev ? (ms / prev.ms).toFixed(2) + 'x' : ''
      console.log(`${n},${ms.toFixed(0)},${count},${perPair.toFixed(0)},${factor}`)
      prev = { n, ms }
    }
  }
}

main().catch(e => { console.error(e); process.exit(1) })
