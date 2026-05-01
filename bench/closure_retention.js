// Closure-hygiene retention benchmark.
//
// Demonstrates the violations identified in the audit: aggregate, buffered
// window, and sort all stamp output cell closures inside a scope that also
// holds the entire input buffer, so a downstream consumer holding a single
// output AsyncRow keeps the whole input alive.
//
// Method:
//   1. Run the query against a wide synthetic source (~30 KB/row).
//   2. Drain it into an array of output AsyncRows (held until measured).
//   3. Drop everything else, force major GC, measure heap.
//
// What that heap number means: "what does *only the output set* pin?"
// Pre-fix the cell closures share a scope with the input buffer, so holding
// any output row pins all of it. After fixing, heap should reflect the size
// of the output rows alone.
//
// Run:
//   node --expose-gc bench/closure_retention.js
//   node --expose-gc bench/closure_retention.js --rows=5000
//
// `--expose-gc` is required — the harness forces a major GC before
// measuring so the reported heap is live retention, not allocation churn.
//
// Numbers (rows=5000, msgs=64, ~12 KB/row live, ~60 MB whole input):
//
//   query                            before fix   after fix
//   MAX aggregate                       58 MB         5 MB
//   ROW_NUMBER OVER (PARTITION BY)      60 MB         7 MB
//   ORDER BY UPPER(error)               16 MB         6 MB
//   hash join (control)                  5 MB         5 MB
//
// "retained" is the heap that survives a forced major GC after dropping
// every reference to the executor and source — i.e. what the output rows
// alone are pinning. The control row was already fixed in an earlier pass.
//
// The fix has two layers:
//   - cell construction goes through src/execute/cells.js helpers
//     (`valueCell`, `expressionCell`, etc.) so closure contexts are bounded
//     to the helper's parameters — never the caller's loop locals.
//   - buffered nodes (aggregate, sort, buffered window) materialize their
//     output up front: aggregates eagerly evaluate the result; sort and
//     window pass each row through `materializeRow` before yielding so the
//     output rows don't carry source-row cell closures.

import { performance } from 'node:perf_hooks'
import { executeSql } from '../src/index.js'

function parseArgs(argv) {
  const out = { rows: 2000, sessions: 50, msgs: 64 }
  for (const a of argv.slice(2)) {
    const m = a.match(/^--(\w+)=(\d+)$/)
    if (m) out[m[1]] = Number(m[2])
  }
  return out
}

/**
 * Synthetic AsyncDataSource that mimics a parquet adapter's per-row retention.
 * Every cell on a row closes over a shared `payload` so any retained cell
 * pins the whole row's decoded state — see DEBUG.md and bench/wide_hash_build.js.
 *
 * @param {object} options
 * @param {number} options.rows
 * @param {number} options.sessions
 * @param {number} options.msgs
 * @returns {import('../src/types.js').AsyncDataSource}
 */
function wideSource({ rows, sessions, msgs }) {
  const allColumns = ['session_id', 'ts', 'error', 'messages', 'tool_calls']
  return {
    numRows: rows,
    columns: allColumns,
    scan({ columns: scanColumns, signal }) {
      const rowColumns = scanColumns ?? allColumns
      return {
        async *rows() {
          for (let i = 0; i < rows; i++) {
            if (signal?.aborted) return
            const messages = new Array(msgs)
            for (let j = 0; j < msgs; j++) {
              messages[j] = { role: j & 1 ? 'user' : 'assistant', content: 'message ' + i + ':' + j + ' lorem ipsum dolor sit amet consectetur' }
            }
            const payload = {
              session_id: 's' + i % sessions,
              ts: i,
              error: i % 5 === 0 ? 'oops_' + i : null,
              messages,
              tool_calls: null,
            }
            /** @type {Record<string, () => Promise<any>>} */
            const cells = {}
            for (const k of rowColumns) {
              cells[k] = () => Promise.resolve(payload[k])
            }
            yield { columns: rowColumns, cells }
          }
        },
        appliedWhere: false,
        appliedLimitOffset: false,
      }
    },
  }
}

function heapMB() {
  return +(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(0)
}

function gc() {
  if (global.gc) {
    global.gc({ execution: 'sync', type: 'major' })
    global.gc({ execution: 'sync', type: 'major' })
  }
}

/**
 * Runs `query`, drains it into an array of AsyncRows, drops the executor /
 * source / generator references, forces GC, and reports heap. The reported
 * number is what the *output rows alone* are pinning.
 *
 * @param {string} label
 * @param {string} query
 * @param {{ rows: number, sessions: number, msgs: number }} args
 */
async function measure(label, query, args) {
  // Source is created inside this function so dropping it after the run lets
  // GC reclaim it — only the output rows survive into the measurement.
  let traces = wideSource(args)
  let result = executeSql({ tables: { traces }, query })

  const t0 = performance.now()
  /** @type {import('../src/types.js').AsyncRow[]} */
  const out = []
  for await (const row of result.rows()) {
    out.push(row)
  }
  const dt = performance.now() - t0

  // Drop every reference to the pipeline state. After this only `out` is
  // reachable from this function, plus whatever `out`'s rows transitively pin.
  traces = undefined
  result = undefined

  gc()
  const retained = heapMB()
  console.log(`  ${label.padEnd(36)} rows-out=${String(out.length).padStart(5)}  retained=${retained} MB  ${dt.toFixed(0)} ms`)

  // Now drop `out` too and confirm heap drops back to baseline. If it doesn't,
  // something else outside `out` was leaking.
  out.length = 0
  gc()
}

async function main() {
  const args = parseArgs(process.argv)
  console.log(`# closure-retention, rows=${args.rows}, sessions=${args.sessions}, msgs=${args.msgs}`)
  console.log(`# nominal input pinned per row ≈ 30 KB; whole table ≈ ${(args.rows * 30 / 1024).toFixed(0)} MB`)

  gc()
  console.log(`baseline heap=${heapMB()} MB\n`)

  // Aggregate violation: aggregates.js:42 — output cell closure shares scope
  // with the buffered `group` array.
  await measure(
    'aggregate (MAX over input)',
    'SELECT MAX(ts) AS m FROM traces',
    args
  )

  // Buffered window violation: window.js:81 — output cells stamped while the
  // full `rows` buffer is in scope.
  await measure(
    'buffered window (PARTITION BY)',
    'SELECT session_id, ROW_NUMBER() OVER (PARTITION BY session_id) AS rn FROM traces',
    args
  )

  // Sort violation: sort.js:77 — derived ORDER BY stamps a cached cell whose
  // closure pins the sort buffer. Need a derived expression so the alias
  // isn't already in row.cells.
  await measure(
    'sort by UPPER(error)',
    'SELECT session_id, ts FROM traces WHERE error IS NOT NULL ORDER BY UPPER(error)',
    args
  )

  // Control: hash join build, which we already fixed. Should retain only the
  // output rows themselves, not the input.
  await measure(
    'hash join build (control, fixed)',
    'SELECT a.session_id, a.ts FROM traces a JOIN traces b ON a.session_id = b.session_id AND b.ts > a.ts WHERE a.error IS NOT NULL AND b.error IS NOT NULL LIMIT 100',
    args
  )
}

main().catch(e => { console.error(e); process.exit(1) })
