// Hash-join build-side retention benchmark.
//
// Reproduces the OOM described in DEBUG.md without needing the parquet file.
// The synthetic AsyncDataSource here emits AsyncRows whose cell closures all
// capture a shared per-row "decoded payload" object — exactly the shape
// DEBUG.md attributes to the parquet adapter (cells indexing into a parent
// struct that owns all column buffers for the row). Holding any one cell on
// a row therefore pins a per-row payload, regardless of which columns were
// actually requested via the scan hints.
//
// The payload is a decoded array of message-like objects (heap-tracked) so
// V8's normal GC handles the pressure — this is what a real parquet
// `messages: array<struct>` column expands to after decoding. Earlier
// drafts used a Uint8Array which V8 tracks externally and doesn't
// pressure GC the same way; that masked the executor's retention.
//
// The hash-join executor (pre-fix) buffers every right row into both
// `rightRows` and the per-key bucket, so each retained row pins its full
// payload via the cell closures. Heap balloons.
//
// Run from the repo root:
//   node --expose-gc bench/wide_hash_build.js
//   node --expose-gc bench/wide_hash_build.js --rows=20000 --msgs=64
//
// `--expose-gc` is required for clean numbers — the harness forces a major
// GC at each sample so the reported peak reflects live retention rather
// than allocation churn.
//
// Phase-split heap (post-fix vs pre-fix, msgs=64):
//
//   rows    build (post)  build (pre)   probe (both)
//   5000    7 MB          58 MB         81 MB
//   10000   10 MB         116 MB        204 MB
//
// Build-side retention is now ~constant in payload size — slim rows hold
// only the requested column values, not the upstream cell closures. The
// probe-side number is separate (each yielded merged row pins its left-side
// AsyncRow until downstream drops the row), and it grows with output
// cardinality regardless of the build-side fix.

import { performance } from 'node:perf_hooks'
import { executeSql } from '../src/index.js'

function parseArgs(argv) {
  const out = { rows: 10000, sessions: 200, msgs: 64 }
  for (const a of argv.slice(2)) {
    const m = a.match(/^--(\w+)=(\d+)$/)
    if (m) out[m[1]] = Number(m[2])
  }
  return out
}

/**
 * Synthetic AsyncDataSource that mimics a parquet adapter's per-row retention.
 *
 * Every row owns a `payload` object holding a `Uint8Array` of `fatBytes`. Every
 * cell closure on the row captures `payload` (not just the value it returns),
 * so any retained cell pins the buffer. Column hints are honored — only the
 * requested columns are exposed as cells — but even one retained cell on a
 * row keeps the whole `payload` alive, matching DEBUG.md's hypothesis.
 *
 * @param {object} options
 * @param {number} options.rows
 * @param {number} options.sessions
 * @param {number} options.msgs - per-row "messages" array length; each entry
 *   is a decoded struct (~150 bytes resident on the heap)
 * @param {(stage: string) => void} options.peek - called from inside the
 *   generator before each yield; lets the harness sample memory at points
 *   where the consumer has actually finished processing the previous row
 * @returns {import('../src/types.js').AsyncDataSource}
 */
function wideParquetLikeSource({ rows, sessions, msgs, peek }) {
  const allColumns = ['session_id', 'ts', 'error', 'messages', 'tool_calls']
  let scanCount = 0
  return {
    numRows: rows,
    columns: allColumns,
    scan({ columns: scanColumns, signal }) {
      const rowColumns = scanColumns ?? allColumns
      const scanId = ++scanCount
      // executeHashJoin requests both sides up front (left first, right
      // second), then iterates the right side for build, then the left for
      // probe. So scanId 1 = left = probe, scanId 2 = right = build.
      const phase = scanId === 2 ? 'build' : 'probe'
      return {
        async *rows() {
          for (let i = 0; i < rows; i++) {
            if (signal?.aborted) return
            if (i % 256 === 0) peek(phase)
            // Per-row decoded payload. Every cell on this row will close over
            // `payload`, so as long as any cell on the AsyncRow is reachable
            // the whole payload (including the messages array) is pinned —
            // same retention shape as a parquet page indirected through a
            // row-level struct.
            const messages = new Array(msgs)
            for (let j = 0; j < msgs; j++) {
              messages[j] = { role: j & 1 ? 'user' : 'assistant', content: 'message ' + i + ':' + j + ' lorem ipsum dolor sit amet' }
            }
            const payload = {
              session_id: 's' + i % sessions,
              ts: i,
              error: i % 5 === 0 ? 'oops' : null,
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

function mem() {
  const { rss, heapUsed } = process.memoryUsage()
  return { rssMB: +(rss / 1024 / 1024).toFixed(0), heapMB: +(heapUsed / 1024 / 1024).toFixed(0) }
}

async function main() {
  const { rows, sessions, msgs } = parseArgs(process.argv)
  console.log(`# wide-hash-build, rows=${rows}, sessions=${sessions}, msgs=${msgs}/row`)

  const traces = wideParquetLikeSource({ rows, sessions, msgs, peek: phase => peek(phase) })

  if (global.gc) global.gc()
  const before = mem()
  console.log(`before: rss=${before.rssMB} MB, heap=${before.heapMB} MB`)

  // Sample heap during the run so we can report peak retention, not just
  // the post-run number (which collapses once the build set is dropped).
  // The source calls `peek` from inside its generator at fixed intervals —
  // by the time control returns there, the consumer has finished its
  // previous round of work, so each sample reflects the live retention at
  // a real synchronization point in the executor.
  // We force GC at each peek so reported peaks reflect live retention, not
  // transient allocation churn. The build phase is what this bench targets;
  // probe-side retention is a separate issue (each yielded merged row pins
  // its leftRow's cells until downstream drops the merged row, so peak grows
  // with output cardinality independent of build).
  /** @type {Record<string, number>} */
  const peakByPhase = { build: before.heapMB, probe: before.heapMB }
  function peek(phase) {
    if (global.gc) global.gc({ execution: 'sync', type: 'major' })
    const heap = mem().heapMB
    if (heap > peakByPhase[phase]) peakByPhase[phase] = heap
  }

  // We sink the join output through a streaming consumer (a per-row counter)
  // rather than COUNT(*) — executeScalarAggregate buffers every input row,
  // which would dominate the peak measurement and obscure the build-side
  // retention this bench is meant to test.
  let pairs = 0
  const results = executeSql({
    tables: { traces },
    query: `
      SELECT a.session_id, a.ts AS at, b.ts AS bt
      FROM traces a
      JOIN traces b ON a.session_id = b.session_id AND b.ts > a.ts
      WHERE a.error IS NOT NULL AND b.error IS NOT NULL
    `,
  })
  const t0 = performance.now()
  for await (const row of results.rows()) {
    // Touch one cell so the row isn't elided; awaiting the cell keeps the
    // shape of how a real consumer would drain.
    await row.cells[row.columns[0]]()
    pairs++
  }
  const result = [{ n: pairs }]
  const dt = performance.now() - t0

  if (global.gc) global.gc()
  const after = mem()
  console.log(`peak:   build heap=${peakByPhase.build} MB, probe heap=${peakByPhase.probe} MB`)
  console.log(`after:  rss=${after.rssMB} MB, heap=${after.heapMB} MB`)
  console.log(`time:   ${dt.toFixed(0)} ms, pairs=${result[0]?.n ?? 'n/a'}`)
}

main().catch(e => { console.error(e); process.exit(1) })
