import { readFileSync } from 'fs'
import { parquetMetadata, parquetReadObjects, parquetSchema } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { asyncRow, executeSql, collect, parseSql } from './src/index.js'

const buffer = readFileSync('0000.parquet')
const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
const metadata = parquetMetadata(arrayBuffer)
const schema = parquetSchema(metadata)

console.log(`File: 0000.parquet`)
console.log(`Rows: ${metadata.num_rows}, Row groups: ${metadata.row_groups.length}`)
console.log(`Columns: ${schema.children.map(c => c.element.name).join(', ')}`)
console.log()

// --- Test 1: Direct count per row group with various filter options ---
console.log('=== Test 1: Direct parquetReadObjects per row group ===')
let groupStart = 0
let totalStrict = 0, totalLoose = 0, totalBigint = 0, totalNoFilter = 0
let totalUOI = 0 // useOffsetIndex

for (const rowGroup of metadata.row_groups) {
  const rowCount = Number(rowGroup.num_rows)

  const [strict, loose, bigint, noFilter, uoi] = await Promise.all([
    parquetReadObjects({ file: arrayBuffer, compressors, metadata,
      rowStart: groupStart, rowEnd: groupStart + rowCount,
      columns: ['turns'], filter: { turns: { $eq: 8 } }, filterStrict: true }),
    parquetReadObjects({ file: arrayBuffer, compressors, metadata,
      rowStart: groupStart, rowEnd: groupStart + rowCount,
      columns: ['turns'], filter: { turns: { $eq: 8 } }, filterStrict: false }),
    parquetReadObjects({ file: arrayBuffer, compressors, metadata,
      rowStart: groupStart, rowEnd: groupStart + rowCount,
      columns: ['turns'], filter: { turns: { $eq: 8n } }, filterStrict: true }),
    parquetReadObjects({ file: arrayBuffer, compressors, metadata,
      rowStart: groupStart, rowEnd: groupStart + rowCount,
      columns: ['turns'] }),
    parquetReadObjects({ file: arrayBuffer, compressors, metadata,
      rowStart: groupStart, rowEnd: groupStart + rowCount,
      columns: ['turns'], filter: { turns: { $eq: 8 } }, filterStrict: false,
      useOffsetIndex: true }),
  ])

  if (strict.length || loose.length || bigint.length || uoi.length) {
    console.log(`  RG @${groupStart} (${rowCount} rows): strict=${strict.length} loose=${loose.length} bigint=${bigint.length} noFilter=${noFilter.length} useOffsetIndex=${uoi.length}`)
  }
  totalStrict += strict.length
  totalLoose += loose.length
  totalBigint += bigint.length
  totalNoFilter += noFilter.length
  totalUOI += uoi.length
  groupStart += rowCount
}

console.log(`\nTotals: strict=${totalStrict} loose=${totalLoose} bigint=${totalBigint} total=${totalNoFilter} useOffsetIndex=${totalUOI}`)

// --- Test 2: Simulate workerParquetDataSource exactly ---
console.log('\n=== Test 2: Exact workerParquetDataSource simulation ===')

function whereToParquetFilter(where) {
  if (!where) return undefined
  if (where.type === 'binary') {
    const { op, left, right } = where
    if (op === 'AND') {
      const l = whereToParquetFilter(left)
      const r = whereToParquetFilter(right)
      if (!l || !r) return
      return { $and: [l, r] }
    }
    if (op === 'OR') {
      const l = whereToParquetFilter(left)
      const r = whereToParquetFilter(right)
      if (!l || !r) return
      return { $or: [l, r] }
    }
    if (left.type === 'identifier' && right.type === 'literal') {
      const opMap = { '=': '$eq', '!=': '$ne', '<': '$lt', '<=': '$lte', '>': '$gt', '>=': '$gte' }
      if (opMap[op]) return { [left.name]: { [opMap[op]]: right.value } }
    }
    if (left.type === 'literal' && right.type === 'identifier') {
      const flipMap = { '<': '$gt', '<=': '$gte', '>': '$lt', '>=': '$lte', '=': '$eq', '!=': '$ne' }
      if (flipMap[op]) return { [right.name]: { [flipMap[op]]: left.value } }
    }
  }
  return undefined
}

function workerParquetDataSource(file, metadata) {
  const schema = parquetSchema(metadata)
  return {
    numRows: Number(metadata.num_rows),
    columns: schema.children.map(c => c.element.name),
    scan(hints) {
      const whereFilter = hints.where && whereToParquetFilter(hints.where)
      const filter = hints.where ? whereFilter : undefined
      const appliedWhere = Boolean(filter && whereFilter)
      const appliedLimitOffset = !hints.where || appliedWhere

      console.log(`  scan() called:`)
      console.log(`    columns: ${JSON.stringify(hints.columns)}`)
      console.log(`    where: ${JSON.stringify(hints.where?.op)} ${JSON.stringify(hints.where?.right?.value)}`)
      console.log(`    limit: ${hints.limit}, offset: ${hints.offset}`)
      console.log(`    filter: ${JSON.stringify(filter)}`)
      console.log(`    appliedWhere: ${appliedWhere}, appliedLimitOffset: ${appliedLimitOffset}`)

      return {
        rows: (async function* () {
          let groupStart = 0
          let remainingLimit = hints.limit ?? Infinity
          for (const rowGroup of metadata.row_groups) {
            if (hints.signal?.aborted) throw new DOMException('Aborted', 'AbortError')
            const rowCount = Number(rowGroup.num_rows)

            let safeOffset = 0
            let safeLimit = rowCount
            if (appliedLimitOffset) {
              if (hints.offset !== undefined && groupStart < hints.offset) {
                safeOffset = Math.min(rowCount, hints.offset - groupStart)
              }
              safeLimit = Math.min(rowCount - safeOffset, remainingLimit)
              if (safeLimit <= 0 && safeOffset < rowCount) {
                console.log(`    RG @${groupStart}: BREAK (safeLimit=${safeLimit}, safeOffset=${safeOffset})`)
                break
              }
            }
            if (safeOffset === rowCount) {
              groupStart += rowCount
              continue
            }

            const data = await parquetReadObjects({
              file,
              compressors,
              metadata,
              rowStart: groupStart + safeOffset,
              rowEnd: groupStart + safeOffset + safeLimit,
              columns: hints.columns,
              filter,
              filterStrict: false,
              useOffsetIndex: true,
            })

            console.log(`    RG @${groupStart}: requested=${safeLimit}, got=${data.length}, remainingLimit=${remainingLimit}`)

            for (const row of data) {
              yield asyncRow(row, Object.keys(row))
            }

            remainingLimit -= data.length
            groupStart += rowCount
          }
        })(),
        appliedWhere,
        appliedLimitOffset,
      }
    },
  }
}

const table = workerParquetDataSource(arrayBuffer, metadata)
const ast = parseSql({ query: 'SELECT COUNT(*) as cnt FROM table WHERE turns = 8' })
const results = await collect(executeSql({ tables: { table }, query: ast }))
console.log('\nResult:', results)

// --- Test 3: Simulate AsyncBuffer (chunked reads like HTTP range requests) ---
console.log('\n=== Test 3: AsyncBuffer simulation (like browser HTTP range requests) ===')

const asyncBuffer = {
  byteLength: arrayBuffer.byteLength,
  slice(start, end) {
    return Promise.resolve(arrayBuffer.slice(start, end))
  },
}

groupStart = 0
let totalAsync = 0
for (const rowGroup of metadata.row_groups) {
  const rowCount = Number(rowGroup.num_rows)
  const data = await parquetReadObjects({
    file: asyncBuffer,
    compressors,
    metadata,
    rowStart: groupStart,
    rowEnd: groupStart + rowCount,
    columns: ['turns'],
    filter: { turns: { $eq: 8 } },
    filterStrict: false,
    useOffsetIndex: true,
  })
  if (data.length > 0) {
    console.log(`  RG @${groupStart}: ${data.length} rows`)
  }
  totalAsync += data.length
  groupStart += rowCount
}
console.log(`  Total with AsyncBuffer: ${totalAsync}`)

// --- Test 4: Check if 525 relates to any partial read ---
console.log('\n=== Test 4: Checking partial counts ===')
const allCounts = []
groupStart = 0
for (const rowGroup of metadata.row_groups) {
  const rowCount = Number(rowGroup.num_rows)
  const data = await parquetReadObjects({
    file: arrayBuffer, compressors, metadata,
    rowStart: groupStart, rowEnd: groupStart + rowCount,
    columns: ['turns'], filter: { turns: { $eq: 8 } }, filterStrict: false,
  })
  allCounts.push({ groupStart, rowCount, matchCount: data.length })
  groupStart += rowCount
}

// Check cumulative sums
let cumulative = 0
for (const { groupStart, rowCount, matchCount } of allCounts) {
  cumulative += matchCount
  if (cumulative === 525 || matchCount === 525) {
    console.log(`  FOUND 525! cumulative=${cumulative} at groupStart=${groupStart}`)
  }
}
console.log(`  Final cumulative: ${cumulative}`)

// Check if 525 matches total minus something
console.log(`  7108 - 525 = ${7108 - 525}`)
console.log(`  525 / 7108 = ${(525/7108).toFixed(4)}`)

// Check if manual WHERE filter in JS gives different results
console.log('\n=== Test 5: Manual filter on all rows ===')
groupStart = 0
let manualCount = 0
let manualLooseCount = 0
for (const rowGroup of metadata.row_groups) {
  const rowCount = Number(rowGroup.num_rows)
  const data = await parquetReadObjects({
    file: arrayBuffer, compressors, metadata,
    rowStart: groupStart, rowEnd: groupStart + rowCount,
    columns: ['turns'],
  })
  for (const row of data) {
    if (row.turns === 8) manualCount++      // strict
    if (row.turns == 8) manualLooseCount++   // loose
  }
  groupStart += rowCount
}
console.log(`  Strict (=== 8): ${manualCount}`)
console.log(`  Loose (== 8): ${manualLooseCount}`)
