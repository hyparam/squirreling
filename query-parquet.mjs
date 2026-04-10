import { readFileSync } from 'fs'
import { parquetMetadata, parquetReadObjects, parquetSchema } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'
import { asyncRow, executeSql, collect, parseSql } from './src/index.js'

// --- WHERE pushdown: ported from hyperparam's parquetPushdownFilter.ts ---

function whereToParquetFilter(where) {
  if (!where) return undefined
  return convertExpr(where, false)
}

function convertExpr(node, negate) {
  if (node.type === 'unary' && node.op === 'NOT') {
    return convertExpr(node.argument, !negate)
  }
  if (node.type === 'binary') {
    return convertBinary(node, negate)
  }
  if (node.type === 'in valuelist') {
    return convertInValues(node, negate)
  }
  if (node.type === 'cast') {
    return convertExpr(node.expr, negate)
  }
  return undefined
}

function convertBinary({ op, left, right }, negate) {
  if (op === 'AND') {
    const l = convertExpr(left, negate)
    const r = convertExpr(right, negate)
    if (!l || !r) return
    return negate ? { $or: [l, r] } : { $and: [l, r] }
  }
  if (op === 'OR') {
    const l = convertExpr(left, false)
    const r = convertExpr(right, false)
    if (!l || !r) return
    return negate ? { $nor: [l, r] } : { $or: [l, r] }
  }
  if (op === 'LIKE') return

  const { column, value, flipped } = extractColumnAndValue(left, right)
  if (!column || value === undefined) return

  const mongoOp = mapOperator(op, flipped, negate)
  if (!mongoOp) return
  return { [column]: { [mongoOp]: value } }
}

function extractColumnAndValue(left, right) {
  if (left.type === 'identifier' && right.type === 'literal') {
    return { column: left.name, value: coerceToBigInt(right.value), flipped: false }
  }
  if (left.type === 'literal' && right.type === 'identifier') {
    return { column: right.name, value: coerceToBigInt(left.value), flipped: true }
  }
  return { column: undefined, value: undefined, flipped: false }
}

// Parquet integer columns are bigint — coerce number literals to match
function coerceToBigInt(value) {
  if (typeof value === 'number' && Number.isInteger(value)) return BigInt(value)
  return value
}

function mapOperator(op, flipped, negate) {
  const comparisons = ['=', '!=', '<>', '<', '>', '<=', '>=']
  if (!comparisons.includes(op)) return
  let mapped = op
  if (negate) mapped = neg(mapped)
  if (flipped) mapped = flip(mapped)
  if (mapped === '<') return '$lt'
  if (mapped === '<=') return '$lte'
  if (mapped === '>') return '$gt'
  if (mapped === '>=') return '$gte'
  if (mapped === '=') return '$eq'
  return '$ne'
}

function neg(op) {
  const map = { '<': '>=', '<=': '>', '>': '<=', '>=': '<', '=': '!=', '!=': '=' }
  return map[op] ?? op
}

function flip(op) {
  const map = { '<': '>', '<=': '>=', '>': '<', '>=': '<=' }
  return map[op] ?? op
}

function convertInValues(node, negate) {
  if (node.expr.type !== 'identifier') return
  const values = []
  for (const val of node.values) {
    if (val.type !== 'literal') return
    values.push(val.value)
  }
  return { [node.expr.name]: { [negate ? '$nin' : '$in']: values } }
}

// --- Parquet data source with pushdown ---

function parquetDataSource(file, metadata) {
  const schema = parquetSchema(metadata)
  return {
    numRows: Number(metadata.num_rows),
    columns: schema.children.map(c => c.element.name),
    scan(hints) {
      const whereFilter = hints.where && whereToParquetFilter(hints.where)
      const filter = hints.where ? whereFilter : undefined
      const appliedWhere = Boolean(filter && whereFilter)
      const appliedLimitOffset = !hints.where || appliedWhere

      return {
        rows: (async function* () {
          let groupStart = 0
          let remainingLimit = hints.limit ?? Infinity
          for (const rowGroup of metadata.row_groups) {
            if (hints.signal?.aborted) break
            const rowCount = Number(rowGroup.num_rows)

            let safeOffset = 0
            let safeLimit = rowCount
            if (appliedLimitOffset) {
              if (hints.offset !== undefined && groupStart < hints.offset) {
                safeOffset = Math.min(rowCount, hints.offset - groupStart)
              }
              safeLimit = Math.min(rowCount - safeOffset, remainingLimit)
              if (safeLimit <= 0 && safeOffset < rowCount) break
            }
            if (safeOffset === rowCount) {
              groupStart += rowCount
              continue
            }

            const cols = hints.columns ?? schema.children.map(c => c.element.name)
            console.error(`  row group ${groupStart}: reading ${safeLimit} rows, columns: [${cols}]${filter ? ', with pushdown filter' : ''}`)
            const data = await parquetReadObjects({
              file,
              compressors,
              metadata,
              rowStart: groupStart + safeOffset,
              rowEnd: groupStart + safeOffset + safeLimit,
              columns: cols,
              filter,
            })

            console.error(`    -> ${data.length} rows after filter`)
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

// --- Main ---
const parquetFile = process.argv[2] || '0000.parquet'
const query = process.argv[3] || 'SELECT COUNT(*) as cnt FROM data WHERE turns = 8'

const buffer = readFileSync(parquetFile)
const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
const metadata = parquetMetadata(arrayBuffer)

console.log(`File: ${parquetFile}`)
console.log(`Rows: ${metadata.num_rows}, Row groups: ${metadata.row_groups.length}`)
console.log(`Query: ${query}`)
console.log()

const source = parquetDataSource(arrayBuffer, metadata)
const start = performance.now()
const result = await collect(executeSql({ tables: { data: source }, query }))
const elapsed = (performance.now() - start).toFixed(0)

console.log('\nResult:')
for (const row of result) {
  console.log(row)
}
console.log(`(${elapsed}ms)`)
