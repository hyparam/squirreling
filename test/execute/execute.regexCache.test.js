import { describe, expect, it } from 'vitest'
import { collect, executeSql } from '../../src/index.js'

/**
 * Inputs long enough to cross the memoization floor (1024 chars)
 *
 * @param {string} tag
 * @returns {string}
 */
function bigText(tag) {
  return `${tag} lorem ipsum dolor sit `.repeat(60)
}

describe('regexp per-node caching', () => {
  it('memoized REGEXP_REPLACE stays per-row-correct over repeated large inputs', async () => {
    const a = bigText('aa')
    const b = bigText('bb')
    const data = [a, a, b, a, b].map(text => ({ text }))
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT REGEXP_REPLACE(text, \' +\', \'-\') AS r FROM data',
    }))
    expect(result).toEqual(data.map(({ text }) => ({ r: text.replace(/ +/g, '-') })))
  })

  it('REGEXP_REPLACE with a non-literal replacement is never shared across rows', async () => {
    const a = bigText('cc')
    const data = [
      { text: a, rep: 'XX' },
      { text: a, rep: 'YY' },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT REGEXP_REPLACE(text, \'lorem\', rep) AS r FROM data',
    }))
    expect(result[0].r).toBe(a.replace(/lorem/g, 'XX'))
    expect(result[1].r).toBe(a.replace(/lorem/g, 'YY'))
    expect(result[0].r).not.toEqual(result[1].r)
  })

  it('REGEXP_SUBSTR occurrence search restarts per row when the compiled regex is reused', async () => {
    // The first row's occurrence scan stops mid-string; a reused 'g' regex
    // whose lastIndex were not reset would then miss the second row's early
    // matches and return null instead of num55.
    const first = 'x'.repeat(1500) + ' num11 num22 num33'
    const second = 'num44 num55 ' + 'y'.repeat(1500)
    const data = [{ text: first }, { text: second }]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT REGEXP_SUBSTR(text, \'num[0-9]+\', 1, 2) AS r FROM data',
    }))
    expect(result).toEqual([{ r: 'num22' }, { r: 'num55' }])
  })

  it('REGEXP_SUBSTR memoizes null results without contaminating later rows', async () => {
    const noDigits = bigText('dd')
    const withDigits = bigText('ee') + ' 123'
    const data = [noDigits, noDigits, withDigits].map(text => ({ text }))
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT REGEXP_SUBSTR(text, \'[0-9]+\') AS r FROM data',
    }))
    expect(result).toEqual([{ r: null }, { r: null }, { r: '123' }])
  })

  it('a pattern read from a column recompiles per row', async () => {
    const a = bigText('ff')
    const data = [
      { text: a, pat: 'lorem' },
      { text: a, pat: 'ipsum' },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT REGEXP_SUBSTR(text, pat) AS r FROM data',
    }))
    expect(result).toEqual([{ r: 'lorem' }, { r: 'ipsum' }])
  })

  it('short inputs below the memo floor behave identically', async () => {
    const data = [
      { text: 'one  two' },
      { text: 'one  two' },
      { text: 'three four' },
    ]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT REGEXP_REPLACE(text, \' +\', \'_\') AS r FROM data',
    }))
    expect(result).toEqual([{ r: 'one_two' }, { r: 'one_two' }, { r: 'three_four' }])
  })

  it('memoized results respect position and occurrence arguments', async () => {
    const a = 'z'.repeat(1200) + ' cat cat cat'
    const data = [{ text: a }, { text: a }]
    const result = await collect(executeSql({
      tables: { data },
      query: 'SELECT REGEXP_REPLACE(text, \'cat\', \'dog\', 1, 2) AS r FROM data',
    }))
    const expected = 'z'.repeat(1200) + ' cat dog cat'
    expect(result).toEqual([{ r: expected }, { r: expected }])
  })

  it('invalid literal patterns still throw with caching active', async () => {
    const data = [{ text: bigText('gg') }]
    await expect(collect(executeSql({
      tables: { data },
      query: 'SELECT REGEXP_REPLACE(text, \'[\', \'-\') AS r FROM data',
    }))).rejects.toThrow(/invalid regex pattern/)
  })
})
