/**
 * Collects all results from an async generator into an array
 *
 * @template T
 * @param {AsyncGenerator<T>} asyncGen - the async generator
 * @returns {Promise<T[]>} array of all yielded values
 */
export async function collect(asyncGen) {
  const results = []
  for await (const item of asyncGen) {
    results.push(item)
  }
  return results
}
