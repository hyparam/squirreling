// Yield to the event loop so that timer-based aborts can fire.
//
// In Node, setTimeout(fn, 0) is clamped to a 1ms minimum, so a tight loop
// that yields every few thousand iterations can spend hundreds of ms in
// scheduling overhead alone. setImmediate (Node) and MessageChannel
// (browsers) provide the same macrotask boundary at a fraction of the cost.
//
// We need a macrotask boundary (not just a microtask) because the abort
// timer itself is a macrotask; microtasks alone cannot let it fire.

/** @type {() => Promise<void>} */
export const yieldToEventLoop = (() => {
  if (typeof setImmediate === 'function') {
    return () => new Promise(resolve => setImmediate(resolve))
  }
  if (typeof MessageChannel !== 'undefined') {
    const channel = new MessageChannel()
    /** @type {Array<() => void>} */
    const queue = []
    channel.port1.onmessage = () => {
      const resolve = queue.shift()
      if (resolve) resolve()
    }
    return () => new Promise(resolve => {
      queue.push(resolve)
      channel.port2.postMessage(0)
    })
  }
  return () => new Promise(resolve => setTimeout(resolve, 0))
})()
