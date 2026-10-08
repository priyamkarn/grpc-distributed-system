export const jobError = (message, retryable = true) =>
  Object.assign(new Error(message), { retryable });

export const sleep = (ms, signal) =>
  new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal.addEventListener('abort', onAbort, { once: true });
  });

export const defaultHandlers = {
  echo: async (payload) => payload,

  sleep: async ({ ms = 1000 } = {}, { signal }) => {
    await sleep(ms, signal);
    return { slept: ms };
  },

  sum: async ({ numbers } = {}) => {
    if (!Array.isArray(numbers) || numbers.some((n) => typeof n !== 'number'))
      throw jobError('payload.numbers must be an array of numbers', false);
    return numbers.reduce((a, b) => a + b, 0);
  },

  fib: async ({ n } = {}) => {
    if (!Number.isInteger(n) || n < 0 || n > 10000)
      throw jobError('payload.n must be an integer in [0, 10000]', false);
    let a = 0n;
    let b = 1n;
    for (let i = 0; i < n; i++) [a, b] = [b, a + b];
    return a.toString();
  },

  flaky: async ({ failRate = 0.5 } = {}) => {
    if (Math.random() < failRate) throw jobError('random failure');
    return 'survived';
  },

  fail: async ({ message = 'boom', permanent = false } = {}) => {
    throw jobError(message, !permanent);
  },
};
