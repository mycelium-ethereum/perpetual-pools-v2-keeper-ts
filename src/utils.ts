export const attemptPromiseRecursively = async <T>({
  promise,
  interval = 1000,
  label = 'error caught in attemptPromiseRecursively'
}: {
  promise: () => Promise<T>,
  interval?: number
  label?: string
}): Promise<T> => {
  try {
    return await promise();
  } catch (error: any) {
    console.error(`[${nowFormatted()}: ${label}] ${error.message}`);
    await new Promise(resolve => setTimeout(resolve, interval + 500));
    return attemptPromiseRecursively({ promise, interval });
  }
};

/**
 * run asynchronous function `fn` on an interval
 *
 * if `fn()` runs for longer than the interval size
 * the interval will perform a noop until `fn` has completed
 * before running `fn` again
 * @param options
 * @param options.fn asynchronous function to perform
 * @param options.delayMs ms between intervals
 * @param options.onError called if `fn` rejects
 * @param options.onUnavailable called if `fn` is still running from a previous interval
 * @param options.runImmediately if true, will run `fn` immediately instead of waiting for first interval
 *
 * @returns the interval instance which can be called by passing to `clearInterval()`
 */
export const asyncInterval = ({
  fn,
  delayMs,
  onError,
  onUnavailable,
  runImmediately = false
}: {
  fn: () => Promise<any>,
  delayMs: number,
  onError?: (error: Error) => any
  onUnavailable?: () => void,
  runImmediately?: boolean
}) => {
  // create a lock to prevent promises backing up
  // if they run longer than provided delay
  // unavailable by default until initial call to `fn` completes
  let isAvailable = !runImmediately;

  if (runImmediately) {
    // attempt first call immediately
    // and then set lock available
    fn()
      .then(() => {
        // set lock to be available
        isAvailable = true;
      })
      .catch(error => {
        isAvailable = true;
        onError?.(error);
      });
  }

  const interval = setInterval(async () => {
    if (isAvailable) {
      try {
        isAvailable = false;
        await fn();
        isAvailable = true;
      } catch (error: any) {
        onError?.(error);
        isAvailable = true;
      }
    } else {
      onUnavailable?.();
    }
  }, delayMs);

  return interval;
};
export const nowFormatted = () => new Date().toLocaleString();
