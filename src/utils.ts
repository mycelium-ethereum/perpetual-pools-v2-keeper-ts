/**
 * attempt promise until it succeeds or the maximum number of allowed attempts is reached
 *
 * @returns a promise that will eventually error or resolve to the same type as the original promise
 */
export const attemptPromiseRecursively = async <T>({
  promise,
  retryCheck,
  maxAttempts = 10,
  interval = 1000,
  attemptCount = 1,
  label = 'error caught in attemptPromiseRecursively'
}: {
  promise: () => Promise<T>
  retryCheck?: (error: unknown) => Promise<boolean>
  maxAttempts?: number
  interval?: number
  attemptCount?: number
  label?: string
}): Promise<T> => {
  try {
    const result = await promise();
    return result;
  } catch (error: any) {
    console.error(`[${nowFormatted()}: ${label}] ${error.message}`);

    if (attemptCount >= maxAttempts) {
      throw error;
    }

    // back off increasingly between attempts
    const newInterval = interval + 500;

    await new Promise(resolve => setTimeout(resolve, newInterval));

    if (!retryCheck || (retryCheck && await retryCheck(error))) {
      return attemptPromiseRecursively({ promise, retryCheck, interval: newInterval, maxAttempts, attemptCount: attemptCount + 1 });
    } else {
      throw error;
    }
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
