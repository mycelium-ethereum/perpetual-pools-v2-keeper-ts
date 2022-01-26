export const attemptPromiseRecursively = async <T>({
  promise,
  interval = 1000
}: {
  promise: () => Promise<T>,
  interval?: number
}): Promise<T> => {
  try {
    return await promise();
  } catch (error) {
    await new Promise(resolve => setTimeout(resolve, interval));
    return attemptPromiseRecursively({ promise, interval });
  }
};
