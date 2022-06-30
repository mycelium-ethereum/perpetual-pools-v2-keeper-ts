import { ethers } from 'ethers';
import {
  LeveragedPool,
  LeveragedPool__factory,
  PoolKeeper,
  PoolKeeper__factory,
  PoolFactory__factory,
  PoolFactory
} from '@tracer-protocol/perpetual-pools-contracts/types';
import { asyncInterval, attemptPromiseRecursively, nowFormatted } from './utils';

type KeeperConstructorArgs = {
  poolFactoryAddress: string,
  poolFactoryDeployedAtBlock: number,
  nodeUrl: string,
  privateKey: string,
  skipPools: { [poolAddress: string]: boolean },
  gasLimit: number
}

type WatchedPool = {
  address: string,
  keeperAddress: string,
  updateInterval: number,
  lastPriceTimestamp: number,
  contractInstance: LeveragedPool,
  nextUpkeepDue: number,
  isBusy: boolean
}

class Keeper {
  provider: ethers.providers.BaseProvider
  wallet: ethers.Wallet
  poolFactoryInstance: PoolFactory
  poolFactoryDeployedAtBlock: number
  watchedPools: Record<string, WatchedPool>
  keeperInstances: Record<string, PoolKeeper>
  scheduledUpkeeps: Record<string, Record<string, { pools: string[], upkeepPromise: Promise<void> }>>
  skipPools: Record<string, boolean>
  onChainTimestamp: number
  gasLimit: number

  constructor ({
    nodeUrl,
    privateKey,
    skipPools,
    gasLimit,
    poolFactoryAddress,
    poolFactoryDeployedAtBlock
  }: KeeperConstructorArgs) {
    this.provider = ethers.getDefaultProvider(nodeUrl);
    this.poolFactoryInstance = PoolFactory__factory.connect(poolFactoryAddress, this.provider);
    this.poolFactoryDeployedAtBlock = poolFactoryDeployedAtBlock;
    this.wallet = new ethers.Wallet(privateKey, this.provider);
    this.onChainTimestamp = 0;
    this.watchedPools = {};
    this.keeperInstances = {};
    this.scheduledUpkeeps = {};
    this.skipPools = skipPools;
    this.gasLimit = gasLimit;
  }

  // fetch known pools from the factory and add them as watched pools
  async syncKnownPools () {
    // create instance of pool factory
    // search logs for all DeployPool events
    // for each event, initialize the watched pool

    const deployPoolEventsFilter = this.poolFactoryInstance.filters.DeployPool();

    const deployPoolEvents = await this.poolFactoryInstance.queryFilter(
      deployPoolEventsFilter,
      this.poolFactoryDeployedAtBlock
    );

    for (const event of deployPoolEvents) {
      if (!this.skipPools[ethers.utils.getAddress(event.args.pool)]) {
        await this.initializeWatchedPool(event.args.pool);
      }
    }
  }

  initializeWatchedPool (poolAddress: string) {
    console.log(`[${nowFormatted()}] initializing ${poolAddress} as a watched pool`);
    return attemptPromiseRecursively({
      promise: async () => {
        const poolInstance = LeveragedPool__factory.connect(poolAddress, this.provider);

        const [
          lastPriceTimestamp,
          updateInterval,
          keeper
        ] = await Promise.all([
          poolInstance.lastPriceTimestamp(),
          poolInstance.updateInterval(),
          poolInstance.keeper()
        ]);
        const lastPriceTimestampNumber = lastPriceTimestamp.toNumber();

        const updateIntervalNumber = Number(updateInterval);

        this.watchedPools[poolAddress] = {
          address: poolAddress,
          keeperAddress: keeper,
          updateInterval: updateIntervalNumber,
          lastPriceTimestamp: lastPriceTimestampNumber,
          contractInstance: poolInstance,
          nextUpkeepDue: lastPriceTimestampNumber + updateIntervalNumber,
          isBusy: false
        };

        if (!this.keeperInstances[keeper]) {
          this.keeperInstances[keeper] = PoolKeeper__factory.connect(keeper, this.wallet);
        }
      }
    });
  }

  processDueUpkeeps () {
    return new Promise((resolve, reject) => {
      const dueForUpkeepByKeeperAddress: Record<string, WatchedPool[]> = {};

      const now = Math.floor(Date.now() / 1000);

      for (const poolAddress in this.watchedPools) {
        const { keeperAddress, nextUpkeepDue, isBusy } = this.watchedPools[poolAddress];

        if (!isBusy && nextUpkeepDue <= now) {
          // mark watched pool as busy
          this.watchedPools[poolAddress].isBusy = true;

          dueForUpkeepByKeeperAddress[keeperAddress] = dueForUpkeepByKeeperAddress[keeperAddress] || [];
          dueForUpkeepByKeeperAddress[keeperAddress].push(this.watchedPools[poolAddress]);
        }
      }

      const promises = [];

      for (const keeperAddress in dueForUpkeepByKeeperAddress) {
        const keeperInstance = this.keeperInstances[keeperAddress];
        const poolsDue = dueForUpkeepByKeeperAddress[keeperAddress];

        console.log(`[${nowFormatted()}] ${JSON.stringify(poolsDue.map(pool => pool.address))} are due for upkeep by keeper ${keeperAddress}`);

        // find the pool that became due for upkeep most recently
        const mostRecentlyDuePool = poolsDue.sort((a, b) => b.nextUpkeepDue - a.nextUpkeepDue)[0];

        console.log(`[${nowFormatted()}] ${mostRecentlyDuePool.address} is due next at ${new Date(mostRecentlyDuePool.nextUpkeepDue * 1000).toLocaleString()}`);

        // wait until most recently due pool can be upkept according to on-chain time
        const processUpkeepForKeeper = async () => {
          try {
            // ensure at least one pool is actually ready to be upkept
            await attemptPromiseRecursively({
              promise: async () => {
                console.log(`[${nowFormatted()}] checking upkeep required for pool ${mostRecentlyDuePool.address}`);

                const readyForUpkeep = await keeperInstance.isUpkeepRequiredSinglePool(mostRecentlyDuePool.address);

                if (!readyForUpkeep) {
                  // retrying will be handled by attemptPromiseRecursively
                  throw new Error(`[${nowFormatted()}] ${mostRecentlyDuePool.address} not ready for upkeep yet`);
                }
              }
            });

            // submit the upkeep transaction
            const poolsDueAddresses = poolsDue.map(({ address }) => address);

            await attemptPromiseRecursively({
              promise: async () => {
                const tx = poolsDueAddresses.length === 1
                  ? await keeperInstance.performUpkeepSinglePool(poolsDueAddresses[0], { gasLimit: this.gasLimit })
                  : await keeperInstance.performUpkeepMultiplePools(poolsDueAddresses, { gasLimit: this.gasLimit });

                await this.provider.waitForTransaction(tx.hash);
              }
            });

            // dont need to wait for this, the pools will eventually become available for upkeep again
            for (const { address, contractInstance } of poolsDue) {
              attemptPromiseRecursively({
                label: `updating last price timestamp for pool ${address}`,
                promise: async () => {
                  // fetch new lastPriceTimestamp
                  return contractInstance.lastPriceTimestamp();
                }
              })
                .then(lastPriceTimestamp => {
                  const lastPriceTimestampNumber = lastPriceTimestamp.toNumber();

                  this.watchedPools[address].nextUpkeepDue = lastPriceTimestampNumber + this.watchedPools[address].updateInterval;
                  this.watchedPools[address].lastPriceTimestamp = lastPriceTimestampNumber;

                  console.log(`[${nowFormatted()}] ${address} upkept at ${this.watchedPools[address].lastPriceTimestamp}, next due at ${this.watchedPools[address].nextUpkeepDue}`);
                })
                .finally(() => {
                  this.watchedPools[address].isBusy = false;
                  console.log(`[${nowFormatted()}] ${address} is no longer busy`);
                });
            };
          } catch (error: any) {
            console.error(`[${nowFormatted()}] failed to process upkeeps for keeper ${keeperAddress}: ${error.message}`);
          }
        };

        promises.push(processUpkeepForKeeper());
      }

      Promise.allSettled(promises)
        .then(() => resolve(null))
        .catch(error => reject(error));
    });
  }

  startUpkeeping ({ interval }: { interval: number }) {
    console.log(`[${nowFormatted()}] starting upkeeping interval`);

    asyncInterval({
      fn: async () => this.processDueUpkeeps(),
      delayMs: interval,
      runImmediately: true,
      onError: (error) => {
        console.error(`[${nowFormatted()}: asyncInterval] ${error.message}`);
      }
    });
  }

  startWatchingForNewPools () {
    const deployPoolEventsFilter = this.poolFactoryInstance.filters.DeployPool();

    this.poolFactoryInstance.on(deployPoolEventsFilter, async (poolAddress) => {
      await this.initializeWatchedPool(poolAddress);
    });
  }
}

export default Keeper;
