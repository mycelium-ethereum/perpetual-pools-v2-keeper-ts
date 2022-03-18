import { ethers } from 'ethers';
import {
  LeveragedPool,
  LeveragedPool__factory,
  PoolKeeper,
  PoolKeeper__factory,
  PoolFactory__factory,
  PoolFactory
} from './typesV2';
import { attemptPromiseRecursively } from './utils';

type KeeperConstructorArgs = {
  poolFactoryAddress: string,
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
  watchedPools: Record<string, WatchedPool>
  keeperInstances: Record<string, PoolKeeper>
  scheduledUpkeeps: Record<string, Record<string, { pools: string[], upkeepPromise: Promise<void> }>>
  skipPools: Record<string, boolean>
  onChainTimestamp: number
  gasLimit: number

  constructor ({ nodeUrl, privateKey, skipPools, gasLimit, poolFactoryAddress }: KeeperConstructorArgs) {
    this.provider = ethers.getDefaultProvider(nodeUrl);
    this.poolFactoryInstance = PoolFactory__factory.connect(poolFactoryAddress, this.provider);
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

    const deployPoolEvents = await this.poolFactoryInstance.queryFilter(deployPoolEventsFilter);

    for (const event of deployPoolEvents) {
      await this.initializeWatchedPool(event.args.pool);
    }
  }

  initializeWatchedPool (poolAddress: string) {
    console.log(`initializing ${poolAddress} as a watched pool`);
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
    const dueForUpkeepByKeeperAddress: Record<string, WatchedPool[]> = {};

    const now = Math.floor(Date.now() / 1000);

    for (const poolAddress in this.watchedPools) {
      const { keeperAddress, nextUpkeepDue, isBusy } = this.watchedPools[poolAddress];

      // add a buffer because of arbitrum block time jank
      if (!isBusy && nextUpkeepDue + 5 <= now) {
        // mark watched pool as busy
        this.watchedPools[poolAddress].isBusy = true;

        dueForUpkeepByKeeperAddress[keeperAddress] = dueForUpkeepByKeeperAddress[keeperAddress] || [];
        dueForUpkeepByKeeperAddress[keeperAddress].push(this.watchedPools[poolAddress]);
      }
    }

    for (const keeperAddress in dueForUpkeepByKeeperAddress) {
      const keeperInstance = this.keeperInstances[keeperAddress];
      const poolsDue = dueForUpkeepByKeeperAddress[keeperAddress];

      // find the pool that was last upkeep the most recently
      const mostRecentlyDuePool = poolsDue.sort((a, b) => b.nextUpkeepDue - a.nextUpkeepDue)[0];

      // wait until most recently due pool can be upkept according to on-chain time
      attemptPromiseRecursively({
        promise: async () => {
          const readyForUpkeep = await keeperInstance.isUpkeepRequiredSinglePool(mostRecentlyDuePool.address);

          if (!readyForUpkeep) {
            // attemptPromiseRecursively will retry after 1 second
            throw new Error('Pool not ready for upkeep yet');
          }
        }
      })
        .then(async () => {
          const poolsDueAddresses = poolsDue.map(({ address }) => address);

          console.log(`${JSON.stringify(poolsDueAddresses)} are due for upkeep by keeper ${keeperAddress}`);

          return attemptPromiseRecursively({
            promise: async () => {
              const tx = poolsDueAddresses.length === 1
                ? await keeperInstance.performUpkeepSinglePool(poolsDueAddresses[0], { gasLimit: this.gasLimit })
                : await keeperInstance.performUpkeepMultiplePools(poolsDueAddresses, { gasLimit: this.gasLimit });

              await this.provider.waitForTransaction(tx.hash);
            }
          });
        })
        .then(async () => {
          const promises = poolsDue.map(async ({ address, contractInstance }) => {
            return attemptPromiseRecursively({
              promise: async () => {
                // fetch new lastPriceTimestamp
                const lastPriceTimestamp = await contractInstance.lastPriceTimestamp();
                const lastPriceTimestampNumber = lastPriceTimestamp.toNumber();

                console.log(`COMPLETE UPKEEP FOR POOL ${address}, OLD LAST PRICE TIMESTAMP WAS ${this.watchedPools[address].lastPriceTimestamp}`);

                this.watchedPools[address].isBusy = false;
                this.watchedPools[address].nextUpkeepDue = lastPriceTimestampNumber + this.watchedPools[address].updateInterval;
                this.watchedPools[address].lastPriceTimestamp = lastPriceTimestampNumber;

                console.log(`COMPLETE UPKEEP FOR POOL ${address}, NEW LAST PRICE TIMESTAMP IS ${this.watchedPools[address].lastPriceTimestamp}`);
              }
            });
          });

          await Promise.all(promises);
        })
        .catch(error => {
          console.error(error);
        });
    }
  }

  startUpkeeping ({ interval }: { interval: number }) {
    setInterval(this.processDueUpkeeps.bind(this), interval);
  }

  startWatchingForNewPools () {
    const deployPoolEventsFilter = this.poolFactoryInstance.filters.DeployPool();

    this.poolFactoryInstance.on(deployPoolEventsFilter, async (poolAddress) => {
      await this.initializeWatchedPool(poolAddress);
    });
  }
}

export default Keeper;
