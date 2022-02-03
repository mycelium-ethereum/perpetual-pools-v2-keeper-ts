import { ethers } from 'ethers';
import { makeGraphRequest } from './thegraph';
import { LeveragedPool, LeveragedPool__factory, PoolKeeper, PoolKeeper__factory } from './typesV2';
import { attemptPromiseRecursively } from './utils';

type KeeperConstructorArgs = {
  graphUrl: string,
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

type GraphLeveragedPool = {
  keeper: string,
  id: string,
  updateInterval: string
}

class Keeper {
  provider: ethers.providers.BaseProvider
  wallet: ethers.Wallet
  graphUrl: string
  watchedPools: Record<string, WatchedPool>
  keeperInstances: Record<string, PoolKeeper>
  scheduledUpkeeps: Record<string, Record<string, { pools: string[], upkeepPromise: Promise<void> }>>
  skipPools: Record<string, boolean>
  onChainTimestamp: number
  gasLimit: number

  constructor ({ graphUrl, nodeUrl, privateKey, skipPools, gasLimit }: KeeperConstructorArgs) {
    this.provider = ethers.getDefaultProvider(nodeUrl);
    this.wallet = new ethers.Wallet(privateKey, this.provider);
    this.graphUrl = graphUrl;
    this.onChainTimestamp = 0;
    this.watchedPools = {};
    this.keeperInstances = {};
    this.scheduledUpkeeps = {};
    this.skipPools = skipPools;
    this.gasLimit = gasLimit;
  }

  // fetch known pools from the graph and add any new ones as a watched pool
  async syncWatchedPools () {
    const graphResponse = await makeGraphRequest<{ data: { leveragedPools: GraphLeveragedPool[] } }>({
      url: this.graphUrl,
      query: `
        {
          leveragedPools {
            id
            keeper
            updateInterval
          }
        }
      `
    });

    const promises = graphResponse.data.leveragedPools.map(({ id, keeper, updateInterval }) => {
      if (!this.watchedPools[id] && !this.skipPools[id]) {
        console.log(`Adding ${id} as a watched pool`);
        return attemptPromiseRecursively({
          promise: async () => {
            const poolInstance = LeveragedPool__factory.connect(id, this.provider);

            const lastPriceTimestamp = await poolInstance.lastPriceTimestamp();
            const lastPriceTimestampNumber = lastPriceTimestamp.toNumber();

            const updateIntervalNumber = Number(updateInterval);

            this.watchedPools[id] = {
              address: id,
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
      return Promise.resolve();
    });

    await Promise.all(promises);
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
                ? await keeperInstance.performUpkeepSinglePool(poolsDueAddresses[0], false, 0, { gasLimit: this.gasLimit })
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

  startSyncingNewPools ({ interval }: { interval: number }) {
    setInterval(this.syncWatchedPools.bind(this), interval);
  }
}

export default Keeper;
