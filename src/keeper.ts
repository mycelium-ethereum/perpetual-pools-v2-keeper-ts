import { ethers } from 'ethers';
import {
  LeveragedPool,
  LeveragedPool__factory,
  PoolKeeper,
  PoolKeeper__factory,
  PoolFactory__factory,
  PoolFactory,
  ERC20__factory
} from '@tracer-protocol/perpetual-pools-contracts/types';
import { asyncInterval, attemptPromiseRecursively, nowFormatted } from './utils';

type KeeperConstructorArgs = {
  poolFactoryAddress: string,
  poolFactoryDeployedAtBlock: number,
  nodeUrl: string,
  privateKey: string,
  skipPools: { [poolAddress: string]: boolean },
  includePools: { [poolAddress: string]: boolean },
  gasLimit: number,
  balanceThresholds: { [tokenAddress: string]: string }
}

type WatchedPool = {
  address: string,
  keeperAddress: string,
  updateInterval: number,
  lastPriceTimestamp: number,
  contractInstance: LeveragedPool,
  nextUpkeepDue: number,
  isBusy: boolean,
  settlementTokenAddress: string,
  settlementTokenDecimals: number,
  totalBalance: ethers.BigNumber
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
  includePools: Record<string, boolean>
  balanceThresholds: Record<string, string>
  onChainTimestamp: number
  gasLimit: number

  constructor ({
    nodeUrl,
    privateKey,
    skipPools,
    includePools,
    gasLimit,
    poolFactoryAddress,
    poolFactoryDeployedAtBlock,
    balanceThresholds
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
    this.includePools = includePools;
    this.gasLimit = gasLimit;
    this.balanceThresholds = balanceThresholds;
  }

  // fetch known pools from the factory and add them as watched pools
  async syncKnownPools () {
    // create instance of pool factory
    // search logs for all DeployPool events
    // for each event, initialize the watched pool

    // allows for fallback list of watched pools in case rpc fails to sync deployed pools
    for (const poolAddress in this.includePools) {
      if (!this.skipPools[ethers.utils.getAddress(poolAddress)]) {
        await this.initializeWatchedPool(ethers.utils.getAddress(poolAddress));
      }
    }

    try {
      const deployPoolEventsFilter = this.poolFactoryInstance.filters.DeployPool();

      const deployPoolEvents = await this.poolFactoryInstance.queryFilter(
        deployPoolEventsFilter,
        this.poolFactoryDeployedAtBlock
      );

      for (const event of deployPoolEvents) {
        // if we are already watching it or its flagged to skip, ignore it
        const _poolAddress = ethers.utils.getAddress(event.args.pool);
        if (this.watchedPools[_poolAddress] || this.skipPools[_poolAddress]) {
          continue;
        }

        await this.initializeWatchedPool(_poolAddress);
      }
    } catch (error: any) {
      console.error(`failed to sync known pools from deployment events: ${error.message}`);
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
          keeper,
          balances,
          settlementTokenAddress
        ] = await Promise.all([
          poolInstance.lastPriceTimestamp(),
          poolInstance.updateInterval(),
          poolInstance.keeper(),
          poolInstance.balances(),
          poolInstance.settlementToken()
        ]);
        const lastPriceTimestampNumber = lastPriceTimestamp.toNumber();

        const updateIntervalNumber = Number(updateInterval);

        const settlementToken = ERC20__factory.connect(settlementTokenAddress, this.provider);

        const settlementTokenDecimals = await settlementToken.decimals();

        this.watchedPools[poolAddress] = {
          address: poolAddress,
          keeperAddress: keeper,
          updateInterval: updateIntervalNumber,
          lastPriceTimestamp: lastPriceTimestampNumber,
          contractInstance: poolInstance,
          nextUpkeepDue: lastPriceTimestampNumber + updateIntervalNumber,
          isBusy: false,
          settlementTokenAddress,
          settlementTokenDecimals,
          totalBalance: balances[0].add(balances[1])
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
        const {
          keeperAddress,
          nextUpkeepDue,
          isBusy,
          totalBalance,
          settlementTokenAddress,
          settlementTokenDecimals
        } = this.watchedPools[poolAddress];

        const lowBalanceThreshold = ethers.utils.parseUnits(
          this.balanceThresholds[settlementTokenAddress] || '0', settlementTokenDecimals
        );

        // do not consider pools with balances too low
        if (totalBalance.lt(lowBalanceThreshold)) {
          continue;
        }

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
          } catch (error: any) {
            console.error(`[${nowFormatted()}] failed to process upkeeps for keeper ${keeperAddress}: ${error.message}`);
          } finally {
            const timestampUpdates = poolsDue.map(async ({ address, contractInstance }) => {
              const lastPriceTimestamp = await attemptPromiseRecursively({
                label: `updating last price timestamp for pool ${address}`,
                promise: () => contractInstance.lastPriceTimestamp()
              });

              const balances = await attemptPromiseRecursively({
                label: `updating total balances for pool ${address}`,
                promise: () => contractInstance.balances()
              });

              const lastPriceTimestampNumber = lastPriceTimestamp.toNumber();

              this.watchedPools[address].totalBalance = balances[0].add(balances[1]);
              this.watchedPools[address].nextUpkeepDue = lastPriceTimestampNumber + this.watchedPools[address].updateInterval;
              this.watchedPools[address].lastPriceTimestamp = lastPriceTimestampNumber;
              this.watchedPools[address].isBusy = false;

              console.log(`[${nowFormatted()}] ${address} upkept at ${this.watchedPools[address].lastPriceTimestamp}, next due at ${this.watchedPools[address].nextUpkeepDue}. no longer busy`);
            });

            await Promise.all(timestampUpdates);
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
