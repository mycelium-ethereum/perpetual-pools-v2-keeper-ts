import dotenv from 'dotenv';
import Keeper from './keeper';

dotenv.config();

async function main () {
  const keeper = new Keeper({
    graphUrl: process.env.SUBGRAPH_URL as string,
    privateKey: process.env.PRIVATE_KEY as string,
    nodeUrl: process.env.NODE_URL as string,
    skipPools: {},
    gasLimit: Number(process.env.GAS_LIMIT || 5000000)
  });

  await keeper.syncWatchedPools();

  keeper.startSyncingNewPools({ interval: Number(process.env.POOL_SYNC_INTERVAL || 30000) });
  keeper.startUpkeeping({ interval: Number(process.env.UPKEEP_INTERVAL || 5000) });
}

main();
