import dotenv from 'dotenv';
import Keeper from './keeper';

dotenv.config();

async function main () {
  const keeper = new Keeper({
    privateKey: process.env.PRIVATE_KEY as string,
    poolFactoryAddress: process.env.POOL_FACTORY_ADDRESS as string,
    poolFactoryDeployedAtBlock: Number(process.env.POOL_FACTORY_DEPLOYED_AT_BLOCK || 0),
    nodeUrl: process.env.NODE_URL as string,
    skipPools: {},
    gasLimit: Number(process.env.GAS_LIMIT || 5000000)
  });

  await keeper.syncKnownPools();

  keeper.startWatchingForNewPools();
  keeper.startUpkeeping({ interval: Number(process.env.UPKEEP_INTERVAL || 5000) });
}

main();
