import dotenv from "dotenv";
import debug from "debug";
import {commandOptions, createClient} from "redis";
import BlockStorage from "../abi/BlockStorage.json" assert { type: "json" };
import {Contract, JsonRpcProvider, NonceManager, Wallet} from "ethers";
import {processHashBatch, processNewHashBatch} from "./processNewHashBatch.js";

const [, , ...args] = process.argv;

dotenv.config({path: args[0] || '.env'});
debug.enable('*');

const log = debug('hash-sync')
const abi = BlockStorage.abi;

const RPC_URL = process.env.RPC_URL || 'https://x1-testnet.infrafc.org';
const NETWORK_ID = process.env.NETWORK_ID || '204005';
// const MAX_RETRIES = process.env.MAX_RETRIES || '20';
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;

// entry point
(async () => {

  log('using RPC', RPC_URL);
  log('using network', NETWORK_ID);
  log('using contract', CONTRACT_ADDRESS);
  log('using redis', `${REDIS_HOST}:${REDIS_PORT}`);

  const provider = new JsonRpcProvider(RPC_URL, Number(NETWORK_ID));
  const wallet = new Wallet(process.env.PK, provider);
  // const nonceManager = new NonceManager(wallet);
  const contract = new Contract(CONTRACT_ADDRESS, abi, wallet);

  const redisClient = await createClient({
    url: `redis://${REDIS_HOST}:${REDIS_PORT}`
  }).on('error', (err) => console.error('ERR:REDIS:', err));
  redisClient.on('connect', () => log('redis connected'));
  await redisClient.connect();

  process.on('SIGINT', () => {
    log('SIGINT received, exiting');
    redisClient.quit();
    log('redis client closed');
    process.exit(0);
  })

  let lastBatchId = await redisClient.get('x1:lastBatchId');
  log('last batch_id', lastBatchId);

  while (true) {
    const data = await redisClient.xRead(
      // https://github.com/redis/node-redis/blob/master/docs/isolated-execution.md
      commandOptions({ isolated: true }),
      { key: 'x1:batches', id: lastBatchId || '$' },
      { BLOCK: 0, COUNT: 1 }
    );
    const msg = data[0]?.messages?.[0]?.message;
    const message = { ...msg, hashes: JSON.parse(msg.hashes) };
    // log(message);
    lastBatchId = data[0]?.messages?.reduce((acc, m) => m.id, lastBatchId);

    if (message.type === '0') {
      log('hashes batch', message.hashes.length);
      const r = await processNewHashBatch(message.hashes, contract);
      log('hashes sent', r);
      if (r === 'OK') {
        await redisClient.set('x1:lastBatchId', lastBatchId);
      }
    } else {
      log('xunis batch', message.hashes.length)
      const r = await processHashBatch(message.hashes, contract, wallet.address);
      log('xunis sent', r);
      if (r === 'OK') {
        await redisClient.set('x1:lastBatchId', lastBatchId);
      }
    }
  }

})().catch(console.error);

