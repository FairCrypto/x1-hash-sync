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
// const PORT = process.env.PORT || 9997;
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;
const BATCH_SIZE = process.env.BATCH_SIZE || 10;

// entry point
(async () => {

  log('using RPC', RPC_URL);
  log('using network', NETWORK_ID);
  log('using contract', CONTRACT_ADDRESS);
  log('using redis host:port', `${REDIS_HOST}:${REDIS_PORT}`);
  log('using batch size', BATCH_SIZE);

  const provider = new JsonRpcProvider(RPC_URL, Number(NETWORK_ID));
  const wallet = new Wallet(process.env.PK, provider);
  // const nonceManager = new NonceManager(wallet);
  const contract = new Contract(CONTRACT_ADDRESS, abi, wallet);

  const redisClient = await createClient({
    host: REDIS_HOST,
    port: REDIS_PORT
  }).on('error', (err) => console.error('ERR:REDIS:', err));
  redisClient.on('connect', () => log('redis connected'));
  await redisClient.connect();

  process.on('SIGINT', () => {
    log('SIGINT received, exiting');
    redisClient.quit();
    log('redis client closed');
    process.exit(0);
  })

  const hashes = [];
  const xunis = [];
  while (true) {
    const data = await redisClient.xRead(
      commandOptions({ isolated: true }), // https://github.com/redis/node-redis/blob/master/docs/isolated-execution.md
      { key: 'x1:hashes', id: '$' },
      { BLOCK: 0, COUNT: BATCH_SIZE }
    );
    log(data[0].messages)
    hashes.push(...data[0].messages
      .map(m => m.message)
      .filter(m => m.type === '0')
    );
    xunis.push(...data[0].messages
      .map(m => m.message)
      .filter(m => m.type !== '0')
    );
    if (hashes.length >= BATCH_SIZE) {
      log('hashes', hashes.length);
      const r = await processNewHashBatch(hashes, contract);
      log('hashes sent', r);

      // clear buffer
      hashes.splice(0, BATCH_SIZE)
    }
    if (xunis.length >= BATCH_SIZE) {
      log('xunis', xunis.length)
      const r = await processHashBatch(xunis, contract, wallet.address);
      log('xunis sent', r);

      // clear buffer
      xunis.splice(0, BATCH_SIZE)
    }
  }

})().catch(console.error);

