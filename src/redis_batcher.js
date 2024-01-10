import dotenv from "dotenv";
import debug from "debug";
import {commandOptions, createClient} from "redis";

const [, , ...args] = process.argv;

dotenv.config({path: args[0] || '.env'});
debug.enable('*');

const log = debug('hash-sync')

// const MAX_RETRIES = process.env.MAX_RETRIES || '20';
// const PORT = process.env.PORT || 9997;
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const BATCH_SIZE = process.env.BATCH_SIZE || 10;

// entry point
(async () => {

  log('using redis host:port', `${REDIS_HOST}:${REDIS_PORT}`);
  log('using batch size', BATCH_SIZE);

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
      // https://github.com/redis/node-redis/blob/master/docs/isolated-execution.md
      commandOptions({ isolated: true }),
      { key: 'x1:hashes', id: '$' },
      { BLOCK: 0 }
    );
    // log(data[0].messages)
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
      // const r = await processNewHashBatch(hashes, contract);
      await redisClient.xAdd('x1:batches', '*', { type: '0', hashes: JSON.stringify(hashes) });
      log('hashes batched');

      // clear buffer
      hashes.splice(0, BATCH_SIZE)
    }
    if (xunis.length >= BATCH_SIZE) {
      log('xunis', xunis.length)
      // const r = await processHashBatch(xunis, contract, wallet.address);
      await redisClient.xAdd('x1:batches', '*', { type: '1', hashes: JSON.stringify(xunis) });
      log('xunis batched');

      // clear buffer
      xunis.splice(0, BATCH_SIZE)
    }
  }

})().catch(console.error);

