import * as http from "http";
import dotenv from "dotenv";
import debug from "debug";
import {createClient} from "redis";

const [, , ...args] = process.argv;

dotenv.config({path: args[0] || '.env'});
debug.enable('*');

const log = debug('hash-sync')

// const MAX_RETRIES = process.env.MAX_RETRIES || '20';
const PORT = process.env.PORT || 9997;
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

const get1MinTimestamp = () => {
  const now = new Date();
  // Get the current minutes
  return `1min:${now.getMinutes().toString()}`
}

const get10MinTimestamp = () => {
  const now = new Date();
  // Get the current minutes
  const minutes = now.getMinutes();
  // Calculate the index of the 10-minute interval
  return `10min:${Math.floor(minutes / 10).toString()}`
}

const telemetry = async (redisClient) => {
  const ts = get1MinTimestamp();
  const ts10 = get10MinTimestamp();
  if (await redisClient.ttl('x1:hr1') === -1) {
    await redisClient.expire('x1:hr1', 3600);
  }
  if (await redisClient.ttl('x1:hr10') === -1) {
    await redisClient.expire('x1:hr10', 3600);
  }
  if (!await redisClient.hExists('x1:hr1', ts)) {
    await redisClient.hSet('x1:hr1', ts, 1);
  } else {
    await redisClient.hIncrBy('x1:hr1', ts, 1);
  }
  if (!await redisClient.hExists('x1:hr10', ts10)) {
    await redisClient.hSet('x1:hr10', ts10, 1);
  } else {
    await redisClient.hIncrBy('x1:hr10', ts10, 1);
  }
}

const timestamp = () => Math.floor(Date.now() / 1_000);

// entry point
(async () => {

  log('using listen port', PORT);
  log('using redis', `${REDIS_HOST}:${REDIS_PORT}`);

  const server = http.createServer();

  const redisClient = await createClient({
    url: `redis://${REDIS_HOST}:${REDIS_PORT}`
  }).on('error', (err) => console.error('ERR:REDIS:', err));
  redisClient.on('connect', () => log('redis connected'));
  await redisClient.connect();

  process.on('SIGINT', () => {
    log('SIGINT received, exiting');
    server.close();
    log('server closed');
    redisClient.quit();
    log('redis client closed');
  })

  // TODO: use when REDIS instance has Bloom Filter support
  // await redisClient.bf.reserve({ key: 'uniquesbloom', errorRate: 0.02, capacity: 1000000 })
  // await redisClient.bf.reserve('uniquesbloom', 0.02, 1000000);

  server.on('request', async (req, res) => {
    const {url, method} = req;
    if (method === 'POST') {
      const body = [];
      req
        .on('data', chunk => body.push(chunk))
        .on('end', async () => {
          const data = Buffer.concat(body).toString();
          try {
            const record = JSON.parse(data);
            const isNew = await redisClient.sAdd('x1:keys', record.key);
            if (isNew) {
              log('data', record.key, record.type);
              await redisClient.xAdd('x1:hashes', '*', { ...record, ts: timestamp().toString() });
              if (record.type === '0') {
                await telemetry(redisClient);
              }
              res.writeHead(200);
              res.end();
            } else {
              log('dup', record.key)
              res.writeHead(409);
              res.end('Existing key');
            }
          } catch (e) {
            log('ERROR', e)
            res.writeHead(500);
            res.end(e.message);
          }
        });
    } else {
      res.statusCode = 404;
      res.end();
    }
  });

  server.listen(PORT, '0.0.0.0', 100,
    () => {
      log(`Server is running on port ${PORT}`);
    });

})().catch(console.error);

