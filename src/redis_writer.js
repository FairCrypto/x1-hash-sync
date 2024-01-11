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
// const BATCH_SIZE = process.env.BATCH_SIZE || 10;

const getTimestamp = () => {
  const now = new Date();
// Set seconds and milliseconds to 0 to get the start of the minute
  now.setSeconds(0, 0);
// The timestamp for the start of the current minute
  return (now.getTime() / 1000).toString()
}

// entry point
(async () => {

  log('using listen port', PORT);
  // log('using batch size', BATCH_SIZE);
  log('using redis host:port', `${REDIS_HOST}:${REDIS_PORT}`);

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
            const hash = JSON.parse(data);
            const isNew = await redisClient.sAdd('x1:keys', hash.key);
            if (isNew) {
              log('data', hash.key);
              // await redisClient.lPush(hash.key, data);
              await redisClient.xAdd('x1:hashes', '*', hash);
              const ts = getTimestamp();
              if (! await redisClient.hExists('x1:hashRate', ts)) {
                await redisClient.hSet('x1:hashRate', ts, 1);
              } else {
                await redisClient.hIncrBy('x1:hashRate', ts, 1);
              }
              res.writeHead(200);
              res.end();
            } else {
              log('dup', hash.key)
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

