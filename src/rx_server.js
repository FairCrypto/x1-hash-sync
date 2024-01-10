import * as http from "http";
import dotenv from "dotenv";
import debug from "debug";
import BlockStorage from "../abi/BlockStorage.json";
import {Contract, JsonRpcProvider, NonceManager, Wallet} from "ethers";
import {bufferCount, filter, fromEvent, map, mergeMap, partition, tap} from "rxjs";
import {processHashBatch, processNewHashBatch} from "./processNewHashBatch.js";
import {initBloomFilter} from "./bloomFilter.js";
import path from "path";
import * as fs from "fs";

const [, , ...args] = process.argv;

dotenv.config({path: args[0] || '.env'});
debug.enable('*,-body-parser:*');

const log = debug('hash-sync')
const abi = BlockStorage.abi;

const RPC_URL = process.env.RPC_URL || 'https://x1-testnet.infrafc.org';
const NETWORK_ID = process.env.NETWORK_ID || '204005';
// const MAX_RETRIES = process.env.MAX_RETRIES || '20';
const PORT = process.env.PORT || 9997;
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;
const BATCH_SIZE = process.env.BATCH_SIZE || 10;

log('using RPC', RPC_URL);
log('using network', NETWORK_ID);
log('using contract', CONTRACT_ADDRESS);
log('using listen port', PORT);
log('using batch size', BATCH_SIZE);

const provider = new JsonRpcProvider(RPC_URL, Number(NETWORK_ID));
const wallet = new Wallet(process.env.PK, provider);
// const nonceManager = new NonceManager(wallet);
const contract = new Contract(CONTRACT_ADDRESS, abi, wallet);

let batchedBlocks$, batchedXunis$;
let bloomFilter;
let rss0, heapTotal0, heapUsed0, external0, arrayBuffers0

if (fs.existsSync(path.resolve('.', 'bloom.json'))) {
  const jsonStr = fs.readFileSync(path.resolve('.', 'bloom.json'), 'utf8');
  bloomFilter = initBloomFilter(jsonStr);
  log('loaded bloom filter');
} else {
  bloomFilter = initBloomFilter();
  log('initialized bloom filter');
}

process.on('SIGINT', () => {
  log('SIGINT received, exiting');
  if (batchedBlocks$) batchedBlocks$.unsubscribe();
  log('unsubscribed from blocks')
  if (batchedXunis$) batchedXunis$.unsubscribe();
  log('unsubscribed from xunis')
  if (bloomFilter) {
    const json = bloomFilter.saveAsJSON();
    log('prepping bloom filter');
    const jsonStr = JSON.stringify(json);
    log('writing bloom filter', jsonStr?.length);
    fs.writeFileSync(path.resolve('.', 'bloom.json'), jsonStr, 'utf8');
    log('saved bloom filter');
  }
  process.exit(0);
})

const server = http.createServer();

const records$ = fromEvent(server, 'request')
  .pipe(
    filter(([req, res]) => req.method?.toLowerCase() === 'post'),
    mergeMap(([req, res]) => {
      return fromEvent(req, 'data')
        .pipe(
          map((chunk) => chunk.toString()),
          map((body) => [req, res, JSON.parse(body)])
        )
    })
  );

const [blocks$, xunis$] = partition(
  records$.pipe(filter(async ([req, res, data]) => {
    const hasKey = data?.key && await bloomFilter.has(data?.key);
    if (!hasKey && data?.key) {
      bloomFilter.add(data?.key);
      return true;
    } else if (!data?.key) {
      log('no key', data?.key);
      return false;
    } else {
      const { rss, heapTotal, heapUsed, external, arrayBuffers } = process.memoryUsage();
      rss0 = rss0 || rss;
      heapTotal0 = heapTotal0 || heapTotal;
      heapUsed0 = heapUsed0 || heapUsed;
      external0 = external0 || external;
      arrayBuffers0 = arrayBuffers0 || arrayBuffers;
      log(
        'dup', data?.key, bloomFilter.length,
        'rss', (rss/rss0).toFixed(2),
        'ht', (heapTotal/heapTotal0).toFixed(2),
        'hu', (heapUsed/heapUsed0).toFixed(2),
        'ext', (external/external0).toFixed(2),
        'ab', (arrayBuffers/arrayBuffers0).toFixed(2)
      );
      return false;
    }
  })),
  ([req, res, data]) => data?.type === '0'
);

batchedBlocks$ = blocks$.pipe(
  map(([req, res, data]) => {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({status: 'accepted'}));
    return data
  }),
  filter(data => data?.type && data?.key && data?.account && data?.hash_to_verify),
  bufferCount(Number(BATCH_SIZE)),
  mergeMap( async (data) => {
    log('blocks', data.length)
    if (data.length === 0) return;
    const r = await processNewHashBatch(data, contract);
    data.splice(0, data.length);
    return r;
  })
).subscribe( (data) => {
  server.getConnections((err, count) => {
    log('connections', count)
  })
  log('SEND hashes', data)
});

batchedXunis$ = xunis$.pipe(
  map(([req, res, data]) => {
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify({status: 'accepted'}));
    return data
  }),
  filter(data => data?.type && data?.key && data?.account && data?.hash_to_verify),
  bufferCount(Number(BATCH_SIZE)),
  mergeMap( async (data) => {
    log('xunis', data.length)
    if (data.length === 0) return;
    const r = await processHashBatch(data, contract, wallet.address);
    data.splice(0, data.length);
    return r;
  })
).subscribe( (data) => {
  server.getConnections((err, count) => {
    log('connections', count)
  })
  log('SEND xunis', data)
});

server.listen(PORT, '0.0.0.0', 100,
  () => {
    log(`Server is running on port ${PORT}`);
  });