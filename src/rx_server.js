import * as http from "http";
import dotenv from "dotenv";
import debug from "debug";
import BlockStorage from "../abi/BlockStorage_v0.json";
import {Contract, JsonRpcProvider, NonceManager, Wallet} from "ethers";
import {bufferCount, bufferTime, filter, from, fromEvent, map, merge, mergeMap, partition, tap, buffer} from "rxjs";
import {processHashBatch, processNewHashBatch} from "./processNewHashBatch.js";

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

let subscribe;
process.on('SIGINT', () => {
  log('SIGINT received, exiting');
  if (subscribe) subscribe.unsubscribe();
  process.exit(0);
})

const server = http.createServer();

fromEvent(server, 'request')
  .pipe(
    mergeMap(([req, res]) => {
      const records$ = fromEvent(req, 'data')
        .pipe(
          map((chunk) => chunk.toString()),
          map((body) => [req, res, JSON.parse(body)])
        );
      //const [blocks, xunis] = partition(
      //  records$,
      //  ([req, res, data]) => data.type === '0');
      //return merge(
      return records$.pipe(
          map(([req, res, data]) => {
            res.writeHead(200, {'Content-Type': 'application/json'});
            res.end(JSON.stringify({status: 'accepted'}));
            // console.log(data)
            return data
          }),
          tap((data) => log('block', data)),
          // bufferTime(10_000),
          bufferCount(Number(BATCH_SIZE)),
          tap((data) => log('batch', data)),
          map(data => ['0', data])
        )
        /*
        xunis.pipe(
          // tap(([req, res, data]) => log('xuni', data)),
          map(([req, res, data]) => {
            res.writeHead(200, {'Content-Type': 'application/json'});
            res.end(JSON.stringify({status: 'accepted'}));
            // console.log(data)
            return data
          }),
          bufferCount(Number(BATCH_SIZE)),
          // bufferTime(10_000),
          tap((data) => log('xuni batch', data)),
          map(data => ['1', data])
        ),
        2
         */
      //)
    })
  ).subscribe(async ([type, data]) => {
    const res = type = '0'
      ? await processNewHashBatch(data, contract)
      : await processHashBatch(data, contract, wallet.address);
    log('SEND', res)
});


server.listen(PORT, '0.0.0.0', 100,
  () => {
    log(`Server is running on port ${PORT}`);
  });