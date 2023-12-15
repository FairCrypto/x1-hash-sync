// import { RxHR } from '@akanass/rx-http-request';

import * as http from "http";

import dotenv from "dotenv";
import debug from "debug";
import BlockStorage from "../abi/BlockStorage.json";
import {Contract, JsonRpcProvider, NonceManager, Wallet} from "ethers";
import {processHash} from "./processHash.js";
import {bufferCount, flatMap, fromEvent, map, mergeMap, pipe, reduce, tap} from "rxjs";
import {processHashBatch} from "./processHashBatch.js";

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

log('using RPC', RPC_URL)
log('using network', NETWORK_ID)
log('using contract', CONTRACT_ADDRESS)
log('using listen port', PORT)

const provider = new JsonRpcProvider(RPC_URL, Number(NETWORK_ID));
const wallet = new Wallet(process.env.PK, provider);
const nonceManager = new NonceManager(wallet);
const contract = new Contract(CONTRACT_ADDRESS, abi, nonceManager);

const server = http.createServer();

fromEvent(server, 'request')
  .pipe(
    mergeMap(([req, res]) => {
      return fromEvent(req, 'data')
        .pipe(
          map((chunk) => chunk.toString()),
          map((body) => [req, res, JSON.parse(body)]),
          map(([req, res, data]) => {
            res.writeHead(200, {'Content-Type': 'application/json'});
            res.end(JSON.stringify({status: 'accepted'}));
            return data
          }),
        );
    }),
    bufferCount(2),
    // tap((data) => log('RECV', data)),
  )
  .subscribe(async (data) => {
    log(data);
    const txResult = await processHashBatch(data, contract);
    if (txResult === 0n) log('SEND', txResult);
  });


server.listen(PORT, '0.0.0.0', 100, () => {
  log(`Server is running on port ${PORT}`);
});