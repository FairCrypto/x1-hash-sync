// import { RxHR } from '@akanass/rx-http-request';

import * as http from "http";
import * as rx from "rxjs";

import dotenv from "dotenv";
import debug from "debug";
import BlockStorage from "../abi/BlockStorage.json";
import {Contract, JsonRpcProvider, NonceManager, Wallet} from "ethers";
import {processHash} from "./processHash.js";
import {fromEvent} from "rxjs";

const [,, ...args] = process.argv;

dotenv.config({ path: args[0] || '.env' });
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
  .subscribe(async (req, res) => {
    log(req.method, req.url);
    if (req.method === 'POST' && req.url === '/process_hash') {
      const body = await rx.fromEvent(req, 'data')
        .pipe(rx.map((chunk) => chunk.toString()))
        .pipe(rx.reduce((acc, chunk) => acc + chunk))
        .toPromise();
      const data = JSON.parse(body);
      log(data)
      // log('RECV', data?.key);
      // const txResult = await processHash(data, contract);
      // if (txResult?.[1] === 0n) log('SEND', txResult?.[1]);
      // res.writeHead(200, {'Content-Type': 'application/json'});
      // res.end(JSON.stringify({ status: 'accepted' }));
    } else {
      // res.writeHead(404, {'Content-Type': 'application/json'});
      // res.end(JSON.stringify({ status: 'not found' }));
    }
  });

server.listen(PORT, () => {
  log(`Server is running on port ${PORT}`);
});