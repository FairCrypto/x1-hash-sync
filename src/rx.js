import { Contract, Wallet, solidityPacked, JsonRpcProvider, NonceManager} from "ethers";
import path from "path";
import dotenv from 'dotenv';
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import debug from "debug";
import assert from "assert";
import BlockStorage from "../abi/BlockStorage.json" assert { type: "json" };
import RxDatabase from "./rx/index.ts";

dotenv.config();

debug.enable('*');

const log = debug('hash-sync')
const DB_LOCATION = process.env.DB_LOCATION || './blockchain.db';
const RPC_URL = process.env.RPC_URL || 'https://x1-testnet.infrafc.org';
const NETWORK_ID = process.env.NETWORK_ID || '204005';

const sql = `
        SELECT block_id, hash_to_verify, key, account, created_at 
		    FROM blocks 
		    ORDER BY block_id DESC 
		    LIMIT 1;
      `;

async function* getNextHash(db) {
  while (true) {
    try {
      const row = await db.get(sql);
      yield row;
    } catch (e) {
      log(e)
    }
  }
}

let db;

// entry point
(async () => {
  const abi = BlockStorage.abi;

  log('using DB', DB_LOCATION)
  log('using RPC', RPC_URL)
  log('using network', NETWORK_ID)

  const rxDb = new RxDatabase(path.resolve(DB_LOCATION),  sqlite3.OPEN_READONLY)

  log('rxDb open')

  const provider = new JsonRpcProvider(RPC_URL, Number(NETWORK_ID));
  const wallet = new Wallet(process.env.PK, provider);
  const contract = new Contract(process.env.CONTRACT_ADDRESS, abi, wallet);

  rxDb.all(sql)
    .subscribe(log)

  while (true) {
    await new Promise(resolve => setTimeout(resolve, 100))
  }

})()
  .catch(log)
  .finally(() => {;
    log('db closed')
  })
