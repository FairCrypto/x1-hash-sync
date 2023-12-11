import { Contract, Wallet, JsonRpcProvider, NonceManager} from "ethers";
import path from "path";
import dotenv from 'dotenv';
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import debug from "debug";
import BlockStorage from "../abi/BlockStorage.json" assert { type: "json" };
import rx, {delay, distinctUntilChanged, from, map, Observable, retry, retryWhen, take, tap} from "rxjs";
import {processHash} from "./processHash.js";

const [,, ...args] = process.argv;

dotenv.config({ path: args[0] || '.env' });

debug.enable('*');

const log = debug('hash-sync')
const DB_LOCATION = process.env.DB_LOCATION || './blockchain.db';
const RPC_URL = process.env.RPC_URL || 'https://x1-testnet.infrafc.org';
// const RPC_URL = process.env.RPC_URL || 'http://localhost:8546';
const NETWORK_ID = process.env.NETWORK_ID || '204005';
const MAX_RETRIES = process.env.MAX_RETRIES || '20';

const sql = `
        SELECT block_id, hash_to_verify, key, account, created_at 
		    FROM blocks 
		    WHERE block_id > ?
		    ORDER BY block_id DESC 
		    LIMIT 30;
      `;

async function* getNextHash(db) {
  let lastProcessed = 0;
  while (true) {
    try {
      const rows = await db.all(sql, [lastProcessed]);
      if (rows && Array.isArray(rows) && rows.length > 0) {
        for (const row of rows) {
          if (row.block_id - lastProcessed > 1) {
            log('skipped!', row.block_id - 1)
          }
          if (row.block_id > lastProcessed) {
            lastProcessed = row?.block_id;
          }
          yield row;
        }
      }
      await new Promise(resolve => setTimeout(resolve, 10))
    } catch (e) {
      log(e)
    }
  }
}

let db;
let subs;

// entry point
(async () => {
  const abi = BlockStorage.abi;

  log('using DB', DB_LOCATION)
  log('using RPC', RPC_URL)
  log('using network', NETWORK_ID)

  db = await open({
    filename: path.resolve(DB_LOCATION),
    driver: sqlite3.Database,
    mode: sqlite3.OPEN_READONLY
  });

  log('db open')

  const provider = new JsonRpcProvider(RPC_URL, Number(NETWORK_ID));
  const wallet = new Wallet(process.env.PK, provider);
  const nonceManager = new NonceManager(wallet);
  const contract = new Contract(process.env.CONTRACT_ADDRESS, abi, nonceManager);

  subs = rx.from(getNextHash(db))
    .pipe(
      distinctUntilChanged((a, b) => a.block_id === b.block_id),
      map(async hash => {
        let res;
        try {
          res = await processHash(hash, contract)
          return res
        } catch (e) {
          log(e)
          return null
        }
      }),
      // retry({ maxRetryAttempts: Number(MAX_RETRIES), delay: 1_000 })
      retryWhen(errors =>
      {
        return errors.pipe(
          tap(val => console.log('errs', errors)),
          delay(1000), // You can adjust the delay between retries (in milliseconds)
        )
      })
    )
    .subscribe({
      next: async (o) => log((await o)?.[0], '>', (await o)?.[1]),
    });

  process.on('SIGTERM', () => {
    log('interrupt signal received');
    db.close();
    subs.unsubscribe();
    log('done')
  });

  // wait until interrupted
  while (true) {
    await new Promise(resolve => setTimeout(resolve, 10))
  }

})()
  .catch(log)
  .finally(() => {
    db.close();
    subs.unsubscribe();
    log('db closed')
  })
