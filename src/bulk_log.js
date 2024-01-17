import {Contract, Wallet, solidityPacked, JsonRpcProvider, NonceManager, getAddress, isAddress} from "ethers";
import path from "path";
import dotenv from 'dotenv';
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import debug from "debug";
import assert from "assert";
import BlockStorage from "../abi/BlockStorage_v2.json" assert { type: "json" };
import pako from 'pako';
import {processNewLogBatch} from "./processLogBatch.js";

const [,, ...args] = process.argv;

dotenv.config({ path: args[0] || '.env' });

debug.enable('*');

const log = debug('hash-sync')
const DB_LOCATION = process.env.DB_LOCATION || './blockchain.db';
const RPC_URL = process.env.RPC_URL || 'https://x1-testnet.infrafc.org';
const NETWORK_ID = process.env.NETWORK_ID || '204005';
const STARTING_HASH_ID = process.env.STARTING_HASH_ID || '0';
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;
const BATCH_SIZE = process.env.BATCH_SIZE ? Number(process.env.BATCH_SIZE) || 60 : 60;

async function* getNextHash(db, offset = 0) {
  let off = offset;
  let rows = [];
  do {
    try {
      const sql = `
        SELECT block_id, hash_to_verify, key, account, created_at 
		    FROM blocks 
		    ORDER BY block_id ASC 
		    LIMIT ${BATCH_SIZE}
		    OFFSET ${offset};
      `;
      rows = await db.all(sql);
      yield rows;
    } catch (e) {
      log(e)
      yield [];
    }
    offset += BATCH_SIZE;
  } while (rows.length > 0)
}

let db;

// entry point
(async () => {
  const abi = BlockStorage.abi;

  process.on('SIGINT', () => {
    log('SIGINT received');
    db && db.close();
    log('db closed');
    process.exit(1);
  });

  log('using DB', DB_LOCATION)
  log('using RPC', RPC_URL)
  log('using network', NETWORK_ID)
  log('using contract', CONTRACT_ADDRESS)

  db = await open({
    filename: path.resolve(DB_LOCATION),
    driver: sqlite3.Database,
    mode: sqlite3.OPEN_READONLY
  });

  log('db open')

  const provider = new JsonRpcProvider(RPC_URL, Number(NETWORK_ID));
  const wallet = new Wallet(process.env.PK, provider);
  // const nonceManager = new NonceManager(wallet);
  const contract = new Contract(CONTRACT_ADDRESS, abi, wallet);
  await new Promise(resolve => setTimeout(resolve, 1000));
  // await nonceManager.getNonce()

  for await (const hashes of getNextHash(db, Number(STARTING_HASH_ID))) {
    if (!hashes || !Array.isArray(hashes) || !hashes.length) {
      log( 'no records; skipping');
      await new Promise(resolve => setTimeout(resolve, 100));
      continue;
    }
    const res = await processNewLogBatch(hashes, contract);
    log('processed', res);
    await new Promise(resolve => setTimeout(resolve, 100));
  }


})()
  .catch(log)
  .finally(() => {
    db.close();
    log('db closed')
  })
