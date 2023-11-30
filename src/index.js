import { Contract, Wallet, solidityPacked, JsonRpcProvider, NonceManager} from "ethers";
import path from "path";
import dotenv from 'dotenv';
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import debug from "debug";

dotenv.config();

debug.enable('*');

const log = debug('hash-sync')
const DB_LOCATION = process.env.DB_LOCATION || './blockchain.db';

async function* getNextHash(db) {
  try {
    const sql = `
        SELECT id, hash_to_verify, key, account, created_at 
		    FROM blocks 
		    ORDER BY id DESC 
		    LIMIT 1;
      `;
    const row = await db.get(sql);
    yield row;
  } catch (e) {
    log(e)
  }
}

// entry point
(async () => {

  log('using DB', DB_LOCATION)

  const db = await open({
    filename: path.resolve(DB_LOCATION),
    driver: sqlite3.Database,
    mode: sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE
  });

  log('db open')


  for await (const hash of getNextHash(db)) {
    log(hash);
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

})().catch(log)
