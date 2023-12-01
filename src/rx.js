import { Contract, Wallet, solidityPacked, JsonRpcProvider, NonceManager} from "ethers";
import path from "path";
import dotenv from 'dotenv';
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import debug from "debug";
import assert from "assert";
import BlockStorage from "../abi/BlockStorage.json" assert { type: "json" };
import rx, {distinctUntilChanged} from "rxjs";

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
      await new Promise(resolve => setTimeout(resolve, 200))
    } catch (e) {
      log(e)
    }
  }
}

let db;
let subs

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
  const contract = new Contract(process.env.CONTRACT_ADDRESS, abi, wallet);

  subs = rx.from(getNextHash(db))
    .pipe(distinctUntilChanged((a, b) => a.block_id === b.block_id))
    .subscribe(async (hash) => {
      try {
        const {block_id, hash_to_verify, key, account} = hash;
        const [, type, v0, mtp, s64, hash64] = hash_to_verify.split('$');
        log(type, v0, mtp, 's=', s64, 'h=', hash64);
        assert.equal(type, 'argon2id');
        const v = v0.split('=')[1];
        assert.equal(v, '19');
        const [m0, t0, p0] = mtp.split(',');
        const m = m0.split('=')[1];
        const t = t0.split('=')[1];
        const c = p0.split('=')[1];
        const s = Buffer.from(s64, 'base64');
        const k = Buffer.from(key, 'hex');
        // log(block_id, m, t, v, k, s)
        const bytes = solidityPacked(
          ["uint8", "uint32", "uint8", "uint8", "bytes32", "bytes"],
          [c, m, t, v, k, s]);
        const res = await contract.storeNewRecordBytes(account, bytes);
        log(block_id, '->', res.value)
      } catch (e) {
        log(e)
      }
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
