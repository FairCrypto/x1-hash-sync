import { Contract, Wallet, solidityPacked, JsonRpcProvider, NonceManager} from "ethers";
import path from "path";
import dotenv from 'dotenv';
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import debug from "debug";
import assert from "assert";
import BlockStorage from "../abi/BlockStorage.json" assert { type: "json" };

dotenv.config();

debug.enable('*');

const log = debug('hash-sync')
const DB_LOCATION = process.env.DB_LOCATION || './blockchain.db';
const RPC_URL = process.env.RPC_URL || 'https://x1-testnet.infrafc.org';
const NETWORK_ID = process.env.NETWORK_ID || '204005';

async function* getNextHash(db, offset = 0) {
  let rows = [];
  do {
    try {
      const sql = `
        SELECT block_id, hash_to_verify, key, account, created_at 
		    FROM blocks 
		    ORDER BY block_id ASC 
		    LIMIT 60;
		    OFFSET ${offset};
      `;
      rows = await db.all(sql);
      yield rows;
    } catch (e) {
      log(e)
      yield [];
    }
  } while (rows.length > 0)
}

let db;

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

  let offset = 0;
  for await (const hashes of getNextHash(db, offset)) {
    try {
      log('hashes', hashes)
      const bytes = hashes
        .map(hash => {
          const {hash_to_verify, key} = hash;
          const [, type, v0, mtp, s64, hash64] = hash_to_verify.split('$');
          assert.equal(type, 'argon2id');
          const v = v0.split('=')[1];
          assert.equal(v, '19');
          const [m0, t0, p0] = mtp.split(',');
          const m = m0.split('=')[1];
          const t = t0.split('=')[1];
          const c = p0.split('=')[1];
          const s = Buffer.from(s64, 'base64');
          const k = Buffer.from(key, 'hex');
          if (k.length !== 32) { // skip invalid keys
            return null;
          }
          return solidityPacked(
            ["uint8", "uint32", "uint8", "uint8", "bytes32", "bytes"],
            [c, m, t, v, k, s]);
        }).filter(Boolean);
      if (!bytes.length) {
        offset += 60;
        log('no bytes to send; skipping');
        await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }
      const res = await contract.bulkStoreRecordBytesInc(wallet.address, bytes);
      log(res.value)
      offset += 60;
      // await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (e) {
      log(e);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

})()
  .catch(log)
  .finally(() => {
    db.close();
    log('db closed')
  })
