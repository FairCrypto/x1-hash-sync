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
const STARTING_HASH_ID = process.env.STARTING_HASH_ID || '0';

async function* getNextHash(db, offset = 0) {
  let off = offset;
  let rows = [];
  do {
    try {
      const sql = `
        SELECT block_id, hash_to_verify, key, account, created_at 
		    FROM blocks 
		    ORDER BY block_id ASC 
		    LIMIT 60
		    OFFSET ${offset};
      `;
      rows = await db.all(sql);
      yield rows;
    } catch (e) {
      log(e)
      yield [];
    }
    offset += 60;
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
  const nonceManager = new NonceManager(wallet);
  const contract = new Contract(process.env.CONTRACT_ADDRESS, abi, nonceManager);

  for await (const hashes of getNextHash(db, Number(STARTING_HASH_ID))) {
    try {
      // log('hashes', hashes.length)
      const addresses = hashes.map(hash => hash.address);
      const hashIds = hashes.map(hash => hash.block_id);
      const bytes = hashes
        .map(hash => {
          const {hash_to_verify, key, address, block_idy} = hash;
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
          if (k.length > 32) { // skip invalid keys
            return null;
          }
          return solidityPacked(
            ["uint8", "uint32", "uint8", "uint8", "bytes32", "bytes"],
            [c, m, t, v, k, s]);
        }).filter(Boolean);
      if (!bytes.length) {
        log(hashes[0]?.block_id, 'no conforming hashes; skipping');
        // await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }
      const res = await contract.bulkStoreNewRecords(addresses, hashIds, bytes);
      log(bytes.length, res.value)
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
