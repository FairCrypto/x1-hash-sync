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

async function* getNextHash(db) {
  while (true) {
    try {
      const sql = `
        SELECT block_id, hash_to_verify, key, account, created_at 
		    FROM blocks 
		    ORDER BY block_id DESC 
		    LIMIT 1;
      `;
      const row = await db.get(sql);
      yield row;
    } catch (e) {
      log(e)
    }
  }
}

// entry point
(async () => {
  const abi = BlockStorage.abi;

  log('using DB', DB_LOCATION)

  const db = await open({
    filename: path.resolve(DB_LOCATION),
    driver: sqlite3.Database,
    mode: sqlite3.OPEN_READONLY
  });

  log('db open')

  const provider = new JsonRpcProvider('https://x1-testnet.infrafc.org', 204005);
  const wallet = new Wallet(process.env.PK, provider);
  const contract = new Contract(process.env.CONTRACT_ADDRESS, abi, wallet);

  for await (const hash of getNextHash(db)) {
    try {
      const {block_id, hash_to_verify, key, account} = hash;
      const [, type, v, mtp, s64, hash64] = hash_to_verify.split('$');
      log(type, v, mtp, s64, hash64);
      assert.equal(type, 'argon2id');
      assert.equal(v, 'v=19');
      const [m0, t0, p0] = mtp.split(',');
      const m = m0.split('=')[1];
      const t = t0.split('=')[1];
      // const p = p0.split('=')[1];
      const s = Buffer.from(s64, 'base64');
      const k = Buffer.from(`0x${key}`, 'hex');
      log(k.length, s.length)
      const bytes = solidityPacked(
        ["uint8", "uint32", "uint8", "uint8", "bytes32", "bytes"],
        [block_id, m, t, v, k, s]);
      const res = await contract.storeNewRecordBytes(account, bytes);
      log(res.value)
      await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (e) {
      log(e);
    }
  }

})().catch(log)
