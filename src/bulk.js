import {Contract, Wallet, solidityPacked, JsonRpcProvider, NonceManager, getAddress, isAddress} from "ethers";
import path from "path";
import dotenv from 'dotenv';
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import debug from "debug";
import assert from "assert";
import BlockStorage from "../abi/BlockStorage.json" assert { type: "json" };

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

const unzip3 = (hashes) => hashes.reduce(
    (acc, [v1, v2, v3]) => {
      acc[0].push(v1);
      acc[1].push(v2);
      acc[2].push(v3);
      return acc;
    },
    [[], [], []]
  );

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
    try {
      log(
        'hashes from', hashes[0]?.block_id, 'to', hashes[hashes.length - 1]?.block_id,
        // 'nonce', await nonceManager.getNonce()
      )
      const zippedData = hashes
        .map(hash => {
          const {hash_to_verify, key, account, block_id} = hash;
          const [, type, v0, mtp, s64, hash64] = hash_to_verify.split('$');
          assert.equal(type, 'argon2id');
          const v = v0.split('=')[1];
          assert.equal(v, '19');
          const [m0, t0, p0] = mtp.split(',');
          const m = m0.split('=')[1];
          const t = t0.split('=')[1];
          const c = p0.split('=')[1];
          const s = Buffer.from(s64, 'base64');
          const k = Buffer.from(key, 'hex').slice(0, 32);
          if (k.length > 32) { // skip invalid keys
            return null;
          }
          if (account.length !== 42) {
            return null // skip invalid accounts
          }
          const accountNormalized = getAddress(account);
          assert.ok(isAddress(accountNormalized), 'account is not valid: ' + accountNormalized);
          const bb = solidityPacked(
            ["uint8", "uint32", "uint8", "uint8", "bytes32", "bytes"],
            [c, m, t, v, k, s]);
          return [accountNormalized, block_id, bb];
        }).filter(Boolean);
      if (!zippedData.length) {
        log( 'no conforming records; skipping');
        // await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }
      const [addresses, hashIds, bytes] = unzip3(zippedData);
      // console.log(addresses);
      console.log(JSON.stringify(hashIds));
      bytes.forEach(b => console.log(b));
      const gas = await contract.bulkStoreNewRecords.estimateGas(addresses, hashIds, bytes);
      const res = await contract.bulkStoreNewRecords(addresses, hashIds, bytes, {
        gasLimit: gas * 120n / 100n,
      });
      const result = await res.wait(0);
      log(bytes.length, result?.status)
      await new Promise(resolve => setTimeout(resolve, 100));
    } catch (e) {
      log('error', e);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }


})()
  .catch(log)
  .finally(() => {
    db.close();
    log('db closed')
  })
