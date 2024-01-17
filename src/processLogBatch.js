import assert from "assert";
import {getAddress, isAddress, solidityPacked} from "ethers";
import pako from "pako";
import debug from "debug";

const log = debug('hash-sync', {colors: false});

const unzip3 = (hashes) => hashes.reduce(
  (acc, [v1, v2, v3]) => {
    acc[0].push(v1);
    acc[1].push(v2);
    acc[2].push(v3);
    return acc;
  },
  [[], [], []]
);

export const processLogBatch = async (hashes, type, contract) => {
  assert.ok(Array.isArray(hashes), 'hashes is not array');

  try {
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
      return 'SKIP';
    }
    const [addresses, , bytes] = unzip3(zippedData);

    const blob = solidityPacked(["address[]", "bytes[]"], [addresses, bytes]);
    const deflated = pako.deflate(blob);

    const gas = await contract.logStoreRecords.estimateGas(
      type, Math.round(Date.now() / 1000), bytes.length, deflated
    );
    const res = await contract.logStoreRecords(
      type, Math.round(Date.now() / 1000), bytes.length, deflated, { gasLimit: gas * 120n / 100n }
    );
    const result = await res.wait(1);
    return result?.status === 1 ? 'OK' : 'FAIL';
  } catch (e) {
    log('ERR', e.message);
    // throw e;
  }
}

export const processNewLogBatch = async (hashes, type, contract) => {
  assert.ok(Array.isArray(hashes), 'hashes is not array');

  try {
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
      return 'SKIP';
    }
    const [addresses, , bytes] = unzip3(zippedData);

    const blob = solidityPacked(["address[]", "bytes[]"], [addresses, bytes]);
    const deflated = pako.deflate(blob);

    const gas = await contract.logStoreNewRecords.estimateGas(
      type, bytes.length, deflated
    );
    const res = await contract.logStoreNewRecords(
      type, bytes.length, deflated, { gasLimit: gas * 120n / 100n }
    );
    const result = await res.wait(1);
    return result?.status === 1 ? 'OK' : 'FAIL';
  } catch (e) {
    log('ERR', e.message);
    // throw e;
  }
}