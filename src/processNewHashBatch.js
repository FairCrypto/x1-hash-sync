import assert from "assert";
import {solidityPacked, getAddress, isAddress} from "ethers";
import debug from "debug";

const log = debug('hash-sync', {colors: false});

const prepareBytes = (hash) => {
  const {hash_to_verify, key, account, block_id} = hash;
  const [, type, v0, mtp, s64, hash64] = hash_to_verify.split('$');
  // log(type, v0, mtp, 's=', s64, 'h=', hash64);
  assert.equal(type, 'argon2id');
  const v = v0.split('=')[1];
  assert.equal(v, '19');
  const [m0, t0, p0] = mtp.split(',');
  const m = m0.split('=')[1];
  const t = t0.split('=')[1];
  const c = p0.split('=')[1];
  const s = Buffer.from(s64, 'base64');
  const k = Buffer.from(key, 'hex').slice(0, 32);
  const accountNormalized = getAddress(account);
  assert.ok(isAddress(accountNormalized), 'account is not valid: ' + accountNormalized);
  const bytes = solidityPacked(
    ["uint8", "uint32", "uint8", "uint8", "bytes32", "bytes"],
    [c, m, t, v, k, s]);
  return [accountNormalized, block_id, bytes];
}

export const processNewHashBatch = async (hashes, contract) => {
  assert.ok(Array.isArray(hashes), 'hashes is not array');
  try {
    const params = hashes.map(prepareBytes)
      .reduce(
        (acc, [value1, value2, value3]) => {
          acc[0].push(value1);
          acc[1].push(value2);
          acc[2].push(value3);
          return acc;
        },
        [[], [], []]
      );

    const gas = await contract.bulkStoreNewRecords.estimateGas(params[0], params[2]);
    const res = await contract.bulkStoreNewRecords(params[0], params[2], {
      gasLimit: gas * 120n / 100n,
      maxFeePerGas: 10_000_000_000n,
      maxPriorityFeePerGas: 2_000_000_000n,
    });
    const result = await res.wait(1);
    return result?.status === 1 ? 'OK' : 'FAIL';
  } catch (e) {
    log('ERR', e.message);
    // throw e;
  }
}

export const processHashBatch = async (hashes, contract, address) => {
  assert.ok(Array.isArray(hashes), 'hashes is not array');
  try {
    const params = hashes.map(prepareBytes)
      .reduce(
        (acc, [value1, value2, value3]) => {
          acc[0].push(value1);
          acc[1].push(value2);
          acc[2].push(value3);
          return acc;
        },
        [[], [], []]
      );

    const gas = await contract.bulkStoreRecordBytesInc.estimateGas(address, params[2]);
    const res = await contract.bulkStoreRecordBytesInc(address, params[2], {
      gasLimit: gas * 120n / 100n,
      maxFeePerGas: 10_000_000_000n,
      maxPriorityFeePerGas: 2_000_000_000n,
    });
    const result = await res.wait(1);
    return result?.status === 1 ? 'OK' : 'FAIL';
  } catch (e) {
    log('ERR', address, e);
    // throw e;
  }
}
