import assert from "assert";
import {solidityPacked, getAddress, isAddress} from "ethers";
import debug from "debug";

const log = debug('hash-sync', {colors: false});

export const processHash = async (hash, contract) => {
  try {
    const {block_id, hash_to_verify, key, account} = hash;
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
    const gas = await contract.storeNewRecordBytes.estimateGas(accountNormalized, bytes);
    const res = await contract.storeNewRecordBytes(accountNormalized, bytes, {gasLimit: gas * 120n / 100n});
    return [block_id, res?.value];
  } catch (e) {
    log('ERR', e?.message);
    // throw e;
  }
}

