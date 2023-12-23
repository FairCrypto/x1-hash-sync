import * as BF from 'bloom-filters';

// source: https://gist.github.com/brandt/8f9ab3ceae37562a2841
// Optimal bloom filter size and number of hashes

// Tips:
  // 1. One byte per item in the input set gives about a 2% false positive rate.
// 2. The optimal number of hash functions is ~0.7x the number of bits per item.
// 3. The number of hashes dominates performance.

// Expected number of items in the collection
// n = (m * ln(2))/k;
const n = 2 ** 64;

// Acceptable false-positive rate (0.01 = 1%)
// p = e^(-(m/n) * (ln(2)^2));
const fpr = 0.01;

// Optimal size (number of elements in the bit array)
// m = -((n*ln(p))/(ln(2)^2));
const m = (n * Math.abs(Math.log(fpr))) / (Math.log(2) ** 2);

// Optimal number of hash functions
// k = (m/n) * ln(2);
const k = (m / n) * Math.log(2);

let bloomFilter;

export const initBloomFilter = (jsonStr) => {
  if (jsonStr) {
    try {
      const json = JSON.parse(jsonStr);
      bloomFilter = BF.BloomFilter.fromJSON(json)
      console.log('BloomFilter loaded');
    } catch (e) {
      console.error('BloomFilter loading error', e);
      bloomFilter = new BF.BloomFilter(m, k);
      console.log('init BloomFilter');
    }
  } else {
    bloomFilter = new BF.BloomFilter(m, k);
    console.log('init BloomFilter');
  }
  return bloomFilter;
}


