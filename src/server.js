import express from 'express';
import bodyParser from 'body-parser';
import dotenv from "dotenv";
import debug from "debug";
import BlockStorage from "../abi/BlockStorage_v1.json";
import {Contract, JsonRpcProvider, NonceManager, Wallet} from "ethers";
import {processHash} from "./processHash.js";

const [,, ...args] = process.argv;

dotenv.config({ path: args[0] || '.env' });
debug.enable('*,-body-parser:*');

const log = debug('hash-sync')
const abi = BlockStorage.abi;

const RPC_URL = process.env.RPC_URL || 'https://x1-testnet.infrafc.org';
const NETWORK_ID = process.env.NETWORK_ID || '204005';
// const MAX_RETRIES = process.env.MAX_RETRIES || '20';
const PORT = process.env.PORT || 9997;
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;

log('using RPC', RPC_URL)
log('using network', NETWORK_ID)
log('using contract', CONTRACT_ADDRESS)
log('using listen port', PORT)

const provider = new JsonRpcProvider(RPC_URL, Number(NETWORK_ID));
const wallet = new Wallet(process.env.PK, provider);
const nonceManager = new NonceManager(wallet);
const contract = new Contract(CONTRACT_ADDRESS, abi, nonceManager);

const app = express();

// Use middleware to parse JSON requests
app.use(bodyParser.json());

// Define a route for handling POST requests
app.post('/process_hash', async (req, res) => {
  log('RECV', req.body?.key);
  const txResult = await processHash(req.body, contract);
  if (txResult?.[1] === 0n) log('SEND', txResult?.[1]);
  res.json({ status: 'accepted' });
});

// Start the server
app.listen(PORT, () => {
  log(`Server is running on port ${PORT}`);
});