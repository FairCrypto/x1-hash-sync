import express from 'express';
import bodyParser from 'body-parser';
import dotenv from "dotenv";
import debug from "debug";
import BlockStorage from "../abi/BlockStorage.json";
import {Contract, JsonRpcProvider, NonceManager, Wallet} from "ethers";
import {processHash} from "./processHash.js";

const [,, ...args] = process.argv;

dotenv.config({ path: args[0] || '.env' });
debug.enable('*');

const log = debug('hash-sync')
const abi = BlockStorage.abi;

const RPC_URL = process.env.RPC_URL || 'https://x1-testnet.infrafc.org';
// const RPC_URL = process.env.RPC_URL || 'http://localhost:8546';
const NETWORK_ID = process.env.NETWORK_ID || '204005';
// const MAX_RETRIES = process.env.MAX_RETRIES || '20';
const PORT = process.env.PORT || 9997;

log('using RPC', RPC_URL)
log('using network', NETWORK_ID)
log('using listen port', PORT)

const provider = new JsonRpcProvider(RPC_URL, Number(NETWORK_ID));
const wallet = new Wallet(process.env.PK, provider);
const nonceManager = new NonceManager(wallet);
const contract = new Contract(process.env.CONTRACT_ADDRESS, abi, nonceManager);

const providerIsReady = new Promise((resolve, reject) => {
  provider.once('ready', resolve);
  provider.once('error', reject);
});

const app = express();

// Use middleware to parse JSON requests
app.use(bodyParser.json());

// Define a route for handling POST requests
app.post('/process_hash', async (req, res) => {
  // Print the received JSON data
  await providerIsReady;
  log('RECV', req.body?.key);
  const txResult = await processHash(req.body, contract);
  log('SEND', txResult);
  // Respond with a simple message
  res.json({ status: 'accepted' });
  // res.json({ status: 'accepted' });
});

// Start the server
app.listen(PORT, () => {
  log(`Server is running on port ${PORT}`);
});