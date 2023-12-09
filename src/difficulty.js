import BlockStorage from "./BlockStorage.json";
import {Contract, JsonRpcProvider} from "ethers";
import dotenv from "dotenv";

const [,, ...args] = process.argv;
dotenv.config({ path: args[0] || '.env' });

const RPC_URL = process.env.RPC_URL || 'https://x1-testnet.infrafc.org';
const NETWORK_ID = process.env.NETWORK_ID || '204005';

(async () => {
  const abi = BlockStorage.abi;

  const provider = new JsonRpcProvider(RPC_URL, Number(NETWORK_ID));
  const contract = new Contract(process.env.CONTRACT_ADDRESS, abi, provider);

  const res = await contract.difficulty();
  console.log(res.toString());

})().catch(e => console.log(e));