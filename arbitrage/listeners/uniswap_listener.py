from web3 import Web3
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Logger configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Web3 connection
INFURA_URL = "https://arbitrum-mainnet.infura.io/v3/YOUR_INFURA_KEY"
web3 = Web3(Web3.HTTPProvider(INFURA_URL))

# Minimal Swap event ABI
SWAP_EVENT_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "sender", "type": "address"},
            {"indexed": False, "internalType": "uint256", "name": "amount0In", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "amount1In", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "amount0Out", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "amount1Out", "type": "uint256"},
            {"indexed": False, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
        ],
        "name": "Swap",
        "type": "event"
    }
]

# Example pool addresses for top 50 pairs (replace with actual pool addresses)
TOP_POOL_ADDRESSES = [
    "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",  # USDC/WETH
    "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed",  # WBTC/WETH
    # Add more pool addresses
]

# Price conversion utility
def sqrt_price_to_price(sqrt_price):
    return (sqrt_price / (2 ** 96)) ** 2

# Handle Swap event
def handle_swap_event(event, pool_address):
    args = event["args"]
    sqrt_price = args["sqrtPriceX96"]
    price = sqrt_price_to_price(sqrt_price)

    logger.info(
        f"Pool: {pool_address} | Price: {price:.6f} | Amount0In: {args['amount0In']} | Amount1In: {args['amount1In']} "
        f"| Amount0Out: {args['amount0Out']} | Amount1Out: {args['amount1Out']}"
    )

# Listener function
def listen_to_pool(pool_address):
    pool_contract = web3.eth.contract(address=pool_address, abi=SWAP_EVENT_ABI)
    event_filter = pool_contract.events.Swap.createFilter(fromBlock="latest")

    while True:
        for event in event_filter.get_new_entries():
            handle_swap_event(event, pool_address)

# Main function to run listeners for all pools
def start_uniswap_listeners():
    with ThreadPoolExecutor() as executor:
        for pool_address in TOP_POOL_ADDRESSES:
            executor.submit(listen_to_pool, pool_address)

if __name__ == "__main__":
    start_uniswap_listeners()
