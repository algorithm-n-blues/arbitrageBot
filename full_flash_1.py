import os
import logging
from dotenv import load_dotenv
from web3 import Web3
from web3.exceptions import ContractLogicError
from web3.middleware import geth_poa_middleware
from aave.aave_flashloan_abi import AAVE_POOL_ABI

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
AAVE_POOL_ADDRESS = "0x794a61358d6845594f94dc1db02a252b5b4814ad"

# Connect to Web3
ARBITRUM_RPC_URL = os.getenv("INFURA_ARBITRUM_MAINNET_URL")
PRIVATE_KEY = os.getenv("MY_PRIVATE_KEY")
PUBLIC_ADDRESS = os.getenv("MY_PUBLIC_ADDRESS")

if not ARBITRUM_RPC_URL or not PRIVATE_KEY or not PUBLIC_ADDRESS:
    logger.error("Environment variables are not set correctly.")
    exit(1)

try:
    w3_arbitrum = Web3(Web3.HTTPProvider(ARBITRUM_RPC_URL))
    w3_arbitrum.middleware_onion.inject(geth_poa_middleware, layer=0)
    network_id = w3_arbitrum.eth.chain_id
    logger.info(f"Connected to Arbitrum. Network ID: {network_id}")
except Exception as e:
    logger.error(f"Error connecting to Web3: {str(e)}")
    exit(1)

# Validate the AAVE Pool Address
AAVE_POOL_ADDRESS = Web3.to_checksum_address(AAVE_POOL_ADDRESS)

# Load AAVE Pool Contract
try:
    aave_pool = w3_arbitrum.eth.contract(address=AAVE_POOL_ADDRESS, abi=AAVE_POOL_ABI)
    logger.info("AAVE Pool contract loaded successfully.")
except ValueError as e:
    logger.error(f"Error loading AAVE Pool contract: {str(e)}")
    exit(1)

def calculate_flash_loan_profit(asset, amount, buy_price, sell_price, gas_cost):
    """
    Calculate potential profit for a flash loan.
    """
    flash_loan_fee = amount * 0.0009  # Aave charges 0.09%
    total_cost = flash_loan_fee + gas_cost
    profit = (sell_price - buy_price) * amount - total_cost
    return profit

def execute_flash_loan_with_profit_check(asset, amount, buy_price, sell_price, mode, on_behalf_of, referral_code, min_profit_threshold):
    try:
        gas_price = w3_arbitrum.eth.gas_price
        estimated_gas = 2000000  # Estimated gas limit for the transaction
        gas_cost = gas_price * estimated_gas / 10**18  # Convert wei to ETH

        # Retrieve current ETH balance
        balance = w3_arbitrum.eth.get_balance(PUBLIC_ADDRESS) / 10**18
        logger.info(f"Current wallet balance: {balance:.6f} ETH")

        # Check if balance is sufficient
        if balance < gas_cost:
            logger.error(f"Insufficient balance for gas. Required: {gas_cost:.6f} ETH, Available: {balance:.6f} ETH")
            return

        # Calculate potential profit
        profit = calculate_flash_loan_profit(asset, amount, buy_price, sell_price, gas_cost)
        logger.info(f"Estimated Profit: {profit:.6f} ETH")

        if profit <= min_profit_threshold:
            logger.warning(f"Potential profit ({profit:.6f} ETH) is below the minimum threshold ({min_profit_threshold:.6f} ETH). Aborting.")
            return

        asset_checksum = Web3.to_checksum_address(asset)

        tx = aave_pool.functions.flashLoan(
            on_behalf_of,
            [asset_checksum],
            [Web3.to_wei(amount, "ether")],
            [mode],
            on_behalf_of,
            b"",  # Empty params
        ).build_transaction({
            "from": PUBLIC_ADDRESS,
            "gas": estimated_gas,
            "gasPrice": gas_price,
            "nonce": w3_arbitrum.eth.get_transaction_count(PUBLIC_ADDRESS),
        })

        signed_tx = w3_arbitrum.eth.account.sign_transaction(tx, private_key=PRIVATE_KEY)
        tx_hash = w3_arbitrum.eth.send_raw_transaction(signed_tx.rawTransaction)
        logger.info(f"Transaction sent: {tx_hash.hex()}")
        receipt = w3_arbitrum.eth.wait_for_transaction_receipt(tx_hash)
        logger.info(f"Transaction receipt: {receipt}")
        return receipt
    except ContractLogicError as cle:
        logger.error(f"Contract logic error: {str(cle)}")
    except Exception as e:
        logger.error(f"Unexpected error during flash loan: {str(e)}")

# Execute the flash loan
try:
    execute_flash_loan_with_profit_check(
        asset="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
        amount=1000,  # Example amount
        buy_price=1.0,  # Example buy price in ETH
        sell_price=1.01,  # Example sell price in ETH
        mode=0,  # Full debt mode
        on_behalf_of=PUBLIC_ADDRESS,
        referral_code=0,
        min_profit_threshold=0.01,  # Minimum acceptable profit in ETH
    )
except Exception as e:
    logger.error(f"Error executing flash loan: {str(e)}")
