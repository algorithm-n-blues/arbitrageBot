from decouple import config
from web3 import Web3
from decimal import Decimal
from pyth.pyth_data import get_eth_usd_price
from uniswap_v3.uniswap_abi import UNISWAP_ABI  # Import the ABI

# Infura URLs
INFURA_ARBITRUM_MAINNET_URL = config("INFURA_ARBITRUM_MAINNET_URL")
INFURA_ETHEREUM_MAINNET_URL = config("INFURA_ETHEREUM_MAINNET_URL")
PRIVATE_KEY = config("MY_PRIVATE_KEY")
PUBLIC_ADDRESS = config("MY_PUBLIC_ADDRESS")
RECEIVING_PUBLIC_ADDRESS = config("RECEIVING_PUBLIC_ADDRESS")

# Uniswap Router Address
UNISWAP_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"  # Uniswap V3 Router Address

# Initialize Web3 connections
w3_ARBITRUM = Web3(Web3.HTTPProvider(INFURA_ARBITRUM_MAINNET_URL))
w3_ETHEREUM = Web3(Web3.HTTPProvider(INFURA_ETHEREUM_MAINNET_URL))

def connect_to_network(w3, name):
    if w3.is_connected():
        print(f"Connected to {name}: {w3.client_version}")
        return True
    else:
        print(f"Failed to connect to {name}.")
        return False

def get_wallet_balance(w3, address):
    balance = w3.eth.get_balance(address)
    return Decimal(str(w3.from_wei(balance, 'ether')))

def execute_trade(w3, token_in, token_out, amount_in, slippage, recipient, chain="Ethereum"):
    """
    Execute a Uniswap trade using the exactInputSingle function.
    """
    try:
        # Uniswap contract
        uniswap = w3.eth.contract(address=UNISWAP_ROUTER, abi=UNISWAP_ABI)

        # Estimate output with slippage
        amount_out_min = int(amount_in * (1 - slippage) * (10**18))  # Convert to wei

        # Build the transaction
        txn = uniswap.functions.exactInputSingle(
            token_in,                         # Address of the input token
            token_out,                        # Address of the output token
            3000,                             # Fee tier (example: 0.3%)
            recipient,                        # Recipient address
            w3.eth.get_block("latest")["timestamp"] + 60,  # Deadline (current time + 60 seconds)
            w3.to_wei(amount_in, "ether"),   # Amount of input token (in wei)
            amount_out_min,                  # Minimum amount of output token (in wei)
            0                                # sqrtPriceLimitX96 (0 means no limit)
        ).buildTransaction({
            "chainId": w3.eth.chain_id,
            "gas": 250000,                   # Estimated gas
            "gasPrice": w3.eth.gas_price,    # Current gas price
            "nonce": w3.eth.get_transaction_count(PUBLIC_ADDRESS),
        })

        # Sign and send the transaction
        signed_txn = w3.eth.account.sign_transaction(txn, private_key=PRIVATE_KEY)
        txn_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        print(f"Trade executed! Transaction hash: {txn_hash.hex()} on {chain}")
    except Exception as e:
        print(f"Error executing trade on {chain}: {str(e)}")

if __name__ == "__main__":
    # Connect to Ethereum and Arbitrum
    if connect_to_network(w3_ETHEREUM, "Ethereum Mainnet"):
        eth_balance = get_wallet_balance(w3_ETHEREUM, PUBLIC_ADDRESS)
        print(f"Ethereum Wallet Balance: {eth_balance:.6f} ETH")

    if connect_to_network(w3_ARBITRUM, "Arbitrum Mainnet"):
        arb_balance = get_wallet_balance(w3_ARBITRUM, PUBLIC_ADDRESS)
        print(f"Arbitrum Wallet Balance: {arb_balance:.6f} ETH")

    # Fetch ETH/USD price
    eth_usd_price = get_eth_usd_price()
    if eth_usd_price:
        eth_usd_price_decimal = Decimal(str(eth_usd_price))
        print(f"Current ETH/USD Price: ${eth_usd_price:.2f}")

    # Execute a test trade on Arbitrum
    execute_trade(
        w3=w3_ARBITRUM,
        token_in="0x...",  # Replace with token address
        token_out="0x...",  # Replace with token address
        amount_in=0.001,    # Amount in ETH
        slippage=0.005,     # 0.5% slippage
        recipient=RECEIVING_PUBLIC_ADDRESS,
        chain="Arbitrum"
    )