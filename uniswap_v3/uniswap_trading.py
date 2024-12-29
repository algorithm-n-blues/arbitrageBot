from uniswap import Uniswap
from web3 import Web3
from decouple import config

# Configuration from .env
INFURA_ARBITRUM_URL = config("INFURA_ARBITRUM_MAINNET_URL")
PRIVATE_KEY = config("MY_PRIVATE_KEY")  # Private key of the sender
PUBLIC_ADDRESS = config("MY_PUBLIC_ADDRESS")  # Public address of the sender

# Token contract addresses on Arbitrum
USDC_ADDRESS = "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"  # USDC on Arbitrum
WETH_ADDRESS = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"  # WETH on Arbitrum

# Initialize Web3 and Uniswap instance
web3 = Web3(Web3.HTTPProvider(INFURA_ARBITRUM_URL))
if not web3.is_connected():
    raise Exception("Failed to connect to the Arbitrum network.")

# Convert addresses to checksum format
USDC_ADDRESS = web3.to_checksum_address(USDC_ADDRESS)
WETH_ADDRESS = web3.to_checksum_address(WETH_ADDRESS)

uniswap = Uniswap(
    address=PUBLIC_ADDRESS,
    private_key=PRIVATE_KEY,
    provider=INFURA_ARBITRUM_URL,
    version=3,  # Using Uniswap v3
    default_slippage=0.01,  # 1% slippage
)

# Fetch balances
def get_balance(address, token_address=None):
    if token_address:
        try:
            balance = uniswap.get_token_balance(token_address)
            return balance / 10**6  # Adjust decimals for USDC
        except Exception as e:
            print(f"Error fetching token balance: {e}")
            return 0
    else:
        return web3.from_wei(web3.eth.get_balance(address), "ether")

# Wrap ETH into WETH
def wrap_eth(amount_eth):
    try:
        print(f"Attempting to wrap {amount_eth} ETH into WETH...")
        amount_wei = web3.to_wei(amount_eth, "ether")
        balance = web3.eth.get_balance(PUBLIC_ADDRESS)

        # Check for sufficient balance
        if balance < amount_wei:
            raise Exception(f"Insufficient ETH for wrapping. Balance: {web3.from_wei(balance, 'ether')} ETH, Required: {amount_eth} ETH")

        nonce = web3.eth.get_transaction_count(PUBLIC_ADDRESS)
        base_fee = web3.eth.fee_history(1, "latest")["baseFeePerGas"][-1]
        max_priority_fee = web3.to_wei("2", "gwei")
        max_fee = base_fee + max_priority_fee

        transaction = {
            "from": PUBLIC_ADDRESS,
            "to": WETH_ADDRESS,
            "value": amount_wei,
            "gasPrice": max_fee,
            "nonce": nonce,
            "data": web3.eth.contract(address=WETH_ADDRESS, abi=[
                {
                    "constant": False,
                    "inputs": [],
                    "name": "deposit",
                    "outputs": [],
                    "payable": True,
                    "stateMutability": "payable",
                    "type": "function"
                }
            ]).encodeABI(fn_name="deposit"),
            "chainId": 42161,
        }

        estimated_gas = web3.eth.estimate_gas(transaction)
        transaction["gas"] = estimated_gas + 50000  # Add buffer
        print(f"Transaction payload for wrapping ETH: {transaction}")

        signed_txn = web3.eth.account.sign_transaction(transaction, private_key=PRIVATE_KEY)
        tx_hash = web3.eth.send_raw_transaction(signed_txn.rawTransaction)
        web3.eth.wait_for_transaction_receipt(tx_hash)
        print(f"Wrapped {amount_eth} ETH into WETH. Transaction hash: {tx_hash.hex()}")
    except Exception as e:
        print(f"Error wrapping ETH: {e}")

# Approve Uniswap Router to spend WETH
def approve_token(token_address, spender_address, amount):
    try:
        print(f"Approving {spender_address} to spend {amount} of {token_address}...")
        token_contract = web3.eth.contract(address=token_address, abi=[
            {
                "constant": False,
                "inputs": [
                    {"name": "_spender", "type": "address"},
                    {"name": "_value", "type": "uint256"}
                ],
                "name": "approve",
                "outputs": [{"name": "", "type": "bool"}],
                "payable": False,
                "stateMutability": "nonpayable",
                "type": "function"
            }
        ])
        nonce = web3.eth.get_transaction_count(PUBLIC_ADDRESS)
        base_fee = web3.eth.fee_history(1, "latest")["baseFeePerGas"][-1]
        max_priority_fee = web3.to_wei("2", "gwei")
        max_fee = base_fee + max_priority_fee

        approval_tx = token_contract.functions.approve(spender_address, amount).build_transaction({
            "from": PUBLIC_ADDRESS,
            "nonce": nonce,
            "gasPrice": max_fee,
            "chainId": 42161,
        })

        estimated_gas = web3.eth.estimate_gas(approval_tx)
        approval_tx["gas"] = estimated_gas + 200000  # Increase buffer
        print(f"Approval transaction payload: {approval_tx}")

        signed_approval_tx = web3.eth.account.sign_transaction(approval_tx, private_key=PRIVATE_KEY)
        tx_hash = web3.eth.send_raw_transaction(signed_approval_tx.rawTransaction)
        web3.eth.wait_for_transaction_receipt(tx_hash)
        print(f"Approval transaction successful. Hash: {tx_hash.hex()}")
    except Exception as e:
        print(f"Error during approval: {e}")

# Trade ETH for USDC
# Trade ETH for USDC
def trade_eth_for_usdc(amount_eth):
    """
    Trades ETH for USDC on Uniswap by first wrapping ETH into WETH and then executing the trade.

    Args:
        amount_eth (float): Amount of ETH to trade for USDC.
    """
    try:
        # Convert ETH amount to Wei
        amount_in_wei = web3.to_wei(amount_eth, "ether")

        # Debugging: Print inputs to trade function
        print(f"Input Token (WETH): {WETH_ADDRESS}")
        print(f"Output Token (USDC): {USDC_ADDRESS}")
        print(f"Amount to trade (in Wei): {amount_in_wei}")

        # Wrap ETH into WETH
        wrap_eth(amount_eth)

        # Approve Uniswap Router to Spend WETH
        approve_token(WETH_ADDRESS, uniswap.router_address, amount_in_wei)

        # Execute the trade using Uniswap
        print(f"Executing trade for {amount_eth} ETH worth of WETH for USDC...")
        print(f"Debugging: Calling uniswap.make_trade with params:")
        print(f"  input_token: {WETH_ADDRESS}")
        print(f"  output_token: {USDC_ADDRESS}")
        print(f"  qty: {amount_in_wei}")
        print(f"  recipient: {PUBLIC_ADDRESS}")

        tx_hash = uniswap.make_trade(
            input_token=WETH_ADDRESS,  # WETH is used as the input token
            output_token=USDC_ADDRESS,
            qty=amount_in_wei,
            fee=3000,  # Uniswap v3 standard fee tier
            recipient=PUBLIC_ADDRESS,
        )
        print(f"Trade transaction sent! Hash: {tx_hash.hex()}")

        # Wait for the transaction receipt
        trade_receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
        if trade_receipt.status == 1:
            print("Trade executed successfully!")
        else:
            print("Trade failed.")
    except Exception as e:
        print(f"Error executing trade: {e}")
        # Debugging: Print details of the exception
        import traceback
        traceback.print_exc()

# Adjust default_slippage for testing
uniswap.default_slippage = 0.05  # Increase slippage tolerance to 5% for debugging

# Fetch initial balances
eth_balance = get_balance(PUBLIC_ADDRESS)
usdc_balance = get_balance(PUBLIC_ADDRESS, USDC_ADDRESS)
print(f"ETH Balance: {eth_balance} ETH")
print(f"USDC Balance: {usdc_balance} USDC")

# Specify amount of ETH to trade
ETH_TO_TRADE = 0.0001
trade_eth_for_usdc(ETH_TO_TRADE)

# Fetch updated balances
updated_eth_balance = get_balance(PUBLIC_ADDRESS)
updated_usdc_balance = get_balance(PUBLIC_ADDRESS, USDC_ADDRESS)
print(f"Updated ETH Balance: {updated_eth_balance} ETH")
print(f"Updated USDC Balance: {updated_usdc_balance} USDC")
