from web3 import Web3
from decouple import config

# Configuration from .env
INFURA_ARBITRUM_URL = config("INFURA_ARBITRUM_MAINNET_URL")
SENDER_PRIVATE_KEY = config("SENDER_PRIVATE_KEY")
SENDER_PUBLIC_ADDRESS = config("SENDER_PUBLIC_ADDRESS")
RECEIVER_PUBLIC_ADDRESS = config("RECEIVER_PUBLIC_ADDRESS")

# Initialize Web3
web3 = Web3(Web3.HTTPProvider(INFURA_ARBITRUM_URL))

# Check connection
if not web3.is_connected():
    raise Exception("Failed to connect to the Arbitrum network.")

# Confirm network details
network_id = web3.net.version
chain_id = web3.eth.chain_id
print(f"Connected to Network ID: {network_id}, Chain ID: {chain_id}")
if chain_id != 42161:
    raise Exception("Not connected to Arbitrum Mainnet.")

# Minimum ETH to send (in wei)
MINIMUM_ETH_WEI = web3.to_wei(0.0001, "ether")

# Function to fetch balance
def get_balance(address):
    balance_wei = web3.eth.get_balance(address)
    return web3.from_wei(balance_wei, "ether")

# Fetch balances
sender_balance = get_balance(SENDER_PUBLIC_ADDRESS)
receiver_balance = get_balance(RECEIVER_PUBLIC_ADDRESS)

print(f"Sender Balance: {sender_balance} ETH")
print(f"Receiver Balance: {receiver_balance} ETH")

# Fetch dynamic gas price and ensure a minimum of 1 Gwei
gas_price = max(web3.eth.gas_price, web3.to_wei("1", "gwei"))
gas_limit = 100000  # Increased gas limit for Arbitrum

# Calculate required sender balance
required_balance = MINIMUM_ETH_WEI + (gas_limit * gas_price)
if web3.eth.get_balance(SENDER_PUBLIC_ADDRESS) < required_balance:
    raise Exception("Insufficient balance for transaction and gas fees.")

# Create transaction
nonce = web3.eth.get_transaction_count(SENDER_PUBLIC_ADDRESS, "pending")
transaction = {
    "nonce": nonce,
    "to": RECEIVER_PUBLIC_ADDRESS,
    "value": MINIMUM_ETH_WEI,
    "gas": gas_limit,
    "gasPrice": gas_price,
}

print(f"Transaction Nonce: {nonce}")
print(f"Gas Price: {web3.from_wei(gas_price, 'gwei')} Gwei")

# Sign transaction
signed_txn = web3.eth.account.sign_transaction(transaction, private_key=SENDER_PRIVATE_KEY)

# Send transaction
try:
    tx_hash = web3.eth.send_raw_transaction(signed_txn.raw_transaction)
    print(f"Transaction hash: {web3.to_hex(tx_hash)}")

    # Wait for receipt
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    if receipt.status == 1:
        print("Transaction successful!")
    else:
        print("Transaction failed.")
except Exception as e:
    print(f"Error sending transaction: {e}")

# Fetch updated balances
updated_sender_balance = get_balance(SENDER_PUBLIC_ADDRESS)
updated_receiver_balance = get_balance(RECEIVER_PUBLIC_ADDRESS)

print(f"Updated Sender Balance: {updated_sender_balance} ETH")
print(f"Updated Receiver Balance: {updated_receiver_balance} ETH")
