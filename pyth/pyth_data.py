import asyncio
import aiohttp
import logging
import os
import pandas as pd
from uniswap import Uniswap
from web3 import Web3
from aave.aave_data import fetch_aave_data, get_best_tokens_for_flash_loans

# Initialize logger
logging.basicConfig(level=logging.INFO)

# Infura setup for Uniswap
infura_url = "https://mainnet.infura.io/v3/8c665856b4df4e65ad71b86d08ec0a37"
w3 = Web3(Web3.HTTPProvider(infura_url))
uniswap = Uniswap(address=None, private_key=None, provider=infura_url, version=3)

# Sample symbol for testing - should match keys in `pyth_keys`
symbol = "ETH/USD"

# Function to compare prices and check for arbitrage opportunities
def compare_prices(symbol, uniswap_price, pyth_prices):
    # Find the Pyth price for the same symbol
    pyth_price_data = pyth_prices.get(symbol)
    if not pyth_price_data:
        logging.warning(f"No Pyth price data found for symbol: {symbol}")
        return None

    pyth_price = pyth_price_data["Price"]
    price_difference = abs(uniswap_price - pyth_price)
    logging.info(f"Uniswap Price: {uniswap_price}, Pyth Price: {pyth_price}, Difference: {price_difference}")

    # Check for arbitrage potential
    if price_difference > 10:  # Arbitrary threshold, adjust as needed
        logging.info("Arbitrage opportunity detected!")
        return {
            "symbol": symbol,
            "uniswap_price": uniswap_price,
            "pyth_price": pyth_price,
            "price_difference": price_difference
        }
    else:
        logging.info("No significant arbitrage opportunity.")
        return None

# Function to check Aave liquidity
def check_aave_liquidity(aave_data, symbol):
    # Locate the token's liquidity in Aave
    for reserve in aave_data['reserves']:
        if reserve['symbol'].lower() == symbol.lower():
            available_liquidity = float(reserve['totalLiquidityUSD'])
            logging.info(f"Aave liquidity for {symbol}: {available_liquidity}")
            return available_liquidity > 100000  # Adjust as needed
    return False

# Save Pyth Data to CSV
def save_pyth_data_to_csv(pyth_data, filename):
    """
    Save Pyth Network price data to a CSV file.

    Args:
        pyth_data (list): List of price data dictionaries from Pyth Network.
        filename (str): File path to save the CSV.
    """
    # Ensure the data directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    if pyth_data:
        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(pyth_data)
        
        # Optional: Add timestamp column
        df["Timestamp"] = pd.Timestamp.now()
        
        # Save to CSV
        df.to_csv(filename, index=False)
        logging.info(f"Pyth Network price data saved to {filename}")
    else:
        logging.error("No Pyth Network data to save.")

# Example Usage
if __name__ == "__main__":
    # Sample data
    sample_pyth_data = [
        {"symbol": "ETH/USD", "Price": 3720.12, "Confidence": 0.1},
        {"symbol": "BTC/USD", "Price": 51200.00, "Confidence": 0.15},
    ]
    save_pyth_data_to_csv(sample_pyth_data, "data/pyth_prices.csv")

# Main execution function
async def main():
    # Define Pyth Network keys for filtered tokens
    pyth_network_filtered_keys = {
        "ETH/USD": "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
        "WBTC/USD": "c9d8b075a5c69303365ae23633d4e085199bf5c520a3b90fed1322a0342ffc33"
    }

# Run the main function
asyncio.run(main())
