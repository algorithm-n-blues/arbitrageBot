import asyncio
import aiohttp
import logging
from uniswap import Uniswap
from web3 import Web3
from aave.aave_data import fetch_aave_data, get_best_tokens_for_flash_loans
from pyth.pyth_data import fetch_pyth_network_prices, load_pyth_prices, save_pyth_data_to_csv

# Initialize logger
logging.basicConfig(level=logging.INFO)

# Infura setup for Uniswap
infura_url = "https://mainnet.infura.io/v3/8c665856b4df4e65ad71b86d08ec0a37"
w3 = Web3(Web3.HTTPProvider(infura_url))
uniswap = Uniswap(address=None, private_key=None, provider=infura_url, version=3)

# Sample symbol for testing - should match keys in `pyth_keys`
symbol = "ETH/USD"

async def fetch_all_data(pyth_network_filtered_keys):
    async with aiohttp.ClientSession() as session:
        # Fetch Aave data
        aave_data = await fetch_aave_data(session)
        best_token_aave = get_best_tokens_for_flash_loans(aave_data)
        
        # Fetch Pyth Network prices and save to CSV
        pyth_prices = await fetch_pyth_network_prices(session, pyth_network_filtered_keys)
        await save_pyth_data_to_csv(pyth_prices, "pyth_prices.csv")

        # Load Pyth data from CSV to maintain consistent access format
        loaded_pyth_prices = load_pyth_prices("pyth_prices.csv")
        
        # Uniswap token addresses
        eth_address = w3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")  # WETH
        usdc_address = w3.to_checksum_address("0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")  # USDC
        
        # Fetch Uniswap price for ETH/USDC
        uniswap_price_eth_usdc = uniswap.get_raw_price(eth_address, usdc_address, fee=3000)
        
        return aave_data, best_token_aave, loaded_pyth_prices, uniswap_price_eth_usdc

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

# Main execution function
async def main():
    # Define Pyth Network keys for filtered tokens
    pyth_network_filtered_keys = {
        "ETH/USD": "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
        "WBTC/USD": "c9d8b075a5c69303365ae23633d4e085199bf5c520a3b90fed1322a0342ffc33"
    }

    # Fetch data from Aave, Pyth, and Uniswap
    aave_data, best_token_aave, pyth_prices, uniswap_price_eth_usdc = await fetch_all_data(pyth_network_filtered_keys)
    
    # Compare prices for ETH/USD
    arbitrage_opportunity = compare_prices(symbol, uniswap_price_eth_usdc, pyth_prices)
    
    # Check Aave liquidity and finalize arbitrage opportunity
    if arbitrage_opportunity and check_aave_liquidity(aave_data, "wETH"):
        print("Arbitrage and liquidity conditions met.")
        print("Arbitrage Opportunity:", arbitrage_opportunity)
    else:
        print("Conditions not met for arbitrage.")

# Run the main function
asyncio.run(main())
