import aiohttp
import asyncio
import requests
from datetime import datetime
import logging
import os
import pandas as pd
import custom_streamlit as st
from decimal import Decimal
from decouple import config
from aave.aave_data import fetch_aave_data, get_best_tokens_for_flash_loans
from pyth.pyth_data import get_pyth_data, save_pyth_data_to_csv
from uniswap_v3.fetch_uniswap import fetch_top_uniswap_pools, fetch_pool_details
from app_onchain import execute_trade
from utils import (
    add_timestamp,
    calculate_price_difference,
    calculate_weighted_tvl,
    filter_pyth_prices,
    get_last_updated_time,
    is_file_outdated,
    save_uniswap_data_to_csv,
)

# Initialize logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("debug.log"),
    ],
)
logger = logging.getLogger(__name__)

# Directory for saving data
DATA_DIRECTORY = "data3"
os.makedirs(DATA_DIRECTORY, exist_ok=True)

# Uniswap Graph API URL for Arbitrum
API_KEY = "e12c2830e44d2ed329aa22ec5a73fb81"  # Replace with your Graph Gateway API key
UNISWAP_ARBITRUM_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

# Infura URLs
INFURA_ARBITRUM_MAINNET_URL = config("INFURA_ARBITRUM_MAINNET_URL")
INFURA_ETHEREUM_MAINNET_URL = config("INFURA_ETHEREUM_MAINNET_URL")
PRIVATE_KEY = config("MY_PRIVATE_KEY")
PUBLIC_ADDRESS = config("MY_PUBLIC_ADDRESS")
RECEIVING_PUBLIC_ADDRESS = config("RECEIVING_PUBLIC_ADDRESS")

def fetch_and_process_uniswap_data():
    logger.info("Fetching Uniswap pool data...")
    uniswap_data = fetch_top_uniswap_pools(UNISWAP_ARBITRUM_URL)
    if uniswap_data.empty:
        logger.warning("No Uniswap pool data available.")
        return pd.DataFrame()
    
    # Save Uniswap data to CSV
    uniswap_file = os.path.join(DATA_DIRECTORY, "uniswap_top_pools.csv")
    save_uniswap_data_to_csv(uniswap_data, uniswap_file)
    logger.info(f"Uniswap data saved to {uniswap_file}")
    return uniswap_data

def fetch_and_process_aave_data():
    logger.info("Fetching Aave token data...")
    async def fetch_aave_data_async():
        async with aiohttp.ClientSession() as session:
            return await fetch_aave_data(session)
    aave_data = asyncio.run(fetch_aave_data_async())
    if aave_data.empty:
        logger.warning("No Aave token data available.")
        return pd.DataFrame()
    
    # Save Aave data to CSV
    aave_file = os.path.join(DATA_DIRECTORY, "aave_token_data.csv")
    aave_data.to_csv(aave_file, index=False)
    logger.info(f"Aave data saved to {aave_file}")
    return aave_data

def analyze_arbitrage_opportunities(pyth_data, uniswap_data, aave_data):
    logger.info("Analyzing arbitrage opportunities...")
    opportunities = []

    for _, pool in uniswap_data.iterrows():
        try:
            token0_symbol = pool["token0_symbol"].upper()
            token1_symbol = pool["token1_symbol"].upper()
            pool_id = pool["id"]

            # Get Pyth prices
            token0_price = pyth_data.loc[pyth_data["symbol"] == token0_symbol, "Price"].values
            token1_price = pyth_data.loc[pyth_data["symbol"] == token1_symbol, "Price"].values

            if len(token0_price) == 0 or len(token1_price) == 0:
                logger.warning(f"Missing Pyth prices for {token0_symbol}/{token1_symbol}. Skipping pool {pool_id}.")
                continue

            token0_price = token0_price[0]
            token1_price = token1_price[0]

            # Calculate expected pool price and compare with Uniswap price
            expected_pool_price = token0_price / token1_price
            uniswap_price = pool["price_token1_per_token0"]
            price_deviation = abs(expected_pool_price - uniswap_price) / expected_pool_price * 100

            # Aave liquidity and rates
            token0_aave = aave_data[aave_data["symbol"] == token0_symbol]
            token1_aave = aave_data[aave_data["symbol"] == token1_symbol]

            liquidity_token0 = token0_aave["availableLiquidity"].values[0] if not token0_aave.empty else 0
            liquidity_token1 = token1_aave["availableLiquidity"].values[0] if not token1_aave.empty else 0

            opportunities.append({
                "Pool": f"{token0_symbol}/{token1_symbol}",
                "Expected Price": expected_pool_price,
                "Uniswap Price": uniswap_price,
                "Price Deviation (%)": price_deviation,
                "Liquidity Token0": liquidity_token0,
                "Liquidity Token1": liquidity_token1,
                "TVL (USD)": pool["totalValueLockedUSD"],
            })
        except Exception as e:
            logger.error(f"Error analyzing pool {pool['id']}: {e}")

    opportunities_df = pd.DataFrame(opportunities)

    # Save opportunities to CSV
    opportunities_file = os.path.join(DATA_DIRECTORY, "arbitrage_opportunities.csv")
    opportunities_df.to_csv(opportunities_file, index=False)
    logger.info(f"Arbitrage opportunities saved to {opportunities_file}")

    return opportunities_df

def simulate_arbitrage():
    logger.info("Starting arbitrage simulation.")

    # Fetch Pyth Network data
    pyth_data = pd.DataFrame(get_pyth_data())  # Convert the list to a DataFrame

    # Fetch Uniswap data
    uniswap_data = fetch_and_process_uniswap_data()

    # Fetch Aave data
    aave_data = fetch_and_process_aave_data()

    # Validate if data is sufficient
    if pyth_data.empty or uniswap_data.empty or aave_data.empty:
        logger.warning("Insufficient data to simulate arbitrage.")
        return

    # Analyze opportunities
    opportunities = analyze_arbitrage_opportunities(pyth_data, uniswap_data, aave_data)
    logger.info(f"Simulation completed. Opportunities:\n{opportunities}")

def main():
    simulate_arbitrage()

if __name__ == "__main__":
    main()
