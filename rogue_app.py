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
DATA_DIRECTORY = "data2"
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

def simulate_arbitrage():
    logger.info("Starting arbitrage simulation.")

    # Fetch Pyth Network data using the existing utility
    pyth_data = get_pyth_data()
    logger.debug(f"Fetched Pyth Data: {pyth_data}")

    # Ensure Pyth data is valid
    if not pyth_data:
        logger.warning("No Pyth Network data available.")
        return pd.DataFrame()

    # Convert to Pandas DataFrame for further processing
    pyth_df = pd.DataFrame(pyth_data)

    # Calculate confidence intervals
    pyth_df["Upper Bound"] = pyth_df["Price"] + pyth_df["Price Confidence"]
    pyth_df["Lower Bound"] = pyth_df["Price"] - pyth_df["Price Confidence"]
    pyth_df["EMA Upper Bound"] = pyth_df["EMA Price"] + pyth_df["EMA Price Confidence"]
    pyth_df["EMA Lower Bound"] = pyth_df["EMA Price"] - pyth_df["EMA Price Confidence"]

    # Calculate percentage changes for variability
    pyth_df["Price % Change"] = ((pyth_df["Upper Bound"] - pyth_df["Lower Bound"]) / pyth_df["Price"]) * 100
    pyth_df["EMA % Change"] = ((pyth_df["EMA Upper Bound"] - pyth_df["EMA Lower Bound"]) / pyth_df["EMA Price"]) * 100

    # Log processed data for validation
    logger.debug(f"Processed Pyth Data with Confidence Intervals and Percent Changes:\n{pyth_df}")

    # Save Pyth data to CSV for further analysis
    pyth_file = os.path.join(DATA_DIRECTORY, "pyth_processed_data.csv")
    save_pyth_data_to_csv(pyth_df.to_dict("records"), pyth_file)
    logger.info(f"Processed Pyth data saved to {pyth_file}")

    # Return the DataFrame for further analysis
    return pyth_df

def main():
    results = simulate_arbitrage()
    if not results.empty:
        logger.info("Arbitrage Simulation Results:")
        logger.info(results)
    else:
        logger.warning("No arbitrage opportunities identified.")

if __name__ == "__main__":
    main()
