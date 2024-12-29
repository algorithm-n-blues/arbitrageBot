import aiohttp
import asyncio
import requests
from datetime import datetime
import logging
import os
import pandas as pd
from decimal import Decimal
from decouple import config
from aave.aave_data import fetch_aave_data
from pyth.pyth_data import get_pyth_data
from uniswap_v3.fetch_uniswap import fetch_top_uniswap_pools
from utils import save_uniswap_data_to_csv

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

def fetch_and_process_uniswap_data():
    logger.info("Fetching Uniswap pool data...")
    uniswap_data = fetch_top_uniswap_pools(UNISWAP_ARBITRUM_URL)

    # Ensure sqrtPrice values are non-zero
    uniswap_data = uniswap_data[uniswap_data["sqrtPrice"] > 0]

    # Calculate adjusted prices using sqrtPrice
    uniswap_data["adjusted_price_token1_per_token0"] = (
        (uniswap_data["sqrtPrice"] / (2 ** 96)) ** 2
    )
    uniswap_data["adjusted_price_token0_per_token1"] = 1 / uniswap_data["adjusted_price_token1_per_token0"]

    # Validate calculations by considering token decimals
    uniswap_data["adjusted_price_token1_per_token0_scaled"] = (
        uniswap_data["adjusted_price_token1_per_token0"] / (10 ** uniswap_data["token1_decimals"])
    )
    uniswap_data["adjusted_price_token0_per_token1_scaled"] = (
        uniswap_data["adjusted_price_token0_per_token1"] / (10 ** uniswap_data["token0_decimals"])
    )

    # Add a calculated constant_k column
    uniswap_data["constant_k"] = (
        uniswap_data["totalValueLockedToken0"] * uniswap_data["totalValueLockedToken1"]
    )

    # Log for debugging
    logger.debug("Logging detailed price calculations for Uniswap pools...")
    for index, row in uniswap_data.iterrows():
        logger.debug(
            f"Pool {row['id']}: sqrtPrice={row['sqrtPrice']}, "
            f"Price (Token1/Token0)={row['adjusted_price_token1_per_token0']}, "
            f"Scaled Price (Token1/Token0)={row['adjusted_price_token1_per_token0_scaled']}, "
            f"Price (Token0/Token1)={row['adjusted_price_token0_per_token1']}, "
            f"Scaled Price (Token0/Token1)={row['adjusted_price_token0_per_token1_scaled']}, "
            f"Constant k={row['constant_k']}"
        )

    # Validate TVL calculations
    uniswap_data["calculated_tvl_usd"] = (
        uniswap_data["totalValueLockedToken0"] * uniswap_data["price_token0_per_token1"] +
        uniswap_data["totalValueLockedToken1"] * uniswap_data["price_token1_per_token0"]
    )

    # Log discrepancies in x*y=k relationships
    logger.debug("Validating x*y=k relationships for Uniswap pools...")
    for index, row in uniswap_data.iterrows():
        expected_k = row["totalValueLockedToken0"] * row["totalValueLockedToken1"]
        calculated_k = row["constant_k"]
        if abs(expected_k - calculated_k) > 1e-10:
            logger.warning(f"Discrepancy in x*y=k for pool {row['id']}: Expected {expected_k}, Calculated {calculated_k}")

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
            pool_id = pool["id"]
            token0_symbol = pool["token0_symbol"].upper()
            token1_symbol = pool["token1_symbol"].upper()

            # Get Pyth prices and confidence data
            token0_data = pyth_data.loc[pyth_data["symbol"] == token0_symbol]
            token1_data = pyth_data.loc[pyth_data["symbol"] == token1_symbol]

            if token0_data.empty or token1_data.empty:
                logger.warning(f"Missing Pyth prices for {token0_symbol}/{token1_symbol}. Skipping pool {pool_id}.")
                continue

            token0_price = token0_data["Price"].values[0]
            token1_price = token1_data["Price"].values[0]
            token0_confidence = token0_data["Price Confidence"].values[0]
            token1_confidence = token1_data["Price Confidence"].values[0]

            # Adjust Pyth prices for token decimals
            token0_price_adjusted = token0_price / (10 ** pool["token0_decimals"])
            token1_price_adjusted = token1_price / (10 ** pool["token1_decimals"])

            # Calculate expected price based on Pyth data
            expected_pool_price = token0_price_adjusted / token1_price_adjusted

            # Validate prices
            token1_per_token0_uniswap = pool.get("adjusted_price_token1_per_token0", 0)
            if expected_pool_price == 0 or token1_per_token0_uniswap == 0:
                logger.warning(
                    f"Invalid price detected for pool {token0_symbol}/{token1_symbol}. "
                    f"Expected Price: {expected_pool_price}, Uniswap Price: {token1_per_token0_uniswap}"
                )
                continue

            # Calculate price deviation
            price_deviation = abs(expected_pool_price - token1_per_token0_uniswap) / expected_pool_price * 100

            # Debug detailed price information
            logger.debug(
                f"Pool: {token0_symbol}/{token1_symbol}, Token0 Price: {token0_price_adjusted}, "
                f"Token1 Price: {token1_price_adjusted}, Expected Price: {expected_pool_price}, "
                f"Uniswap Price: {token1_per_token0_uniswap}, Deviation: {price_deviation}%"
            )

            # Skip high deviations or suspicious data
            if price_deviation > 10000:
                logger.warning(
                    f"Skipping pool {token0_symbol}/{token1_symbol} due to excessive deviation: {price_deviation}%"
                )
                continue

            # Check liquidity
            token0_aave = aave_data[aave_data["symbol"] == token0_symbol]
            token1_aave = aave_data[aave_data["symbol"] == token1_symbol]

            liquidity_token0 = token0_aave["availableLiquidity"].values[0] if not token0_aave.empty else 0
            liquidity_token1 = token1_aave["availableLiquidity"].values[0] if not token1_aave.empty else 0

            if liquidity_token0 <= 10 or liquidity_token1 <= 10:
                logger.warning(f"Insufficient liquidity for {token0_symbol}/{token1_symbol}. Skipping pool {pool_id}.")
                continue

            opportunities.append({
                "Pool (Token0 / Token1)": f"{token0_symbol}/{token1_symbol}",
                "Pool ID": pool_id,
                "Token0 Symbol": token0_symbol,
                "Token0 Pyth Price": token0_price,
                "Token0 Confidence": token0_confidence,
                "Token0 Uniswap Expected Price": token0_price_adjusted,
                "Token1 Symbol": token1_symbol,
                "Token1 Pyth Price": token1_price,
                "Token1 Confidence": token1_confidence,
                "Token1 Uniswap Expected Price": token1_price_adjusted,
                "Expected Price (Token1/Token0)": expected_pool_price,
                "Uniswap Price (Token1/Token0)": token1_per_token0_uniswap,
                "Price Deviation (%)": round(price_deviation, 6),
                "Liquidity Token0": liquidity_token0,
                "Liquidity Token1": liquidity_token1,
                "TVL (USD)": pool["calculated_tvl_usd"],
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
        missing_datasets = []
        if pyth_data.empty:
            missing_datasets.append("Pyth data")
        if uniswap_data.empty:
            missing_datasets.append("Uniswap data")
        if aave_data.empty:
            missing_datasets.append("Aave data")
        logger.warning(f"Insufficient data to simulate arbitrage. Missing datasets: {', '.join(missing_datasets)}")
        return

    # Analyze opportunities
    opportunities = analyze_arbitrage_opportunities(pyth_data, uniswap_data, aave_data)
    logger.info(f"Simulation completed. Opportunities:\n{opportunities}")

def main():
    simulate_arbitrage()

if __name__ == "__main__":
    main()
