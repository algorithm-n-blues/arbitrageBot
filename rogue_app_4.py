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
from uniswap_v3.fetch_uniswap import fetch_top_uniswap_pools, calculate_uniswap_price
from utils import save_uniswap_data_to_csv
from arbitrage.analyze_opportunities import calculate_opportunities_with_deviation

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

    if uniswap_data.empty:
        logger.warning("No data fetched from Uniswap pools.")
        return uniswap_data

    # Ensure sqrtPrice values are non-zero
    logger.debug("Filtering out pools with sqrtPrice <= 0...")
    uniswap_data = uniswap_data[uniswap_data["sqrtPrice"] > 0]

    if uniswap_data.empty:
        logger.warning("No valid pools with non-zero sqrtPrice.")
        return uniswap_data

    # Calculate token prices using corrected logic
    logger.info("Calculating token prices...")
    uniswap_data["price_token1_per_token0"] = uniswap_data.apply(
        lambda row: calculate_uniswap_price(
            row["sqrtPrice"], row["token0_decimals"], row["token1_decimals"]
        ) if row["sqrtPrice"] > 0 else None,
        axis=1,
    )
    uniswap_data["price_token0_per_token1"] = uniswap_data["price_token1_per_token0"].apply(
        lambda x: round(1 / x, 8) if x and x > 0 else None
    )

    # Log all relevant intermediate values
    logger.debug("Intermediate price calculations:")
    for index, row in uniswap_data.iterrows():
        logger.debug(
            f"Pool {row['id']} - Pair: {row['token0_symbol']}/{row['token1_symbol']}, "
            f"SqrtPrice: {row['sqrtPrice']}, Decimals0: {row['token0_decimals']}, "
            f"Decimals1: {row['token1_decimals']}, "
            f"Token1/Token0 Price: {row['price_token1_per_token0']}, "
            f"Token0/Token1 Price: {row['price_token0_per_token1']}"
        )

    # Identify and log invalid price calculations
    invalid_prices = uniswap_data[uniswap_data["price_token1_per_token0"].isna() | (uniswap_data["price_token1_per_token0"] <= 0)]
    if not invalid_prices.empty:
        logger.warning("Invalid prices detected:")
        for index, row in invalid_prices.iterrows():
            logger.warning(
                f"Invalid Pool {row['id']} - Pair: {row['token0_symbol']}/{row['token1_symbol']}, "
                f"SqrtPrice: {row['sqrtPrice']}, Decimals0: {row['token0_decimals']}, "
                f"Decimals1: {row['token1_decimals']}"
            )

    # Add a calculated constant_k column
    uniswap_data["constant_k"] = (
        uniswap_data["totalValueLockedToken0"] * uniswap_data["totalValueLockedToken1"]
    )

    # Validate TVL calculations
    uniswap_data["calculated_tvl_usd"] = (
        uniswap_data["totalValueLockedToken0"] * uniswap_data["price_token0_per_token1"] +
        uniswap_data["totalValueLockedToken1"] * uniswap_data["price_token1_per_token0"]
    )

    # Log edge cases
    extreme_prices = uniswap_data[(uniswap_data["price_token1_per_token0"] < 1e-12) | (uniswap_data["price_token1_per_token0"] > 1e12)]
    if not extreme_prices.empty:
        logger.info("Pools with extreme price values:")
        for index, row in extreme_prices.iterrows():
            logger.info(
                f"Extreme Pool {row['id']} - Pair: {row['token0_symbol']}/{row['token1_symbol']}, "
                f"Token1/Token0 Price: {row['price_token1_per_token0']}, "
                f"Token0/Token1 Price: {row['price_token0_per_token1']}"
            )

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

            # Calculate expected price based on Pyth data without additional adjustments
            if token1_price == 0:
                logger.warning(f"Division by zero error for {token0_symbol}/{token1_symbol}. Skipping pool {pool_id}.")
                continue

            expected_pool_price = token0_price / token1_price

            # Validate prices
            token1_per_token0_uniswap = pool.get("price_token1_per_token0", 0)
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
                f"Pool: {token0_symbol}/{token1_symbol}, Token0 Price: {token0_price}, "
                f"Token1 Price: {token1_price}, Expected Price: {expected_pool_price}, "
                f"Uniswap Price: {token1_per_token0_uniswap}, Deviation: {price_deviation}%"
            )

            # Skip high deviations or suspicious data
            #if price_deviation > 10000:
            #    logger.warning(
            #        f"Skipping pool {token0_symbol}/{token1_symbol} due to excessive deviation: {price_deviation}%"
            #    )
            #    continue

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
                "Token1 Symbol": token1_symbol,
                "Token1 Pyth Price": token1_price,
                "Token1 Confidence": token1_confidence,
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

    # Analyze opportunities with deviation analysis
    opportunities = calculate_opportunities_with_deviation(
        pyth_data=pyth_data, 
        uniswap_pools=uniswap_data, 
        aave_data=aave_data, 
        deviation_threshold=1.0  # Example threshold
    )

    if opportunities.empty:
        logger.info("No profitable opportunities found with the given deviation threshold.")
    else:
        logger.info(f"Simulation completed. Opportunities:\n{opportunities}")

    # Save results to CSV
    opportunities_file = os.path.join(DATA_DIRECTORY, "arbitrage_opportunities_with_deviation.csv")
    opportunities.to_csv(opportunities_file, index=False)
    logger.info(f"Opportunities saved to {opportunities_file}")

def main():
    simulate_arbitrage()

if __name__ == "__main__":
    main()
