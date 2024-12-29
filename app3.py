import aiohttp
import asyncio
import requests
from datetime import datetime
import logging
import os
import pandas as pd
import custom_streamlit as st
from decimal import Decimal
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
DATA_DIRECTORY = "data"
os.makedirs(DATA_DIRECTORY, exist_ok=True)

# Uniswap Graph API URL for Arbitrum
API_KEY = "e12c2830e44d2ed329aa22ec5a73fb81"  # Replace with your Graph Gateway API key
UNISWAP_ARBITRUM_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

async def fetch_aave_data_helper():
    async with aiohttp.ClientSession() as session:
        return await fetch_aave_data(session)

def fetch_uniswap_data():
    query = """
    {
      pools(first: 50, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id
        token0 {
          symbol
        }
        token1 {
          symbol
        }
        totalValueLockedUSD
        volumeUSD
        feeTier
      }
    }
    """
    try:
        response = requests.post(UNISWAP_ARBITRUM_URL, json={"query": query})
        if response.status_code != 200:
            logger.error(f"Uniswap API returned an error: {response.status_code}")
            logger.debug(f"Response: {response.text}")
            return pd.DataFrame()

        response_json = response.json()
        if "data" not in response_json or "pools" not in response_json["data"]:
            logger.error("Unexpected Uniswap API response format.")
            logger.debug(f"Response JSON: {response_json}")
            return pd.DataFrame()

        data = response_json["data"]["pools"]
        pools = []
        for pool in data:
            pools.append({
                "pair": f"{pool['token0']['symbol']}/{pool['token1']['symbol']}",
                "liquidity": float(pool["totalValueLockedUSD"]),
                "volume": float(pool["volumeUSD"]),
                "feeTier": float(pool["feeTier"]),
            })
        return pd.DataFrame(pools)

    except Exception as e:
        logger.error(f"Error fetching Uniswap data: {e}")
        return pd.DataFrame()

def analyze_price_changes(pyth_data, threshold=0.001):
    """
    Analyze Pyth Network price changes to identify arbitrage opportunities.

    Args:
        pyth_data (DataFrame): Pyth price data with columns ['symbol', 'Price'].
        threshold (float): Minimum price change to log and consider as an opportunity.

    Returns:
        DataFrame: Opportunities with price changes over a threshold.
    """
    opportunities = []
    for _, row in pyth_data.iterrows():
        symbol = row["symbol"]
        price = row["Price"]
        ema_price = row["EMA Price"]

        price_change = abs(price - ema_price) / ema_price
        logger.debug(f"Symbol: {symbol}, Price Change: {price_change:.6f}")
        
        if price_change > threshold:
            opportunities.append({
                "symbol": symbol,
                "price": price,
                "ema_price": ema_price,
                "price_change": price_change,
            })

    return pd.DataFrame(opportunities)

def simulate_arbitrage():
    logger.info("Starting arbitrage simulation.")

    # Fetch Pyth and Uniswap data
    pyth_data = pd.DataFrame(get_pyth_data())
    uniswap_data = fetch_uniswap_data()

    if pyth_data.empty or uniswap_data.empty:
        logger.warning("No data available for simulation.")
        return pd.DataFrame()

    logger.info("Pyth Network Data:")
    logger.info(pyth_data)

    logger.info("Uniswap Data:")
    logger.info(uniswap_data)

    # Analyze Pyth price changes
    opportunities = analyze_price_changes(pyth_data)

    if opportunities.empty:
        logger.info("No significant price changes detected.")
        return pd.DataFrame()

    logger.info("Identified Opportunities:")
    logger.info(opportunities)

    # Combine Pyth opportunities with Uniswap liquidity data
    results = []
    for _, opp in opportunities.iterrows():
        symbol = opp["symbol"]
        uniswap_pool = uniswap_data[uniswap_data["pair"].str.contains(symbol, case=False)]

        if not uniswap_pool.empty:
            liquidity = uniswap_pool.iloc[0]["liquidity"]
            volume = uniswap_pool.iloc[0]["volume"]
            fee_tier = uniswap_pool.iloc[0]["feeTier"]

            results.append({
                "symbol": symbol,
                "price": opp["price"],
                "ema_price": opp["ema_price"],
                "price_change": opp["price_change"],
                "liquidity": liquidity,
                "volume": volume,
                "fee_tier": fee_tier,
            })

    results_df = pd.DataFrame(results)
    logger.info("Arbitrage Opportunities:")
    logger.info(results_df)
    return results_df

def trigger_onchain_execution(arbitrage_results, test_mode=True):
    """
    Trigger the on-chain execution of trades based on arbitrage results.
    """
    for _, opportunity in arbitrage_results.iterrows():
        token_in = opportunity.get("token_in")  # Replace with mapping logic
        token_out = opportunity.get("token_out")  # Replace with mapping logic
        amount_in = Decimal("0.001")  # Example trade amount

        if test_mode:
            logger.info(f"Test mode: Preparing to execute trade for {opportunity['symbol']}.")
            logger.info(f"Token In: {token_in}, Token Out: {token_out}, Amount In: {amount_in}")
            break  # Only execute one trade in test mode
        else:
            execute_trade(
                w3=w3_ARBITRUM,
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                slippage=0.005,
                recipient=PUBLIC_ADDRESS,
                chain="Arbitrum"
            )


def main():
    results = simulate_arbitrage()
    if not results.empty:
        logger.info("Arbitrage Simulation Results:")
        logger.info(results)
        trigger_onchain_execution(results, test_mode=True)
    else:
        logger.warning("No arbitrage opportunities identified.")

if __name__ == "__main__":
    main()
