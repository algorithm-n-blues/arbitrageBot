import aiohttp
import asyncio
import logging
import os
import pandas as pd
from decimal import Decimal
from decouple import config
from aave.aave_data import fetch_aave_data
from pyth.pyth_data import get_pyth_data
from uniswap_v3.fetch_uniswap import fetch_top_uniswap_pools
from utils import calculate_price_difference, calculate_trade_cost, save_uniswap_data_to_csv

# Initialize logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("analyze_opportunities.log"),
    ],
)
logger = logging.getLogger(__name__)

# Configuration
DATA_DIRECTORY = "data"
os.makedirs(DATA_DIRECTORY, exist_ok=True)

# Infura URL
INFURA_ARBITRUM_MAINNET_URL = config("INFURA_ARBITRUM_MAINNET_URL")
UNISWAP_ARBITRUM_URL = config("UNISWAP_ARBITRUM_URL")

# Helper Functions
def fetch_and_prepare_data():
    """
    Fetch data from Pyth, Uniswap, and Aave.
    Returns:
        tuple: Pyth data, Uniswap pools, Aave data.
    """
    try:
        logger.info("Fetching Pyth data...")
        pyth_data = pd.DataFrame(get_pyth_data())
        logger.debug(f"Pyth Data Sample:\n{pyth_data.head()}")

        logger.info("Fetching Uniswap pools...")
        uniswap_pools = fetch_top_uniswap_pools(UNISWAP_ARBITRUM_URL)
        logger.debug(f"Uniswap Pools Sample:\n{uniswap_pools.head()}")

        logger.info("Fetching Aave data...")
        async def fetch_aave_data_helper():
            async with aiohttp.ClientSession() as session:
                return await fetch_aave_data(session)

        aave_data = asyncio.run(fetch_aave_data_helper())
        logger.debug(f"Raw Aave Data: {aave_data}")

        # Validate Aave data
        if isinstance(aave_data, pd.DataFrame):
            if aave_data.empty:
                logger.warning("Aave DataFrame is empty.")
            else:
                logger.debug(f"Aave Data Columns: {aave_data.columns}")
                logger.debug(f"Aave Data Sample:\n{aave_data.head()}")
        else:
            logger.warning("Aave data is not a DataFrame or is None.")

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    return pyth_data, uniswap_pools, aave_data

def calculate_opportunities(pyth_data, uniswap_pools, aave_data):
    """
    Analyze data to find arbitrage opportunities.

    Args:
        pyth_data (DataFrame): Pyth price data.
        uniswap_pools (DataFrame): Uniswap pool data.
        aave_data (DataFrame): Aave token data.

    Returns:
        DataFrame: Opportunities with costs and profitability.
    """
    opportunities = []

    # Normalize Pyth symbols
    pyth_data["symbol"] = pyth_data["symbol"].str.upper().str.strip()

    # Ensure required columns are present in Uniswap pools
    required_columns = {"token0_symbol", "token1_symbol", "id"}
    if not required_columns.issubset(uniswap_pools.columns):
        logger.error(f"Missing expected columns in Uniswap pools: {required_columns - set(uniswap_pools.columns)}")
        return pd.DataFrame()

    # Normalize Aave symbols
    if not aave_data.empty:
        aave_data["symbol"] = aave_data["symbol"].str.upper().str.strip()

    logger.debug("Starting calculation of opportunities...")
    logger.debug(f"Pyth DataFrame Columns: {pyth_data.columns}")
    logger.debug(f"Uniswap DataFrame Columns: {uniswap_pools.columns}")
    logger.debug(f"Aave DataFrame Columns: {aave_data.columns if not aave_data.empty else 'Empty Aave DataFrame'}")

    for _, pool in uniswap_pools.iterrows():
        pool_id = pool["id"]
        token0_symbol = pool["token0_symbol"]
        token1_symbol = pool["token1_symbol"]

        if not token0_symbol or not token1_symbol:
            logger.warning(f"Missing token symbols for pool {pool_id}. Skipping...")
            continue

        # Create pair symbol
        pair_symbol = f"{token0_symbol}/{token1_symbol}"

        # Get Pyth prices for both tokens
        try:
            token0_pyth_price = Decimal(
                pyth_data[pyth_data["symbol"] == token0_symbol]["Price"].values[0]
            )
            token1_pyth_price = Decimal(
                pyth_data[pyth_data["symbol"] == token1_symbol]["Price"].values[0]
            )
        except (IndexError, KeyError, ValueError):
            logger.warning(f"Missing or invalid Pyth price for pair {pair_symbol} in pool {pool_id}. Skipping...")
            continue

        # Determine trade paths
        trade_path_token0_to_token1 = f"{token0_symbol} -> {token1_symbol}"
        trade_path_token1_to_token0 = f"{token1_symbol} -> {token0_symbol}"

        # Trade cost calculations
        gas_cost = Decimal(pool.get("estimatedGasCost", 0))
        fee_tier = Decimal(pool.get("feeTier", 0)) / Decimal("1e6")
        slippage = Decimal("0.005")  # Adjust slippage as needed

        # Calculate trade costs
        trade_cost_token0 = calculate_trade_cost(token0_pyth_price, gas_cost, fee_tier, slippage, fee_tier)
        trade_cost_token1 = calculate_trade_cost(token1_pyth_price, gas_cost, fee_tier, slippage, fee_tier)

        # Integrate Aave data for additional metrics
        def get_aave_metrics(token_symbol):
            if aave_data.empty:
                return Decimal(0), Decimal(0), Decimal(0)
            aave_token = aave_data[aave_data["symbol"] == token_symbol]
            if not aave_token.empty:
                try:
                    liquidity_rate = Decimal(aave_token.iloc[0].get("liquidityRate", 0))
                    variable_borrow_rate = Decimal(aave_token.iloc[0].get("variableBorrowRate", 0))
                    available_liquidity = Decimal(aave_token.iloc[0].get("availableLiquidity", 0))
                    return liquidity_rate, variable_borrow_rate, available_liquidity
                except (ValueError, KeyError):
                    logger.warning(f"Invalid Aave metrics for symbol {token_symbol}. Defaulting to 0.")
            return Decimal(0), Decimal(0), Decimal(0)

        liquidity_rate_token0, variable_borrow_rate_token0, available_liquidity_token0 = get_aave_metrics(token0_symbol)
        liquidity_rate_token1, variable_borrow_rate_token1, available_liquidity_token1 = get_aave_metrics(token1_symbol)

        # Append opportunity details for both trade directions
        opportunities.append({
            "id": pool_id,
            "pair_symbol": pair_symbol,
            "token0_symbol": token0_symbol,
            "token1_symbol": token1_symbol,
            "token0_id": pool.get("token0_id", ""),
            "token1_id": pool.get("token1_id", ""),
            "token0_pyth_price": token0_pyth_price,
            "token1_pyth_price": token1_pyth_price,
            "trade_path": trade_path_token0_to_token1,
            "gas_cost": gas_cost,
            "uniswap_fee": fee_tier,
            "trade_cost": trade_cost_token0["Total Cost"],
            "liquidity_rate_token0": liquidity_rate_token0,
            "variable_borrow_rate_token0": variable_borrow_rate_token0,
            "profitable_token0": token0_pyth_price > trade_cost_token0["Total Cost"]
        })
        opportunities.append({
            "id": pool_id,
            "pair_symbol": pair_symbol,
            "token0_symbol": token0_symbol,
            "token1_symbol": token1_symbol,
            "token0_id": pool.get("token0_id", ""),
            "token1_id": pool.get("token1_id", ""),
            "token0_pyth_price": token0_pyth_price,
            "token1_pyth_price": token1_pyth_price,
            "trade_path": trade_path_token1_to_token0,
            "gas_cost": gas_cost,
            "uniswap_fee": fee_tier,
            "trade_cost": trade_cost_token1["Total Cost"],
            "liquidity_rate_token1": liquidity_rate_token1,
            "variable_borrow_rate_token1": variable_borrow_rate_token1,
            "profitable_token1": token1_pyth_price > trade_cost_token1["Total Cost"]
        })

    logger.info(f"Opportunities found: {len(opportunities)}")
    if not opportunities:
        logger.warning("No arbitrage opportunities were identified.")
    return pd.DataFrame(opportunities)

def save_results_to_csv(results_df, filename):
    """
    Save the results DataFrame to a CSV file.

    Args:
        results_df (DataFrame): DataFrame containing the results.
        filename (str): Filename for the CSV file.
    """
    file_path = os.path.join(DATA_DIRECTORY, filename)
    results_df.to_csv(file_path, index=False)
    logger.info(f"Results saved to {file_path}")

def calculate_opportunities_with_deviation(pyth_data, uniswap_pools, aave_data, deviation_threshold=1.0):
    """
    Extended version of `calculate_opportunities` to include price deviation analysis 
    and potential X->Y->Z trade paths.
    
    Args:
        pyth_data (DataFrame): Pyth price data.
        uniswap_pools (DataFrame): Uniswap pool data.
        aave_data (DataFrame): Aave token data.
        deviation_threshold (float): Minimum percentage deviation to consider for opportunities.
    
    Returns:
        DataFrame: Opportunities with calculated deviations and potential profitability.
    """
    opportunities = []

    # Normalize Pyth symbols
    pyth_data["symbol"] = pyth_data["symbol"].str.upper().str.strip()

    # Ensure required columns are present in Uniswap pools
    required_columns = {"token0_symbol", "token1_symbol", "price_token1_per_token0"}
    if not required_columns.issubset(uniswap_pools.columns):
        logger.error(f"Missing expected columns in Uniswap pools: {required_columns - set(uniswap_pools.columns)}")
        return pd.DataFrame()

    # Normalize Aave symbols
    if not aave_data.empty:
        aave_data["symbol"] = aave_data["symbol"].str.upper().str.strip()

    logger.debug("Analyzing price deviations for opportunities...")
    for _, pool in uniswap_pools.iterrows():
        pool_id = pool["id"]
        token0_symbol = pool["token0_symbol"]
        token1_symbol = pool["token1_symbol"]

        if not token0_symbol or not token1_symbol:
            logger.warning(f"Missing token symbols for pool {pool_id}. Skipping...")
            continue

        try:
            # Retrieve Pyth prices
            token0_pyth_price = Decimal(pyth_data[pyth_data["symbol"] == token0_symbol]["Price"].values[0])
            token1_pyth_price = Decimal(pyth_data[pyth_data["symbol"] == token1_symbol]["Price"].values[0])

            # Calculate expected price and compare with Uniswap price
            expected_price = token0_pyth_price / token1_pyth_price
            uniswap_price = Decimal(pool.get("price_token1_per_token0", 0))
            deviation = abs(expected_price - uniswap_price) / expected_price * 100

            #if deviation >= deviation_threshold:
            # Log and prepare opportunity
            logger.info(f"Significant deviation detected in {token0_symbol}/{token1_symbol}: {deviation}%")
            opportunities.append({
                "Pool ID": pool_id,
                "Pair": f"{token0_symbol}/{token1_symbol}",
                "Token0 Pyth Price": float(token0_pyth_price),
                "Token1 Pyth Price": float(token1_pyth_price),
                "Expected Price": float(expected_price),
                "Uniswap Price": float(uniswap_price),
                "Price Deviation (%)": float(deviation),
            })

            # Simulate X->Y->Z trade paths
            # This section will calculate potential profitability for multi-token trades
            # based on slippage and fees (add logic for trade path simulation as needed)

        except Exception as e:
            logger.error(f"Error processing pool {pool_id}: {e}")
            continue

    logger.info(f"Total opportunities found with deviation threshold {deviation_threshold}%: {len(opportunities)}")
    return pd.DataFrame(opportunities)

# Main Functionality
def analyze_opportunities():
    """
    Analyze arbitrage opportunities using Pyth, Uniswap, and Aave data.
    """
    logger.info("Fetching data...")
    pyth_data, uniswap_pools, aave_data = fetch_and_prepare_data()

    if pyth_data.empty or uniswap_pools.empty:
        logger.warning("Insufficient data for analysis.")
        return

    logger.info("Calculating opportunities...")
    opportunities_df = calculate_opportunities(pyth_data, uniswap_pools, aave_data)

    if not opportunities_df.empty:
        logger.info("Opportunities found!")
        save_results_to_csv(opportunities_df, "arbitrage_opportunities.csv")
    else:
        logger.info("No profitable opportunities identified.")


if __name__ == "__main__":
    analyze_opportunities()