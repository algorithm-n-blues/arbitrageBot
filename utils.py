import pandas as pd
from datetime import datetime
import logging

# Initialize logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler("debug.log"),  # Save logs to a file
    ],
)
logger = logging.getLogger(__name__)


# Add a timestamp column to DataFrame
def add_timestamp(df):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["Timestamp"] = timestamp
    return df

# Standardize symbol for consistency
def standardize_symbol(symbol):
    return symbol.strip().upper()

# Compare prices between Uniswap and Pyth
def compare_prices(uniswap_data, pyth_data):
    comparison_data = []
    pyth_dict = {item["symbol"]: item["Price"] for item in pyth_data}

    for pool in uniswap_data:
        try:
            token0_symbol = standardize_symbol(pool["token0"]["symbol"])
            token1_symbol = standardize_symbol(pool["token1"]["symbol"])
            uniswap_tvl = float(pool.get("totalValueLockedUSD", 0))

            pyth_price_token0 = pyth_dict.get(token0_symbol, None)
            pyth_price_token1 = pyth_dict.get(token1_symbol, None)

            if pyth_price_token0 and pyth_price_token1:
                # Calculate weighted TVL and deviation
                weighted_tvl = pyth_price_token0 + pyth_price_token1
                deviation = abs(weighted_tvl - uniswap_tvl) / uniswap_tvl * 100 if uniswap_tvl > 0 else 0

                comparison_data.append({
                    "Pool": f"{token0_symbol}/{token1_symbol}",
                    "Pyth Price Token0": pyth_price_token0,
                    "Pyth Price Token1": pyth_price_token1,
                    "Uniswap TVL": uniswap_tvl,
                    "Weighted Pyth TVL": weighted_tvl,
                    "Deviation (%)": deviation
                })
        except Exception as e:
            print(f"Error processing pool: {e}")
            continue

    return pd.DataFrame(comparison_data)

def calculate_price_difference(uniswap_data, pyth_data):
    """
    Compare Uniswap and Pyth prices and calculate differences.

    Args:
        uniswap_data (list): Uniswap pool data.
        pyth_data (list): Pyth Network price data.

    Returns:
        pd.DataFrame: DataFrame with price comparisons.
    """
    logger.info("Starting price comparison between Uniswap and Pyth data.")

    # Convert Pyth data to DataFrame and normalize symbols
    pyth_df = pd.DataFrame(pyth_data)
    pyth_df["symbol"] = pyth_df["symbol"].str.upper().str.strip()

    comparison_data = []

    for pool in uniswap_data:
        try:
            token0_symbol = pool["token0_symbol"].upper().strip()
            token1_symbol = pool["token1_symbol"].upper().strip()

            # Get Pyth prices for token0 and token1
            pyth_price_token0 = pyth_df.loc[pyth_df["symbol"] == token0_symbol, "Price"].values
            pyth_price_token1 = pyth_df.loc[pyth_df["symbol"] == token1_symbol, "Price"].values

            pyth_price_token0 = pyth_price_token0[0] if len(pyth_price_token0) > 0 else None
            pyth_price_token1 = pyth_price_token1[0] if len(pyth_price_token1) > 0 else None

            if pyth_price_token0 is None or pyth_price_token1 is None:
                logger.warning(f"Missing Pyth price for token0: {token0_symbol} or token1: {token1_symbol}. Skipping pool.")
                continue

            # Calculate deviations
            token0_diff = abs(pyth_price_token0 - float(pool["token0Price"]))
            token1_diff = abs(pyth_price_token1 - float(pool["token1Price"]))

            uniswap_tvl = float(pool.get("totalValueLockedUSD", 0))
            token0_tvl = float(pool.get("totalValueLockedToken0", 0)) * pyth_price_token0
            token1_tvl = float(pool.get("totalValueLockedToken1", 0)) * pyth_price_token1
            weighted_tvl = token0_tvl + token1_tvl
            deviation = abs(weighted_tvl - uniswap_tvl) / uniswap_tvl * 100 if uniswap_tvl > 0 else 0

            # Add calculated profits (preliminary)
            token0_profit = token0_diff * float(pool.get("totalValueLockedToken0", 0))
            token1_profit = token1_diff * float(pool.get("totalValueLockedToken1", 0))

            comparison_data.append({
                "Pool": f"{token0_symbol}/{token1_symbol}",
                "Pyth Price Token0": pyth_price_token0,
                "Pyth Price Token1": pyth_price_token1,
                "Uniswap Token0 Price": float(pool["token0Price"]),
                "Uniswap Token1 Price": float(pool["token1Price"]),
                "Price Diff Token0": token0_diff,
                "Price Diff Token1": token1_diff,
                "Uniswap TVL": uniswap_tvl,
                "Weighted Pyth TVL": weighted_tvl,
                "Deviation (%)": deviation,
                "Token0 Profit": token0_profit,
                "Token1 Profit": token1_profit,
            })
        except Exception as e:
            logger.error(f"Error processing pool {pool.get('id', 'unknown')}: {e}", exc_info=True)
            continue

    logger.info(f"Price comparison completed for {len(comparison_data)} pools.")
    return pd.DataFrame(comparison_data)

def estimate_arbitrage_profit(comparison_df, gas_price_gwei=10, gas_limit=300000):
    """
    Estimate arbitrage profits considering fees, slippage, and gas costs.

    Args:
        comparison_df (DataFrame): Price comparison data.
        gas_price_gwei (float): Gas price in Gwei for Arbitrum transactions.
        gas_limit (int): Gas limit per transaction.

    Returns:
        DataFrame: DataFrame with estimated profits.
    """
    # Calculate the average ETH price for gas cost calculations
    eth_price = comparison_df.loc[comparison_df["Pool"].str.contains("WETH"), "Pyth Price Token1"].mean()
    if pd.isna(eth_price):
        logger.error("ETH price not found in comparison data. Cannot calculate gas cost.")
        return pd.DataFrame()  # Return empty DataFrame if ETH price is unavailable

    # Calculate gas cost in USD
    gas_cost_usd = (gas_price_gwei * 1e-9) * gas_limit * eth_price  # Convert Gwei to ETH, then to USD
    logger.info(f"Estimated Gas Cost (USD): {gas_cost_usd}")

    results = []
    for _, row in comparison_df.iterrows():
        try:
            # Fetch recent trade volume and calculate trade size
            trade_volume = row.get("recentVolumeUSD", 0)
            trade_size_token0 = min(trade_volume, row.get("totalValueLockedToken0", 1e6)) * 0.01  # 1% of liquidity
            trade_size_token1 = min(trade_volume, row.get("totalValueLockedToken1", 1e6)) * 0.01

            # Slippage based on trade size and liquidity
            token0_liquidity = row.get("totalValueLockedToken0", 1e6)
            token1_liquidity = row.get("totalValueLockedToken1", 1e6)
            token0_slippage = (trade_size_token0 / token0_liquidity) * 100 if token0_liquidity > 0 else 0
            token1_slippage = (trade_size_token1 / token1_liquidity) * 100 if token1_liquidity > 0 else 0

            # Default fee tier for Uniswap pools
            fee_tier = row.get("feeTier", 3000)
            swap_fee = fee_tier / 1e6

            # Calculate fee costs
            token0_fee = trade_size_token0 * swap_fee
            token1_fee = trade_size_token1 * swap_fee

            # Net profit after fees, slippage, and gas costs
            token0_profit = max(0, row.get("Token0 Profit", 0) - token0_fee - token0_slippage - gas_cost_usd)
            token1_profit = max(0, row.get("Token1 Profit", 0) - token1_fee - token1_slippage - gas_cost_usd)

            # Append the results
            results.append({
                "Pool": row["Pool"],
                "Token0 Profit": token0_profit,
                "Token1 Profit": token1_profit,
                "Net Profit": token0_profit + token1_profit,
                "Gas Cost (USD)": gas_cost_usd,
                "Slippage Token0 (%)": token0_slippage,
                "Slippage Token1 (%)": token1_slippage,
                "Fee Token0 (USD)": token0_fee,
                "Fee Token1 (USD)": token1_fee,
                "recentVolumeUSD": trade_volume,  # Include recent volume
            })

        except Exception as e:
            logger.error(f"Error calculating arbitrage for pool {row['Pool']}: {e}", exc_info=True)
            continue

    profit_df = pd.DataFrame(results)
    logger.info(f"Estimated Arbitrage Profits with Fees and Slippage:\n{profit_df}")
    return profit_df

def filter_arbitrage_opportunities(arbitrage_df, min_profit=1000.0, max_slippage=0.5):
    """
    Filter arbitrage opportunities based on profit and slippage thresholds.

    Args:
        arbitrage_df (DataFrame): Arbitrage opportunities DataFrame.
        min_profit (float): Minimum net profit to include.
        max_slippage (float): Maximum acceptable slippage (%) for tokens.

    Returns:
        DataFrame: Filtered arbitrage opportunities.
    """
    filtered_df = arbitrage_df[
        (arbitrage_df["Net Profit"] >= min_profit) &
        (arbitrage_df["Slippage Token0 (%)"] <= max_slippage) &
        (arbitrage_df["Slippage Token1 (%)"] <= max_slippage)
    ]
    logger.info(f"Filtered Arbitrage Opportunities:\n{filtered_df}")
    return filtered_df

def calculate_flash_loan_profitability(arbitrage_df):
    """
    Add columns for flash loan profitability analysis:
    - Recommended Flash Loan Size
    - Adjusted Net Profit for Flash Loan
    - ROI (%)
    - Incorporate trade volume as a limit for realistic profitability calculations.

    Args:
        arbitrage_df (DataFrame): Arbitrage data.

    Returns:
        DataFrame: Updated with new profitability columns.
    """
    results = []

    for _, row in arbitrage_df.iterrows():
        try:
            # Retrieve pool TVL (fallback to $1M if missing or invalid)
            pool_tvl = row.get("Weighted Pyth TVL", 1_000_000)
            if pool_tvl <= 0:
                logger.warning(f"Invalid or zero Weighted Pyth TVL for pool {row['Pool']}. Skipping.")
                continue

            # Retrieve recent trade volume (fallback to $1K if missing or invalid)
            recent_volume = row.get("recentVolumeUSD", 1_000)
            if recent_volume <= 0:
                logger.warning(f"Invalid or zero recent volume for pool {row['Pool']}. Skipping.")
                continue

            # Recommended loan size: capped at $1M, available pool TVL, or 10% of recent trade volume
            recommended_loan_size = min(pool_tvl, recent_volume * 0.1, 1_000_000)

            # Adjust Net Profit proportionally to the recommended loan size
            adjusted_net_profit = row["Net Profit"] * (recommended_loan_size / pool_tvl) if pool_tvl > 0 else 0

            # Calculate ROI (%), ensuring no division by zero
            roi = (adjusted_net_profit / recommended_loan_size) * 100 if recommended_loan_size > 0 else 0

            # Append results with all calculated values
            results.append({
                "Pool": row["Pool"],
                "Net Profit": row["Net Profit"],
                "Recommended Loan Size": recommended_loan_size,
                "Adjusted Net Profit": adjusted_net_profit,
                "ROI (%)": roi,
                "Gas Cost (USD)": row.get("Gas Cost (USD)", 0),
                "Slippage Token0 (%)": row.get("Slippage Token0 (%)", 0),
                "Slippage Token1 (%)": row.get("Slippage Token1 (%)", 0),
                "Recent Volume (USD)": recent_volume,
                "Weighted Pyth TVL (USD)": pool_tvl,
            })

        except Exception as e:
            logger.error(f"Error in flash loan profitability calculations for pool {row['Pool']}: {e}", exc_info=True)
            continue

    # Convert results to DataFrame for easier use
    profit_df = pd.DataFrame(results)
    logger.info(f"Flash Loan Profitability Analysis completed with {len(profit_df)} entries.")
    return profit_df

def calculate_weighted_tvl(comparison_df):
    """
    Calculate or ensure Weighted Pyth TVL is available in the DataFrame.
    """
    if "Weighted Pyth TVL" not in comparison_df.columns:
        comparison_df["Weighted Pyth TVL"] = (
            comparison_df["Pyth Price Token0"] * comparison_df["Uniswap Token0 Price"]
            + comparison_df["Pyth Price Token1"] * comparison_df["Uniswap Token1 Price"]
        ).fillna(1_000_000)  # Fallback to $1M if missing
    return comparison_df
