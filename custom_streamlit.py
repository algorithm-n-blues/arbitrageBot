import aiohttp
import asyncio
from datetime import datetime
import logging
import os
import pandas as pd
import custom_streamlit as st
from uniswap_v3.fetch_uniswap import (
    fetch_top_uniswap_pools,
    fetch_pool_details,
)
from aave.aave_data import fetch_aave_data, save_aave_data_to_csv
from pyth.pyth_data import get_pyth_data, save_pyth_data_to_csv
from utils import (
    add_timestamp,
    standardize_symbol,
    calculate_price_difference,
    estimate_arbitrage_profit,
    calculate_weighted_tvl,
    filter_pyth_prices,
    get_last_updated_time,
    is_file_outdated,
    save_uniswap_data_to_csv
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

# Async helper to fetch Aave data
async def fetch_aave_data_helper():
    async with aiohttp.ClientSession() as session:
        return await fetch_aave_data(session)

def calculate_flash_loan_profitability(arbitrage_df, gas_cost_usd=10, loan_cap=1_000_000):
    """
    Add flash loan profitability columns:
    - Recommended Loan Size
    - Adjusted Net Profit
    - ROI (%)

    Args:
        arbitrage_df (DataFrame): DataFrame with arbitrage results, including 'Net Profit' and 'Weighted Pyth TVL'.
        gas_cost_usd (float): Gas cost in USD for executing the transaction.
        loan_cap (float): Maximum loan size allowed in USD.

    Returns:
        DataFrame: Filtered DataFrame with flash loan profitability analysis.
    """
    required_columns = ["Pool", "Weighted Pyth TVL", "Net Profit"]
    missing_columns = [col for col in required_columns if col not in arbitrage_df.columns]
    if missing_columns:
        logger.error(f"Missing required columns for flash loan analysis: {missing_columns}")
        return pd.DataFrame()

    try:
        arbitrage_df["Weighted Pyth TVL"] = pd.to_numeric(arbitrage_df["Weighted Pyth TVL"], errors="coerce")
        arbitrage_df["Net Profit"] = pd.to_numeric(arbitrage_df["Net Profit"], errors="coerce")

        arbitrage_df["Recommended Loan Size"] = arbitrage_df["Weighted Pyth TVL"].apply(
            lambda tvl: min(tvl, loan_cap) if pd.notnull(tvl) else 0
        )
        arbitrage_df["Adjusted Net Profit"] = arbitrage_df.apply(
            lambda row: max(
                (row["Net Profit"] * row["Recommended Loan Size"] / row["Weighted Pyth TVL"]) - gas_cost_usd, 0
            ) if row["Weighted Pyth TVL"] > 0 else 0,
            axis=1
        )
        arbitrage_df["ROI (%)"] = arbitrage_df.apply(
            lambda row: (row["Adjusted Net Profit"] / row["Recommended Loan Size"] * 100)
            if row["Recommended Loan Size"] > 0 else 0,
            axis=1
        )

        flash_loan_df = arbitrage_df.reset_index(drop=True)
        logger.info("Flash Loan Profitability Analysis completed successfully.")
        return flash_loan_df

    except Exception as e:
        logger.error(f"Error calculating flash loan profitability: {e}")
        return pd.DataFrame()

def analyze_flash_loan_arbitrage(comparison_data, aave_data, gas_cost_usd=10, loan_cap=1_000_000):
    """
    Analyze flash loan arbitrage opportunities with integrated Aave borrowing rates.

    Args:
        comparison_data (DataFrame): DataFrame with price comparison results, including 'Net Profit' and 'Weighted Pyth TVL'.
        aave_data (DataFrame): DataFrame with Aave reserve data for interest rates and available liquidity.
        gas_cost_usd (float): Gas cost in USD for executing the transaction.
        loan_cap (float): Maximum loan size allowed in USD.

    Returns:
        DataFrame: Filtered DataFrame with enhanced flash loan profitability analysis.
    """
    # Check for required columns
    required_columns = ["Pool", "Weighted Pyth TVL", "Net Profit"]
    aave_columns = ["asset_symbol", "available_liquidity", "variable_borrow_rate"]

    missing_arbitrage_columns = [col for col in required_columns if col not in comparison_data.columns]
    missing_aave_columns = [col for col in aave_columns if col not in aave_data.columns]

    if missing_arbitrage_columns:
        logger.error(f"Missing required arbitrage columns: {missing_arbitrage_columns}")
        return pd.DataFrame()
    if missing_aave_columns:
        logger.error(f"Missing required Aave columns: {missing_aave_columns}")
        return pd.DataFrame()

    try:
        # Ensure numeric conversion
        comparison_data["Weighted Pyth TVL"] = pd.to_numeric(comparison_data["Weighted Pyth TVL"], errors="coerce")
        comparison_data["Net Profit"] = pd.to_numeric(comparison_data["Net Profit"], errors="coerce")

        # Convert 'Pool' and 'asset_symbol' to strings for consistent merging
        comparison_data["Pool"] = comparison_data["Pool"].astype(str)
        aave_data["asset_symbol"] = aave_data["asset_symbol"].astype(str)

        # Merge Aave liquidity and rates for matching tokens
        merged_df = comparison_data.merge(
            aave_data[["asset_symbol", "available_liquidity", "variable_borrow_rate"]],
            how="left",
            left_on="Pool",
            right_on="asset_symbol"
        )

        # Filter pools with sufficient Aave liquidity
        merged_df["Available Liquidity (USD)"] = pd.to_numeric(merged_df["available_liquidity"], errors="coerce").fillna(0)
        merged_df = merged_df[merged_df["Available Liquidity (USD)"] > 0]

        # Recommended Loan Size: min of Weighted TVL, Aave liquidity, and loan cap
        merged_df["Recommended Loan Size"] = merged_df.apply(
            lambda row: min(row["Weighted Pyth TVL"], row["Available Liquidity (USD)"], loan_cap), axis=1
        )

        # Adjusted Net Profit: scale based on loan size and account for gas costs
        merged_df["Adjusted Net Profit"] = merged_df.apply(
            lambda row: max(
                (row["Net Profit"] * row["Recommended Loan Size"] / row["Weighted Pyth TVL"]) - gas_cost_usd, 0
            ) if row["Weighted Pyth TVL"] > 0 else 0,
            axis=1
        )

        # Borrowing Cost: interest on recommended loan size
        merged_df["Borrowing Cost (USD)"] = merged_df.apply(
            lambda row: row["Recommended Loan Size"] * row["variable_borrow_rate"] / 365, axis=1
        )

        # Final Profit: Adjusted Net Profit minus borrowing costs
        merged_df["Final Profit"] = merged_df["Adjusted Net Profit"] - merged_df["Borrowing Cost (USD)"]

        # ROI (%): Final profit relative to loan size
        merged_df["ROI (%)"] = merged_df.apply(
            lambda row: (row["Final Profit"] / row["Recommended Loan Size"] * 100)
            if row["Recommended Loan Size"] > 0 else 0,
            axis=1
        )
 
        # Display all opportunities
        all_opportunities_df = merged_df.copy()
        all_opportunities_df = all_opportunities_df.reset_index(drop=True)

        logger.info("Flash Loan Arbitrage Analysis completed successfully.")
        return all_opportunities_df

    except Exception as e:
        logger.error(f"Error during flash loan arbitrage analysis: {e}")
        return pd.DataFrame()

def streamlit_app():
    # Define gas cost in USD
    gas_cost_usd = 10  # Example default value for gas cost in USD
    st.title("Arbitrage & Token Analytics Dashboard")
    st.write("Real-time data fetched from Uniswap, Pyth Network, and Aave.")

    # --- Fetch and Display Pyth Network Data ---
    st.subheader("Pyth Network Prices")
    pyth_file = os.path.join(DATA_DIRECTORY, "pyth_all_prices.csv")

    # Check last updated timestamp
    last_updated, is_outdated = None, True
    if os.path.exists(pyth_file):
        last_updated, is_outdated = get_last_updated_time(pyth_file), is_file_outdated(pyth_file)

    st.write(f"Last updated at: {last_updated}" if last_updated else "No data available.")

    # Refresh data if outdated or on button click
    if is_outdated or st.button("Fetch Pyth Data"):
        with st.spinner("Fetching Pyth Network data..."):
            try:
                pyth_data = get_pyth_data()
                if pyth_data:
                    save_pyth_data_to_csv(pyth_data, pyth_file)
                    st.success("Pyth data fetched and saved successfully.")
                else:
                    st.error("Failed to fetch Pyth Network data.")
            except Exception as e:
                st.error(f"Error fetching Pyth data: {e}")
                logger.error(f"Error fetching Pyth data: {e}")
        last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.write(f"Last updated at: {last_updated}")

    # Display Pyth data if exists
    if os.path.exists(pyth_file):
        pyth_df = pd.read_csv(pyth_file)
        st.dataframe(pyth_df)
    else:
        st.warning("No Pyth Network data available. Fetch it first.")

    # --- Fetch and Display Uniswap Data ---
    st.subheader("Uniswap Pool Explorer")
    sort_options = ["totalValueLockedUSD", "volumeUSD", "feeTier"]
    selected_sort = st.selectbox("Sort Pools By", sort_options, index=0)

    uniswap_file = os.path.join(DATA_DIRECTORY, "uniswap_top_pools.csv")

    # Check last updated timestamp
    last_updated, is_outdated = None, True
    if os.path.exists(uniswap_file):
        last_updated, is_outdated = get_last_updated_time(uniswap_file), is_file_outdated(uniswap_file)

    st.write(f"Last updated at: {last_updated}" if last_updated else "No data available.")

    if is_outdated or st.button("Fetch Uniswap Pools"):
        with st.spinner("Fetching Uniswap data..."):
            try:
                uniswap_data = fetch_top_uniswap_pools(UNISWAP_ARBITRUM_URL, order_by=selected_sort)
                logger.debug(f"Uniswap Data with Derived Prices: {uniswap_data}")
                if uniswap_data:
                    save_uniswap_data_to_csv(uniswap_data, uniswap_file)
                    st.success("Uniswap data fetched and saved successfully.")
                else:
                    st.error("Failed to fetch Uniswap data.")
            except Exception as e:
                st.error(f"Error fetching Uniswap data: {e}")
                logger.error(f"Error fetching Uniswap data: {e}")
        last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.write(f"Last updated at: {last_updated}")

    # Display Uniswap data if exists
    if os.path.exists(uniswap_file):
        uniswap_df = pd.read_csv(uniswap_file)
        logger.debug(f"Uniswap Data: {uniswap_df}")
        # Ensure derived price columns are present
        if "price_token1_per_token0" in uniswap_df.columns and "price_token0_per_token1" in uniswap_df.columns:
            st.dataframe(uniswap_df)
        else:
            st.warning("Uniswap data is missing derived prices. Please refresh the data.")
    else:
        st.warning("No Uniswap data available. Fetch it first.")

    # --- Fetch and Display Aave Data ---
    st.subheader("Aave Top Reserves")
    aave_file = os.path.join(DATA_DIRECTORY, "arbitrum_aave_data.csv")

    # Check last updated timestamp
    last_updated, is_outdated = None, True
    if os.path.exists(aave_file):
        last_updated, is_outdated = get_last_updated_time(aave_file), is_file_outdated(aave_file)

    st.write(f"Last updated at: {last_updated}" if last_updated else "No data available.")

    if is_outdated or st.button("Fetch Aave Reserves"):
        with st.spinner("Fetching Aave reserves..."):
            try:
                aave_data = asyncio.run(fetch_aave_data_helper())
                if aave_data:
                    save_aave_data_to_csv(aave_data, aave_file)
                    st.success("Aave reserves fetched and saved successfully.")
                else:
                    st.error("Failed to fetch Aave data.")
            except Exception as e:
                st.error(f"Error fetching Aave data: {e}")
                logger.error(f"Error fetching Aave data: {e}")
        last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.write(f"Last updated at: {last_updated}")

    # Display Aave data if exists
    if os.path.exists(aave_file):
        aave_df = pd.read_csv(aave_file)
        st.dataframe(aave_df)
    else:
        st.warning("No Aave reserves data available. Fetch it first.")

   # --- Price Comparison ---
    st.subheader("Price Comparison and Arbitrage Opportunities")
    if os.path.exists(pyth_file) and os.path.exists(uniswap_file):
        pyth_df = pd.read_csv(pyth_file)
        uniswap_df = pd.read_csv(uniswap_file)

        # Log missing tokens in Pyth data
        missing_tokens = set(uniswap_df['token0_symbol']).union(uniswap_df['token1_symbol']) - set(pyth_df['symbol'])
        if missing_tokens:
            logger.warning(f"Missing tokens in Pyth data: {missing_tokens}")

        comparison_df = calculate_price_difference(uniswap_df.to_dict("records"), pyth_df.to_dict("records"))
        logger.info(comparison_df)
        if not comparison_df.empty:
            st.dataframe(comparison_df)
            st.success("Price comparison completed successfully.")

            # --- Flash Loan Arbitrage Analysis ---
            st.subheader("Flash Loan Arbitrage Analysis")
            if os.path.exists(aave_file):
                aave_df = pd.read_csv(aave_file)

                # Ensure required columns exist
                required_aave_columns = ['asset_symbol', 'available_liquidity', 'variable_borrow_rate']
                for col in required_aave_columns:
                    if col not in aave_df.columns:
                        aave_df[col] = 0  # Assign default values

                # Ensure 'Net Profit' exists in comparison_df
                if 'Net Profit' not in comparison_df.columns:
                    comparison_df['Net Profit'] = comparison_df.apply(
                        lambda row: row.get('Token0 Profit', 0) + row.get('Token1 Profit', 0) - gas_cost_usd, axis=1
                    )

                try:
                    # Analyze flash loan arbitrage opportunities
                    arbitrage_results = analyze_flash_loan_arbitrage(comparison_data=comparison_df, aave_data=aave_df)
                    if not arbitrage_results.empty:
                        st.dataframe(arbitrage_results)
                        st.success("Flash loan arbitrage analysis completed successfully.")
                    else:
                        st.warning("No profitable flash loan arbitrage opportunities found.")
                except Exception as e:
                    st.error(f"Error in Flash Loan Arbitrage Analysis: {e}")
                    logger.error(f"Error in Flash Loan Arbitrage Analysis: {e}")

            else:
                st.warning("Missing Aave data. Fetch it first to run arbitrage analysis.")
        else:
            st.warning("No significant price differences found.")
    else:
        st.warning("Missing Pyth or Uniswap data. Fetch both to compare prices.")

if __name__ == "__main__":
    streamlit_app()
