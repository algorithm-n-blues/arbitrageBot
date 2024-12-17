import os
import logging
import pandas as pd
import streamlit as st
from uniswap_v3.fetch_uniswap import (
    save_uniswap_data_to_csv,
    fetch_top_uniswap_pools,
    fetch_pool_details,
)
from aave.aave_data import fetch_aave_data, save_aave_data_to_csv
from pyth.pyth_data import save_pyth_data_to_csv
from utils import (
    add_timestamp,
    standardize_symbol,
    calculate_price_difference,
    estimate_arbitrage_profit,
    calculate_weighted_tvl
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

def calculate_flash_loan_profitability(arbitrage_df):
    """
    Add columns for flash loan profitability analysis:
    - Recommended Flash Loan Size
    - Adjusted Net Profit for Flash Loan
    - ROI (%)
    """
    results = []

    logging.info(f"Columns in arbitrage_df before calculate_weighted_tvl: {arbitrage_df.columns}")
    arbitrage_df = calculate_weighted_tvl(arbitrage_df)

    for _, row in arbitrage_df.iterrows():
        try:
            pool_tvl = row["Weighted Pyth TVL"]  # Available liquidity
            recommended_loan_size = min(pool_tvl, 1_000_000)  # Flash loan capped at $1M
            
            # Adjust Net Profit for loan size
            adjusted_net_profit = row["Net Profit"] * (recommended_loan_size / pool_tvl)
            
            # ROI Calculation
            roi = (adjusted_net_profit / recommended_loan_size) * 100
            
            results.append({
                "Pool": row["Pool"],
                "Net Profit": row["Net Profit"],
                "Recommended Loan Size": recommended_loan_size,
                "Adjusted Net Profit": adjusted_net_profit,
                "ROI (%)": roi,
            })
        except Exception as e:
            logger.error(f"Error in flash loan profitability calculations: {e}")
            continue

    return pd.DataFrame(results)

def streamlit_app():
    st.title("Arbitrage & Token Analytics Dashboard")
    st.write("Real-time data fetched from Uniswap, Pyth Network, and Aave.")

    # --- Uniswap Data ---
    st.subheader("Uniswap Pool Explorer")
    sort_options = ["totalValueLockedUSD", "volumeUSD", "feeTier"]
    selected_sort = st.selectbox("Sort Pools By", sort_options, index=0)

    uniswap_data = fetch_top_uniswap_pools(
        UNISWAP_ARBITRUM_URL,
        order_by=selected_sort,
    )

    if uniswap_data:
        st.subheader("Uniswap Top Pools")
        uniswap_df = pd.DataFrame(uniswap_data)

        # Ensure numeric conversion for relevant columns
        numeric_columns = ["totalValueLockedUSD", "volumeUSD", "feeTier"]
        for col in numeric_columns:
            if col in uniswap_df.columns:
                uniswap_df[col] = pd.to_numeric(uniswap_df[col], errors="coerce")

        uniswap_df["token_pair"] = uniswap_df["token0_symbol"] + " / " + uniswap_df["token1_symbol"]
        st.dataframe(uniswap_df)
        logger.info(f"Uniswap Top Pools Data:\n{uniswap_df}")

        # Save Uniswap data
        save_uniswap_data_to_csv(uniswap_data, os.path.join(DATA_DIRECTORY, "uniswap_top_pools.csv"))
        logger.info("Uniswap pools data displayed and saved.")

        # --- Pool Details ---
        st.subheader("Uniswap Pool Details")
        pool_id = st.selectbox(
            "Select Pool ID for Details", [pool["id"] for pool in uniswap_data]
        )
        if pool_id:
            pool_details = fetch_pool_details(pool_id)
            if pool_details:
                st.json(pool_details)
                logger.info(f"Fetched Pool Details for ID {pool_id}:\n{pool_details}")
            else:
                st.error("Could not fetch details for the selected pool.")
                logger.error(f"Failed to fetch details for pool ID: {pool_id}")
    else:
        st.error("Uniswap data not available. Please fetch data first.")
        logger.error("Failed to fetch Uniswap data.")

    # --- Pyth Network Data ---
    st.subheader("Pyth Network Prices")
    pyth_file = os.path.join(DATA_DIRECTORY, "pyth_all_prices.csv")
    if os.path.exists(pyth_file):
        pyth_df = pd.read_csv(pyth_file)
        st.dataframe(pyth_df)
        logger.info(f"Pyth Network Data:\n{pyth_df}")
    else:
        st.error("Pyth Network data not available. Please fetch data first.")
        logger.warning("Pyth Network data file missing.")

    # --- Aave Data ---
    st.subheader("Aave Top Reserves")
    aave_file = os.path.join(DATA_DIRECTORY, "arbitrum_aave_data.csv")
    if os.path.exists(aave_file):
        aave_df = pd.read_csv(aave_file)
        st.dataframe(aave_df)
        logger.info(f"Aave Data:\n{aave_df}")
    else:
        st.error("Aave data not available. Please fetch data first.")
        logger.warning("Aave data file missing.")

    # --- Price Comparisons ---
    if os.path.exists(pyth_file) and uniswap_data:
        st.subheader("Price Comparison and Arbitrage Opportunities")
        pyth_data = pd.read_csv(pyth_file).to_dict("records")
        comparison_df = calculate_price_difference(uniswap_data, pyth_data)

        if not comparison_df.empty:
            st.subheader("Price Comparison")
            st.dataframe(comparison_df)
            logger.info(f"Price Comparison Data:\n{comparison_df}")

            # --- Arbitrage Analysis Filters ---
            st.subheader("Set Filters for Arbitrage Opportunities")
            gas_price_gwei = st.slider("Gas Price (Gwei)", min_value=1, max_value=100, value=10, step=1)
            gas_limit = st.number_input("Gas Limit", min_value=100000, max_value=1000000, value=300000, step=1000)
            min_profit = st.number_input("Minimum Net Profit (USD)", min_value=0.0, value=1000.0, step=100.0)
            max_slippage = st.slider("Maximum Slippage (%)", min_value=0.0, max_value=10.0, value=0.5, step=0.1)

            arbitrage_df = estimate_arbitrage_profit(comparison_df, gas_price_gwei, gas_limit)

            # --- Enhanced Analysis: Profitability ---
            st.subheader("Flash Loan Profitability Analysis")
            flash_loan_df = calculate_flash_loan_profitability(arbitrage_df)
            st.dataframe(flash_loan_df)

            if not flash_loan_df.empty:
                profitable_opportunities = flash_loan_df[flash_loan_df["Net Profit"] > 0]
                if not profitable_opportunities.empty:
                    st.write("Profitable Opportunities (Net Profit > $0):")
                    st.dataframe(profitable_opportunities)
                else:
                    st.warning("No profitable opportunities found after flash loan adjustments.")
            else:
                st.error("No arbitrage data available for analysis.")

        else:
            st.warning("Price comparison data is empty.")
            logger.warning("No price comparison data available.")
    else:
        st.warning("Cannot perform price comparisons: Uniswap or Pyth data missing.")
        logger.warning("Comparison failed due to missing Uniswap or Pyth data.")

if __name__ == "__main__":
    streamlit_app()
