import requests
import pandas as pd
import streamlit as st
from math import sqrt
from decimal import Decimal
import time
import logging

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("uniswap_trades.log")
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Uniswap Graph API URL for Arbitrum
API_KEY = "e12c2830e44d2ed329aa22ec5a73fb81"
UNISWAP_ARBITRUM_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

# Selected pools (Replace with actual pool IDs for BTC, ETH, or other tokens)
SELECTED_POOLS = [
    "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",  # Example USDC/WETH pool
    "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed"   # Example WBTC/WETH pool
]

# Token metadata cache
token_metadata = {}

def fetch_token_metadata(pool_id, uniswap_url):
    """
    Fetch token metadata for a pool to include token symbols and decimals.
    
    Args:
        pool_id (str): Pool ID to fetch metadata for.
        uniswap_url (str): Uniswap Graph API endpoint.

    Returns:
        dict: Token metadata including symbols and decimals.
    """
    query = f"""
    {{
      pool(id: "{pool_id}") {{
        token0 {{
          symbol
          decimals
        }}
        token1 {{
          symbol
          decimals
        }}
      }}
    }}
    """
    try:
        response = requests.post(uniswap_url, json={"query": query})
        response.raise_for_status()
        data = response.json()
        if "data" in data and "pool" in data["data"]:
            pool = data["data"]["pool"]
            return {
                "token0_symbol": pool["token0"]["symbol"],
                "token0_decimals": int(pool["token0"]["decimals"]),
                "token1_symbol": pool["token1"]["symbol"],
                "token1_decimals": int(pool["token1"]["decimals"]),
            }
    except Exception as e:
        logger.error(f"Error fetching token metadata for pool {pool_id}: {e}")
    return {}

def fetch_recent_trades(pool_id, uniswap_url):
    """
    Fetch recent trades for a specific Uniswap pool.

    Args:
        pool_id (str): Pool ID to fetch trades for.
        uniswap_url (str): Uniswap Graph API endpoint.

    Returns:
        list: List of recent trades with price details.
    """
    query = f"""
    {{
      swaps(first: 10, orderBy: timestamp, orderDirection: desc, where: {{pool: "{pool_id}"}}) {{
        amount0
        amount1
        amountUSD
        timestamp
        sender
      }}
    }}
    """
    try:
        response = requests.post(uniswap_url, json={"query": query})
        response.raise_for_status()
        data = response.json()
        if "data" in data and "swaps" in data["data"]:
            return data["data"]["swaps"]
        else:
            logger.warning(f"No swaps data found for pool {pool_id}")
            return []
    except Exception as e:
        logger.error(f"Error fetching trades for pool {pool_id}: {e}")
        return []

def calculate_volatility(trade_data):
    """
    Calculate price volatility based on recent trades.

    Args:
        trade_data (DataFrame): DataFrame containing recent trade data.

    Returns:
        float: Volatility metric (standard deviation of trade prices).
    """
    if trade_data.empty:
        return 0.0
    return trade_data["Amount USD"].std()

def analyze_top_trades(trade_data, token0_symbol, token1_symbol):
    """
    Analyze the top 50 trades to provide insights for arbitrage opportunities.

    Args:
        trade_data (DataFrame): DataFrame containing recent trade data.
    """
    if trade_data.empty:
        return pd.DataFrame()

    # Timeframe of analysis
    start_time = trade_data["Timestamp"].min()
    end_time = trade_data["Timestamp"].max()

    # High/Low Prices
    highest_trade = trade_data["Amount USD"].max()
    lowest_trade = trade_data["Amount USD"].min()

    # Aggregated Volume
    total_volume_token0 = trade_data["Amount Token0"].abs().sum()
    total_volume_token1 = trade_data["Amount Token1"].abs().sum()
    total_volume_usd = trade_data["Amount USD"].sum()

    # Trade Flow
    buy_trades = trade_data[trade_data["Trade Direction"] == "Buy"]
    sell_trades = trade_data[trade_data["Trade Direction"] == "Sell"]

    # Create insights DataFrame
    insights = pd.DataFrame({
        "Analysis Timeframe": [f"{start_time} to {end_time}"],
        "Highest Trade (USD)": [highest_trade],
        "Lowest Trade (USD)": [lowest_trade],
        "Total Volume Token0": [total_volume_token0],
        "Total Volume Token1": [total_volume_token1],
        "Total Volume (USD)": [total_volume_usd],
        "Number of Buy Trades": [len(buy_trades)],
        "Number of Sell Trades": [len(sell_trades)],
        "Token0": [token0_symbol],
        "Token1": [token1_symbol]
    })
    return insights

def display_recent_trades(selected_pools, uniswap_url):
    """
    Display recent trades for selected pools in a Streamlit app.

    Args:
        selected_pools (list): List of pool IDs.
        uniswap_url (str): Uniswap Graph API endpoint.
    """
    st.title("Uniswap Recent Trades")
    st.write("Real-time updates of recent trades for selected pools:")

    # Initialize an empty dataframe
    trade_data = pd.DataFrame(columns=[
        "Pool", "Token0 Symbol", "Token1 Symbol", "Amount Token0", "Amount Token1", 
        "Amount USD", "Sender", "Timestamp", "Trade Direction", "Volatility"
    ])

    # Placeholder for the table
    trade_table = st.empty()

    while True:
        all_insights = pd.DataFrame()
        for pool_id in selected_pools:
            if pool_id not in token_metadata:
                token_metadata[pool_id] = fetch_token_metadata(pool_id, uniswap_url)

            metadata = token_metadata.get(pool_id, {})
            token0_symbol = metadata.get("token0_symbol", "Unknown")
            token1_symbol = metadata.get("token1_symbol", "Unknown")

            trades = fetch_recent_trades(pool_id, uniswap_url)
            for trade in trades:
                try:
                    amount_token0 = float(trade.get("amount0", 0))
                    amount_token1 = float(trade.get("amount1", 0))
                    trade_direction = "Buy" if amount_token0 < 0 else "Sell"

                    new_trade = {
                        "Pool": pool_id,
                        "Token0 Symbol": token0_symbol,
                        "Token1 Symbol": token1_symbol,
                        "Amount Token0": amount_token0,
                        "Amount Token1": amount_token1,
                        "Amount USD": float(trade.get("amountUSD", 0)),
                        "Sender": trade.get("sender", "Unknown"),
                        "Timestamp": pd.to_datetime(int(trade.get("timestamp", 0)), unit="s"),
                        "Trade Direction": trade_direction
                    }
                    trade_data = pd.concat([trade_data, pd.DataFrame([new_trade])], ignore_index=True)

                except Exception as e:
                    logger.error(f"Error processing trade data: {e}")

            # Calculate volatility and add to DataFrame
            trade_data["Volatility"] = calculate_volatility(trade_data)

            # Keep only the most recent 50 trades
            trade_data = trade_data.sort_values(by="Timestamp", ascending=False).head(50)

            # Generate insights for the current pool
            insights = analyze_top_trades(trade_data, token0_symbol, token1_symbol)
            all_insights = pd.concat([all_insights, insights], ignore_index=True)

        # Update the Streamlit table for insights
        st.subheader("Trade Analysis Insights by Pool")
        st.dataframe(all_insights)

        # Update the Streamlit table for recent trades
        st.subheader("Recent Trades")
        trade_table.dataframe(trade_data)

        # Refresh every 10 seconds
        time.sleep(10)

# Run the Streamlit app
display_recent_trades(SELECTED_POOLS, UNISWAP_ARBITRUM_URL)
