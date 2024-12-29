import requests
import pandas as pd
import streamlit as st
from math import sqrt
from decimal import Decimal
import time
import logging
import plotly.express as px

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

def analyze_trends(trade_data, timeframe):
    """
    Analyze trends over a specified timeframe.
    """
    if trade_data.empty:
        return pd.DataFrame()

    now = pd.Timestamp.now()
    filtered_data = trade_data[trade_data["Timestamp"] >= now - pd.Timedelta(minutes=timeframe)]

    avg_price = filtered_data["Amount USD"].mean()
    total_volume = filtered_data["Amount USD"].sum()
    buy_trades = filtered_data[filtered_data["Trade Direction"] == "Buy"].shape[0]
    sell_trades = filtered_data[filtered_data["Trade Direction"] == "Sell"].shape[0]

    return pd.DataFrame({
        "Timeframe (mins)": [timeframe],
        "Average Price (USD)": [avg_price],
        "Total Volume (USD)": [total_volume],
        "Buy Trades": [buy_trades],
        "Sell Trades": [sell_trades]
    })

def display_dashboard(selected_pools, uniswap_url):
    """
    Display the Uniswap dashboard with current and trend data.
    """
    st.title("Uniswap Dashboard")

    # Current Prices Section
    st.header("Current Prices")
    current_prices = []
    for pool_id in selected_pools:
        if pool_id not in token_metadata:
            token_metadata[pool_id] = fetch_token_metadata(pool_id, uniswap_url)

        metadata = token_metadata.get(pool_id, {})
        token0_symbol = metadata.get("token0_symbol", "Unknown")
        token1_symbol = metadata.get("token1_symbol", "Unknown")

        trades = fetch_recent_trades(pool_id, uniswap_url)
        if trades:
            latest_trade = trades[0]
            current_prices.append({
                "Token Pair": f"{token0_symbol}/{token1_symbol}",
                "Latest Price (USD)": float(latest_trade.get("amountUSD", 0)),
                "Timestamp": pd.to_datetime(int(latest_trade.get("timestamp", 0)), unit="s")
            })

    st.table(pd.DataFrame(current_prices))

    # Trends Section
    st.header("Trends")
    trade_data = pd.DataFrame(columns=[
        "Pool", "Token0 Symbol", "Token1 Symbol", "Amount Token0", "Amount Token1", 
        "Amount USD", "Sender", "Timestamp", "Trade Direction"
    ])

    all_trends = pd.DataFrame()
    timeframes = [1, 5, 15]

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

        for timeframe in timeframes:
            trends = analyze_trends(trade_data, timeframe)
            all_trends = pd.concat([all_trends, trends], ignore_index=True)

    st.table(all_trends)

    # Graphs for Visualization
    st.header("Price Trends")
    if not trade_data.empty:
        fig = px.line(
            trade_data, 
            x="Timestamp", 
            y="Amount USD", 
            color="Trade Direction", 
            title="Price Movement"
        )
        st.plotly_chart(fig)

# Run the Streamlit app
display_dashboard(SELECTED_POOLS, UNISWAP_ARBITRUM_URL)
