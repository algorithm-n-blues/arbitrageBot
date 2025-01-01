import os
import requests
import pandas as pd
import logging
from datetime import datetime

# Logger setup
logger = logging.getLogger("UniswapListener")
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("uniswap_trades.log")
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Directory for saving data
DATA_DIRECTORY = "data_5"
os.makedirs(DATA_DIRECTORY, exist_ok=True)
TRADE_DATA_FILE = os.path.join(DATA_DIRECTORY, "trade_data.csv")

# Uniswap Graph API URL for Arbitrum
API_KEY = "e12c2830e44d2ed329aa22ec5a73fb81"
UNISWAP_ARBITRUM_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

# Selected pools
SELECTED_POOLS = [
    "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",  # Example USDC/WETH pool
    "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed"   # Example WBTC/WETH pool
]

# Define column order for CSV
COLUMNS = ["Pool", "Amount Token0", "Amount Token1", "Amount USD", "Sender", "Timestamp"]

def fetch_recent_trades(pool_id, uniswap_url):
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

def save_trade_data(trade_data):
    if os.path.exists(TRADE_DATA_FILE):
        existing_data = pd.read_csv(TRADE_DATA_FILE)
        trade_data = pd.concat([existing_data, trade_data], ignore_index=True).drop_duplicates()
    trade_data.to_csv(TRADE_DATA_FILE, index=False)

def main():
    logger.info("Starting Uniswap trade listener...")

    # Create an empty DataFrame to store trades
    all_trades = pd.DataFrame(columns=COLUMNS)

    for pool_id in SELECTED_POOLS:
        trades = fetch_recent_trades(pool_id, UNISWAP_ARBITRUM_URL)
        
        for trade in trades:
            try:
                amount_token0 = float(trade.get("amount0", 0))
                amount_token1 = float(trade.get("amount1", 0))
                amount_usd = float(trade.get("amountUSD", 0))
                timestamp = datetime.fromtimestamp(int(trade.get("timestamp", 0)))
                sender = trade.get("sender", "Unknown")

                # Create a new trade entry
                new_trade = {
                    "Pool": pool_id,
                    "Amount Token0": amount_token0,
                    "Amount Token1": amount_token1,
                    "Amount USD": amount_usd,
                    "Sender": sender,
                    "Timestamp": timestamp,
                }

                # Append to DataFrame
                all_trades = pd.concat([all_trades, pd.DataFrame([new_trade])], ignore_index=True)

                # Log the trade
                logger.info(f"Logged trade: Pool={pool_id}, Token0={amount_token0}, Token1={amount_token1}, USD={amount_usd}, Timestamp={timestamp}, Sender={sender}")

            except Exception as e:
                logger.error(f"Error processing trade data: {e}")

    # Save trade data to CSV
    save_trade_data(all_trades)
    logger.info(f"Saved trade data to {TRADE_DATA_FILE}")

if __name__ == "__main__":
    main()
