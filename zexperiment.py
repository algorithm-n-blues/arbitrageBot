import os
import asyncio
import logging
import aiohttp
import pandas as pd
from aiohttp import ClientSession
from pyth.pyth_keys import pyth_keys
import requests

# Initialize logger
logging.basicConfig(level=logging.INFO)

# Directory for saving data
DATA_DIRECTORY = "data"
os.makedirs(DATA_DIRECTORY, exist_ok=True)

# Uniswap Graph API URL for Arbitrum
API_KEY = "e12c2830e44d2ed329aa22ec5a73fb81"  # Replace with your Graph Gateway API key
UNISWAP_ARBITRUM_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

# Fetch Pyth Network Prices
async def fetch_pyth_network_prices(session, pyth_network_filtered_keys):
    HERMES_BASE_URL = "https://hermes.pyth.network"
    endpoint = "/v2/updates/price/latest"
    prices_data = []
    id_to_symbol = {v: k for k, v in pyth_network_filtered_keys.items()}

    try:
        async with session.get(
            f"{HERMES_BASE_URL}{endpoint}",
            params={
                "ids[]": list(pyth_network_filtered_keys.values()),
                "encoding": "base64",
                "parsed": "true",
                "ignore_invalid_price_ids": "true"
            },
        ) as response:
            response.raise_for_status()
            json_data = await response.json()

            for entry in json_data['parsed']:
                entry_id = entry['id']
                symbol = id_to_symbol.get(entry_id, "Unknown Symbol").split("/")[0]

                if symbol == "Unknown Symbol":
                    logging.warning(f"Unknown symbol for entry ID: {entry_id}")
                    continue

                price_data = entry['price']
                ema_price_data = entry.get('ema_price', {})

                price = float(price_data['price']) * (10 ** price_data['expo'])
                price_conf = float(price_data.get('conf', 0)) * (10 ** price_data['expo'])
                ema_price = float(ema_price_data.get('price', 0)) * (10 ** ema_price_data.get('expo', 0))
                ema_conf = float(ema_price_data.get('conf', 0)) * (10 ** ema_price_data.get('expo', 0))

                prices_data.append({
                    "symbol": symbol,
                    "Price": price,
                    "EMA Price": ema_price,
                    "Price Confidence": price_conf,
                    "EMA Price Confidence": ema_conf,
                })

            logging.info(f"Pyth Network prices fetched successfully.")
            return prices_data
    except Exception as e:
        logging.error(f"Exception during fetching price data: {str(e)}")
        return []

# Save Pyth Data to CSV
async def save_pyth_data_to_csv(prices_data, filename):
    if prices_data:
        df = pd.DataFrame(prices_data)
        df.to_csv(filename, index=False)
        logging.info(f"Pyth Network prices saved to {filename}")
    else:
        logging.error("No Pyth Network data to save.")

# Fetch Aave Data
async def fetch_aave_data(session):
    AAVE_API_URL = "https://aave-api-v2.aave.com/data/markets-data"
    try:
        async with session.get(AAVE_API_URL) as response:
            response.raise_for_status()
            data = await response.json()

            # Filter for Arbitrum reserves
            arbitrum_reserves = [
                reserve for reserve in data.get("reserves", [])
                if reserve.get("totalLiquidityUSD") and float(reserve["totalLiquidityUSD"]) > 1000000
            ]

            if not arbitrum_reserves:
                logging.warning("No reserves found for Arbitrum.")
                return []

            # Sort reserves by total liquidity and pick the top 10
            top_reserves = sorted(
                arbitrum_reserves,
                key=lambda x: float(x.get("totalLiquidityUSD", 0)),
                reverse=True
            )[:10]

            logging.info(f"Top Arbitrum reserves: {top_reserves}")
            return top_reserves
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching Aave data: {e}")
        return []

# Save Aave Data to CSV
def save_aave_data_to_csv(aave_data, filename):
    if aave_data:
        df = pd.DataFrame(aave_data)
        df.to_csv(filename, index=False)
        logging.info(f"Aave data saved to {filename}.")
    else:
        logging.error("No Aave data to save.")

# Fetch Top Pools from Uniswap
def fetch_top_uniswap_pools(url, first=50):
    query = """
    {
      pools(first: %d, orderBy: volumeUSD, orderDirection: desc) {
        id
        token0 {
          symbol
          id
        }
        token1 {
          symbol
          id
        }
        volumeUSD
        totalValueLockedUSD
      }
    }
    """ % first
    
    try:
        response = requests.post(url, json={"query": query})
        response.raise_for_status()
        data = response.json()
        return data["data"]["pools"]
    except Exception as e:
        logging.error(f"Error fetching top pools from {url}: {str(e)}")
        return []

# Save Uniswap Data to CSV
def save_uniswap_data_to_csv(pools, filename):
    if pools:
        df = pd.DataFrame(pools)
        df.to_csv(filename, index=False)
        logging.info(f"Uniswap pools data saved to {filename}")
    else:
        logging.error("No Uniswap pools data to save.")

# Main execution function
async def main():
    async with ClientSession() as session:
        # Fetch and save Pyth Network data
        pyth_prices = await fetch_pyth_network_prices(session, pyth_keys)
        pyth_file = os.path.join(DATA_DIRECTORY, "pyth_all_prices.csv")
        await save_pyth_data_to_csv(pyth_prices, pyth_file)

        # Fetch and save Aave data
        aave_reserves = await fetch_aave_data(session)
        aave_file = os.path.join(DATA_DIRECTORY, "arbitrum_aave_data.csv")
        save_aave_data_to_csv(aave_reserves, aave_file)

        # Fetch and save Uniswap pools data
        uniswap_pools = fetch_top_uniswap_pools(UNISWAP_ARBITRUM_URL)
        uniswap_file = os.path.join(DATA_DIRECTORY, "uniswap_top_pools.csv")
        save_uniswap_data_to_csv(uniswap_pools, uniswap_file)

if __name__ == "__main__":
    asyncio.run(main())
