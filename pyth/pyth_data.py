import os
import asyncio
import aiohttp
import logging
import pandas as pd
from .pyth_keys import pyth_keys

# Logger
logging.basicConfig(level=logging.INFO)

# Base URL for Pyth Network
HERMES_BASE_URL = "https://hermes.pyth.network"

async def fetch_pyth_network_prices(session, pyth_network_filtered_keys):
    """
    Fetch the latest price data from Pyth Network.
    """
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

def get_pyth_data():
    """
    Fetch and return Pyth Network data.
    """
    async def main():
        async with aiohttp.ClientSession() as session:
            return await fetch_pyth_network_prices(session, pyth_keys)

    # Run the async function safely using asyncio
    return asyncio.run(main())

def save_pyth_data_to_csv(pyth_data, filename):
    """
    Save Pyth Network price data to a CSV file.
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    if pyth_data:
        df = pd.DataFrame(pyth_data)
        df["Timestamp"] = pd.Timestamp.now()
        df.to_csv(filename, index=False)
        logging.info(f"Pyth Network price data saved to {filename}")
    else:
        logging.error("No Pyth Network data to save.")

def get_eth_usd_price():
    """
    Fetch and return the ETH/USD price from Pyth Network.
    """
    eth_usd_id = pyth_keys.get("ETH/USD")
    if not eth_usd_id:
        logging.error("ETH/USD key not found in Pyth keys.")
        return None

    async def fetch_eth_price():
        async with aiohttp.ClientSession() as session:
            try:
                endpoint = "/v2/updates/price/latest"
                async with session.get(
                    f"{HERMES_BASE_URL}{endpoint}",
                    params={"ids[]": eth_usd_id, "parsed": "true"}
                ) as response:
                    response.raise_for_status()
                    json_data = await response.json()
                    price_data = json_data["parsed"][0]["price"]
                    price = float(price_data["price"]) * (10 ** price_data["expo"])
                    return price
            except Exception as e:
                logging.error(f"Failed to fetch ETH/USD price: {e}")
                return None

    return asyncio.run(fetch_eth_price())
