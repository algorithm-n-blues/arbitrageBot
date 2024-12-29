import streamlit as st
import pandas as pd
import asyncio
import aiohttp
from web3 import Web3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pyth.pyth_keys import pyth_keys  # Ensure this imports your pyth_keys dictionary
import logging
import json
import re

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

# Pyth Network API URL
HERMES_BASE_URL = "https://hermes.pyth.network/v2/updates/price/stream"

# Split tokens into categories
stablecoins = ["USDT", "USDC", "DAI", "wstETH"]
others = [key.split("/")[0] for key in pyth_keys.keys() if key.split("/")[0] not in stablecoins]

# Web3 and Pyth configurations
INFURA_URL = "https://arbitrum-mainnet.infura.io/v3/YOUR_INFURA_KEY"
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
PYTH_API_URL = "https://pyth.network/v2/updates/price/stream"

# Swap event ABI
SWAP_EVENT_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "sender", "type": "address"},
            {"indexed": False, "internalType": "uint256", "name": "amount0In", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "amount1In", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "amount0Out", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "amount1Out", "type": "uint256"},
            {"indexed": False, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
        ],
        "name": "Swap",
        "type": "event"
    }
]

# Example pool addresses for Uniswap pairs
TOP_POOL_ADDRESSES = [
    "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",  # USDC/WETH
    "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed",  # WBTC/WETH
    # Add more pool addresses here
]

# Initialize dataframes for display
uniswap_prices_df = pd.DataFrame(columns=["Pool", "Price", "Amount0In", "Amount1In", "Amount0Out", "Amount1Out"])
arbitrage_opportunities_df = pd.DataFrame(columns=["Pair", "Uniswap Price", "Pyth Price", "Deviation", "Timestamp"])

# Global DataFrame to store Pyth price updates
pyth_prices_df = pd.DataFrame(columns=["price_feed_id", "price", "confidence", "timestamp"])

buffer = ""

HIGH_CONFIDENCE_THRESHOLD = 1e6  # Example threshold for high confidence

def log_confidence_warning(price_feed_id, confidence):
    if confidence > HIGH_CONFIDENCE_THRESHOLD:
        logger.warning(
            f"High confidence level detected for {price_feed_id}: {confidence}"
        )

def append_to_dataframe(df, new_row):
    """
    Append a new row to the DataFrame while handling edge cases.
    """
    if new_row and any(new_row.values()):  # Check for non-empty and non-NA row
        new_row_df = pd.DataFrame([new_row]).dropna(how='all')
        return pd.concat([df, new_row_df], ignore_index=True)
    else:
        logger.debug("Skipping empty or all-NA row.")
        return df
    
def handle_json_chunk(chunk):
    """
    Process incoming JSON chunks and buffer incomplete data.
    """
    global buffer
    buffer += chunk
    complete_json_objects = []
    
    try:
        # Regex to match complete JSON objects
        pattern = re.compile(r'{.*?}(?=\s*{|\s*$)', re.DOTALL)
        matches = pattern.findall(buffer)
        
        if matches:
            for match in matches:
                try:
                    complete_json_objects.append(json.loads(match))
                except json.JSONDecodeError as e:
                    logger.debug(f"Malformed JSON in match: {match[:50]}... Error: {e}")
                    continue
            
            # Update buffer to exclude processed JSON
            buffer = buffer[len("".join(matches)):]
        
    except Exception as e:
        logger.error(f"Error processing JSON chunk: {e}")
    
    return complete_json_objects

def sqrt_price_to_price(sqrt_price):
    return (sqrt_price / (2 ** 96)) ** 2

def handle_uniswap_swap_event(event, pool_address):
    """
    Handle a Uniswap swap event and update the dataframe.
    Args:
        event (dict): The event data containing swap information.
        pool_address (str): The address of the Uniswap pool.
    """
    global uniswap_prices_df

    # Parse event arguments
    args = event["args"]
    sqrt_price = args["sqrtPriceX96"]
    price = sqrt_price_to_price(sqrt_price)

    # Create a new entry
    new_entry = {
        "Pool": pool_address,
        "Price": price,
        "Amount0In": args["amount0In"],
        "Amount1In": args["amount1In"],
        "Amount0Out": args["amount0Out"],
        "Amount1Out": args["amount1Out"],
    }

    # Append the new entry to the DataFrame
    uniswap_prices_df = append_to_dataframe(uniswap_prices_df, new_entry)

    # Update Streamlit's session state to reflect the changes in the UI
    st.session_state["uniswap_prices_df"] = uniswap_prices_df

    # Log the event processing
    logger.info(f"Processed Uniswap Swap Event for Pool: {pool_address}, Price: {price}")

def listen_to_uniswap_pool(pool_address):
    pool_contract = web3.eth.contract(address=pool_address, abi=SWAP_EVENT_ABI)
    event_filter = pool_contract.events.Swap.createFilter(fromBlock="latest")

    while True:
        for event in event_filter.get_new_entries():
            handle_uniswap_swap_event(event, pool_address)

def process_pyth_update(update):
    try:
        price_feed_id = update.get("id")
        price_data = update.get("price", {})
        price = price_data.get("price")
        confidence = price_data.get("conf")
        expo = price_data.get("expo")
        timestamp = update.get("metadata", {}).get("proof_available_time")

        if not price_feed_id:
            logger.warning(f"Update missing 'id': {update}")
            return

        if price is None or confidence is None or expo is None or timestamp is None:
            logger.warning(f"Incomplete data in update: {update}")
            return

        timestamp = pd.to_datetime(timestamp, unit="s")
        adjusted_price = float(price) * (10 ** expo)

        logger.info(
            f"Processed Pyth Update: {price_feed_id} - Adjusted Price: {adjusted_price}, Confidence: {confidence}"
        )

        global pyth_prices_df
        new_row = {
            "price_feed_id": price_feed_id,
            "price": adjusted_price,
            "confidence": float(confidence),
            "timestamp": timestamp,
        }
        pyth_prices_df = append_to_dataframe(pyth_prices_df, new_row)

    except Exception as e:
        logger.error(f"Error processing Pyth update: {e}")

# Updated listener logic for Pyth updates
async def listen_to_pyth():
    """
    Listen to Pyth price updates via SSE and process updates in real time.
    """
    url = HERMES_BASE_URL
    price_feed_ids = [feed_id.lstrip("0x") for feed_id in pyth_keys.values()]
    valid_feed_ids = [feed_id for feed_id in price_feed_ids if len(feed_id) % 2 == 0]

    if not valid_feed_ids:
        logger.error("No valid price feed IDs available. Aborting Pyth listener.")
        return

    logger.info(f"Starting Pyth listener with valid feed IDs: {valid_feed_ids}")

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                url,
                params={
                    "ids[]": valid_feed_ids,
                    "parsed": "true",
                    "encoding": "base64",
                    "ignore_invalid_price_ids": "true",
                },
            ) as response:
                if response.status != 200:
                    logger.error(f"Failed to connect to Pyth SSE. Status: {response.status}")
                    return

                logger.info("Connected to Pyth price update stream.")

                async for chunk in response.content.iter_any():
                    try:
                        # Decode the chunk and clean the data
                        chunk_data = chunk.decode("utf-8").strip()
                        if not chunk_data:
                            logger.debug("Received empty chunk; skipping...")
                            continue

                        # Parse only valid data chunks
                        if chunk_data.startswith("data:"):
                            chunk_data = chunk_data[5:].strip()

                        # Process complete JSON objects
                        complete_json_objects = handle_json_chunk(chunk_data)
                        for json_obj in complete_json_objects:
                            if "parsed" in json_obj:
                                for pyth_update in json_obj["parsed"]:
                                    process_pyth_update(pyth_update)

                        # Update Streamlit session state for real-time UI
                        st.session_state["pyth_prices_df"] = pyth_prices_df

                        logger.info(
                            f"Updated Pyth prices DataFrame with latest data. Current entries: {len(pyth_prices_df)}"
                        )

                    except Exception as e:
                        logger.error(f"Error processing Pyth update chunk: {e}")

        except aiohttp.ClientConnectorError as e:
            logger.error(f"Connection error while connecting to Pyth: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in listen_to_pyth: {e}")

st.title("Real-Time Arbitrage Tracker")
st.subheader("Uniswap and Pyth Price Monitoring")

st.write("Uniswap Swap Prices")
st.dataframe(uniswap_prices_df)

st.write("Pyth Price Updates")
st.dataframe(pyth_prices_df)

st.write("Price Deviations and Arbitrage Opportunities")
if not uniswap_prices_df.empty and not pyth_prices_df.empty:
    arbitrage_opportunities_df = pd.DataFrame({
        "Pair": uniswap_prices_df["Pool"],
        "Uniswap Price": uniswap_prices_df["Price"],
        "Pyth Price": pyth_prices_df["price"],
        "Deviation": abs(uniswap_prices_df["Price"] - pyth_prices_df["price"]),
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    st.dataframe(arbitrage_opportunities_df)

async def run_listeners():
    with ThreadPoolExecutor() as executor:
        for pool_address in TOP_POOL_ADDRESSES:
            executor.submit(listen_to_uniswap_pool, pool_address)
        await listen_to_pyth()

asyncio.run(run_listeners())
