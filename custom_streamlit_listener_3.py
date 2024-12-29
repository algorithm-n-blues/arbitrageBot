import streamlit as st
import pandas as pd
import aiohttp
import asyncio
import json

# Define the three data streams
price_feeds = {
    "ETH/USD": "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
    "BTC/USD": "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
    "USDT/USD": "2b89b9dc8fdf9f34709a5b106b472f0f39bb6ca9ce04b0fd7f2e971688e2e53b",
}

# Initialize a global DataFrame to store the price updates
price_data_df = pd.DataFrame(columns=["Price Feed", "Price", "Confidence", "Last Updated"])

# Define Pyth API URL
PYTH_API_URL = "https://hermes.pyth.network/v2/updates/price/stream"

# Placeholder for the real-time updates
data_placeholder = st.empty()

async def fetch_pyth_updates():
    global price_data_df
    async with aiohttp.ClientSession() as session:
        async with session.get(PYTH_API_URL, params={"ids[]": list(price_feeds.values())}) as response:
            if response.status != 200:
                st.error(f"Failed to connect to Pyth Network: {response.status}")
                return

            async for line in response.content:
                # Decode the line and filter out empty data
                line = line.decode("utf-8").strip()
                if not line or not line.startswith("data:"):
                    continue

                # Parse JSON data
                data = line[5:]  # Remove 'data:' prefix
                try:
                    updates = json.loads(data)
                except json.JSONDecodeError:
                    st.error("Error decoding JSON data from Pyth Network.")
                    continue

                parsed_updates = updates.get("parsed", [])
                if not isinstance(parsed_updates, list):
                    st.error("Unexpected data structure in updates.")
                    continue

                for update in parsed_updates:
                    if isinstance(update, dict):
                        feed_id = update.get("id")
                        if feed_id in price_feeds.values():
                            price_data = update.get("price", {})
                            price = float(price_data.get("price", 0))
                            confidence = float(price_data.get("conf", 0))
                            expo = int(price_data.get("expo", 0))
                            
                            # Decode price and confidence using expo
                            decoded_price = price * (10 ** expo)
                            decoded_confidence = confidence * (10 ** expo)

                            feed_name = next((k for k, v in price_feeds.items() if v == feed_id), "Unknown")
                            last_updated = pd.to_datetime("now")

                            # Skip updates with invalid or missing data
                            if decoded_price == 0 or decoded_confidence == 0:
                                continue

                            # Add or update entry in DataFrame
                            new_entry = pd.DataFrame([{
                                "Price Feed": feed_name,
                                "Price": decoded_price,
                                "Confidence": decoded_confidence,
                                "Last Updated": last_updated,
                            }])

                            # Concatenate new data
                            price_data_df = pd.concat([price_data_df, new_entry], ignore_index=True)

                            # Keep only the latest entry for each Price Feed
                            price_data_df.drop_duplicates(subset=["Price Feed"], keep="last", inplace=True)

                # Update the Streamlit display
                with data_placeholder.container():
                    st.dataframe(
                        price_data_df.style.format({
                            "Price": "${:,.8f}",
                            "Confidence": "±${:,.8f}"
                        })
                    )

def decode_price_data(price_data):
    """
    Decodes price and confidence data from the Pyth API response.
    Args:
        price_data (dict): A dictionary containing "price", "conf", and "expo" fields.
    Returns:
        tuple: Decoded price and confidence values.
    """
    try:
        price = float(price_data["price"])
        confidence = float(price_data["conf"])
        expo = int(price_data["expo"])
        decoded_price = price * (10 ** expo)
        decoded_confidence = confidence * (10 ** expo)
        return decoded_price, decoded_confidence
    except (ValueError, KeyError) as e:
        st.error(f"Error decoding price data: {e}")
        return None, None

async def run_pyth_listener():
    while True:
        await fetch_pyth_updates()
        await asyncio.sleep(2)  # Fetch every 2 seconds

# Streamlit UI
def display_dashboard():
    # Streamlit App Layout
    st.title("Pyth Network - Real-Time Price Feeds")
    st.write("Streaming Prices for ETH/USD, BTC/USD, and USDT/USD")

    # Create a container for the dynamic table
    header_container = st.container()
    data_placeholder = st.empty()

    # Ensure headers stay at the top
    with header_container:
        st.title("Pyth Network - Real-Time Price Feeds")
        st.write("Streaming Prices for ETH/USD, BTC/USD, and USDT/USD")
    if not price_data_df.empty:
        st.dataframe(
            price_data_df.style.format({
                "Price": "${:.6f}",
                "Confidence": "±${:.6f}"
            })
        )
    else:
        st.write("Awaiting price updates...")

async def main():
    display_dashboard()
    await run_pyth_listener()

# Run asyncio in Streamlit
asyncio.run(main())
