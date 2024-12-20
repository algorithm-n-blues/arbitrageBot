import requests
import logging
import ast
import pandas as pd
from utils import add_timestamp
from decimal import Decimal

# Logger setup for debugging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("debug.log")
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Uniswap Graph API URL for Arbitrum
API_KEY = "e12c2830e44d2ed329aa22ec5a73fb81"  # Replace with your Graph Gateway API key
UNISWAP_ARBITRUM_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

def calculate_uniswap_price(sqrtPriceX96, decimals0, decimals1):
    """
    Calculate the price from Uniswap V3 sqrtPriceX96.

    Args:
        sqrtPriceX96 (int): The sqrtPriceX96 value from Uniswap.
        decimals0 (int): Number of decimals for token0.
        decimals1 (int): Number of decimals for token1.

    Returns:
        float: The calculated price of token1/token0.
    """
    price = (Decimal(sqrtPriceX96) / (2**96))**2
    adjusted_price = float(price) * 10**(decimals0 - decimals1)
    return adjusted_price

# Fetch Top Pools from Uniswap
def fetch_top_uniswap_pools(url, first=50, order_by="totalValueLockedUSD", order_direction="desc"):
    """
    Fetch top pools from Uniswap using The Graph API and calculate derived prices.

    Args:
        url (str): The Graph API URL for Uniswap.
        first (int): Number of pools to fetch.
        order_by (str): Field to order by.
        order_direction (str): Sort direction ("asc" or "desc").

    Returns:
        list: List of pool data dictionaries, including derived prices.
    """
    query = f"""
    {{
      pools(first: {first}, orderBy: {order_by}, orderDirection: {order_direction}) {{
        id
        token0 {{
          symbol
          decimals
        }}
        token1 {{
          symbol
          decimals
        }}
        sqrtPrice
        totalValueLockedUSD
        totalValueLockedToken0
        totalValueLockedToken1
        volumeUSD
        feeTier
      }}
    }}
    """

    try:
        response = requests.post(url, json={"query": query})
        response.raise_for_status()
        data = response.json()
        pools = data["data"]["pools"]

        # Flatten and process each pool
        processed_pools = []
        for pool in pools:
            try:
                # Skip pools with missing token data
                if "token0" not in pool or "token1" not in pool or not pool["token0"] or not pool["token1"]:
                    logger.warning(f"Skipping pool {pool.get('id', 'unknown')} due to missing token data.")
                    continue

                # Extract and flatten relevant fields
                processed_pool = {
                    "id": pool["id"],
                    "token0_symbol": pool["token0"].get("symbol", "UNKNOWN"),
                    "token0_decimals": int(pool["token0"].get("decimals", 0)),
                    "token1_symbol": pool["token1"].get("symbol", "UNKNOWN"),
                    "token1_decimals": int(pool["token1"].get("decimals", 0)),
                    "sqrtPrice": int(pool.get("sqrtPrice", 0)),
                    "totalValueLockedUSD": float(pool.get("totalValueLockedUSD", 0)),
                    "totalValueLockedToken0": float(pool.get("totalValueLockedToken0", 0)),
                    "totalValueLockedToken1": float(pool.get("totalValueLockedToken1", 0)),
                    "volumeUSD": float(pool.get("volumeUSD", 0)),
                    "feeTier": int(pool.get("feeTier", 0)),
                }

                # Calculate derived prices
                price_token1_per_token0 = calculate_uniswap_price(
                    processed_pool["sqrtPrice"],
                    processed_pool["token0_decimals"],
                    processed_pool["token1_decimals"],
                )
                price_token0_per_token1 = (
                    1 / price_token1_per_token0 if price_token1_per_token0 > 0 else 0
                )

                # Add derived prices
                processed_pool["price_token1_per_token0"] = price_token1_per_token0
                processed_pool["price_token0_per_token1"] = price_token0_per_token1

                processed_pools.append(processed_pool)
            except Exception as e:
                logger.error(f"Error processing pool {pool.get('id', 'unknown')}: {e}", exc_info=True)

        logger.info(f"Processed {len(processed_pools)} valid pools.")
        logger.debug(f"Processed Pools: {processed_pools[:5]}")  # Log first 5 pools for debug
        return processed_pools
    except Exception as e:
        logger.error(f"Error fetching top pools from {url}: {e}", exc_info=True)
        return []

def fetch_pool_details(pool_id):
    """
    Fetch detailed information about a specific Uniswap pool and calculate accurate prices.

    Args:
        pool_id (str): Pool ID to fetch details for.

    Returns:
        dict: Detailed pool information or None.
    """
    query = f"""
    {{
      pool(id: "{pool_id}") {{
        id
        token0 {{
          id
          symbol
          decimals
        }}
        token1 {{
          id
          symbol
          decimals
        }}
        sqrtPrice
        liquidity
        feeTier
        volumeUSD
        totalValueLockedToken0
        totalValueLockedToken1
        totalValueLockedUSD
      }}
    }}
    """
    try:
        response = requests.post(UNISWAP_ARBITRUM_URL, json={"query": query})
        response.raise_for_status()
        data = response.json()

        if data and "data" in data and "pool" in data["data"]:
            pool = data["data"]["pool"]
            
            # Extract token decimals
            decimals0 = int(pool["token0"]["decimals"])
            decimals1 = int(pool["token1"]["decimals"])
            
            # Calculate prices using sqrtPriceX96
            sqrt_price = int(pool["sqrtPrice"])
            logger.debug(f"Pool {pool_id} sqrtPriceX96: {sqrt_price}")
            price_token1_per_token0 = calculate_uniswap_price(sqrt_price, decimals0, decimals1)
            price_token0_per_token1 = 1 / price_token1_per_token0 if price_token1_per_token0 > 0 else 0

            # Add derived prices to pool details
            pool_details = {
                "id": pool["id"],
                "token0_symbol": pool["token0"]["symbol"],
                "token1_symbol": pool["token1"]["symbol"],
                "price_token1_per_token0": price_token1_per_token0,  # Derived price of token1 in terms of token0
                "price_token0_per_token1": price_token0_per_token1,  # Derived price of token0 in terms of token1
                "feeTier": pool["feeTier"],
                "liquidity": pool["liquidity"],
                "volumeUSD": pool["volumeUSD"],
                "totalValueLockedToken0": pool["totalValueLockedToken0"],
                "totalValueLockedToken1": pool["totalValueLockedToken1"],
                "totalValueLockedUSD": pool["totalValueLockedUSD"],
            }

            logger.debug(f"Pool {pool_id} details with derived prices: {pool_details}")
            return pool_details
        else:
            logger.error(f"Invalid response for pool {pool_id}: {data}")
            return None
    except Exception as e:
        logger.error(f"Error fetching pool details for {pool_id}: {str(e)}")
        return None

# Save Uniswap Data to CSV
def save_uniswap_data_to_csv(pools, file_path="data/uniswap_top_pools.csv"):
    """
    Save processed Uniswap pool data to a CSV file.

    Args:
        pools (list): List of processed pool data.
        file_path (str): Path to the CSV file.
    """
    try:
        if not pools:
            logger.warning("No pool data available to save.")
            return

        df = pd.DataFrame(pools)
        logger.debug(f"DataFrame Head:\n{df.head()}")  # Debug log for DataFrame
        df.to_csv(file_path, index=False)
        logger.info(f"Uniswap pools data saved to {file_path}")
    except Exception as e:
        logger.error(f"Error saving data to CSV: {e}", exc_info=True)

def fetch_pool_volume_details(pool_id, uniswap_url, interval="15 seconds"):
    """
    Fetch recent trade volume for a specific Uniswap pool.

    Args:
        pool_id (str): The ID of the Uniswap pool.
        uniswap_url (str): Uniswap Graph API endpoint.
        interval (str): Timeframe for volume analysis (e.g., "15 seconds").

    Returns:
        dict: Pool volume details, including trade volume in USD.
    """
    query = """
    query ($poolId: ID!, $interval: Int!) {
        pool(id: $poolId) {
            swaps(first: $interval) {
                volumeUSD
            }
        }
    }
    """
    variables = {
        "poolId": pool_id,
        "interval": 15,  # Querying swaps for the last 15 seconds
    }
    try:
        response = requests.post(uniswap_url, json={"query": query, "variables": variables})
        response.raise_for_status()
        data = response.json()
        total_volume = sum(float(swap["volumeUSD"]) for swap in data["data"]["pool"]["swaps"])
        return {"volumeUSD": total_volume}
    except Exception as e:
        logger.error(f"Error fetching pool volume details for {pool_id}: {e}")
        return {"volumeUSD": 0}

def fetch_pool_volumes(pools):
    """
    Fetch recent trading volumes for pools and handle missing data gracefully.
    """
    results = []
    for pool in pools:
        try:
            volume = pool.get("recentVolumeUSD", 0)
            if volume == 0:
                logger.warning(f"Pool {pool['id']} has zero recent volume. Skipping.")
                continue
            results.append({
                "id": pool["id"],
                "recentVolumeUSD": volume,
            })
        except Exception as e:
            logger.error(f"Error fetching pool volume details for {pool.get('id', 'unknown')}: {e}")
            continue
    return results
