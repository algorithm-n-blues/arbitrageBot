import requests
import logging
import ast
import pandas as pd
from utils import add_timestamp
from decimal import Decimal
from math import sqrt

# Logger setup for debugging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("debug.log")
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Uniswap Graph API URL for Arbitrum
API_KEY = "e12c2830e44d2ed329aa22ec5a73fb81"  # Replace with your Graph Gateway API key
UNISWAP_ARBITRUM_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

def calculate_uniswap_price(sqrt_price, decimals0, decimals1):
    """
    Calculate Uniswap price from sqrtPriceX96.

    Args:
        sqrt_price (int): SqrtPriceX96 value.
        decimals0 (int): Decimals for token0.
        decimals1 (int): Decimals for token1.

    Returns:
        float: Derived price of token1 per token0.
    """
    try:
        if sqrt_price == 0:
            return 0
        price = (sqrt(sqrt_price / (2**96)) ** 2) * (10 ** (decimals0 - decimals1))
        return price
    except Exception as e:
        logger.error(f"Error calculating Uniswap price: {e}")
        return 0

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
        DataFrame: Processed pool data with derived prices and 'pair' column.
    """
    query = f"""
    {{
      pools(first: {first}, orderBy: {order_by}, orderDirection: {order_direction}) {{
        id
        token0 {{
          symbol
          decimals
          id
        }}
        token1 {{
          symbol
          decimals
          id
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
        # Request data from The Graph API
        response = requests.post(url, json={"query": query})
        response.raise_for_status()
        data = response.json()

        # Check for unexpected API response structure
        if "data" not in data or "pools" not in data["data"]:
            logger.error("Unexpected response structure from Uniswap API.")
            return pd.DataFrame()

        pools = data["data"]["pools"]

        # Process and flatten pool data
        processed_pools = []
        for pool in pools:
            # Skip pools with missing token data
            if not pool.get("token0") or not pool.get("token1"):
                logger.warning(f"Skipping pool {pool.get('id', 'unknown')} due to missing token data.")
                continue

            try:
                processed_pool = {
                    "id": pool["id"],
                    "token0_symbol": pool["token0"].get("symbol", "UNKNOWN").upper(),
                    "token0_decimals": int(pool["token0"].get("decimals", 0)),
                    "token0_id": pool["token0"].get("id", ""),
                    "token1_symbol": pool["token1"].get("symbol", "UNKNOWN").upper(),
                    "token1_decimals": int(pool["token1"].get("decimals", 0)),
                    "token1_id": pool["token1"].get("id", ""),
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

                # Add derived prices and pair column
                processed_pool["price_token1_per_token0"] = price_token1_per_token0
                processed_pool["price_token0_per_token1"] = price_token0_per_token1
                processed_pool["pair"] = f"{processed_pool['token0_symbol']}/{processed_pool['token1_symbol']}"

                processed_pools.append(processed_pool)

            except Exception as e:
                logger.error(f"Error processing pool {pool.get('id', 'unknown')}: {e}", exc_info=True)

        # Log summary and return DataFrame
        logger.info(f"Processed {len(processed_pools)} valid pools.")
        logger.debug(f"Processed Pools: {processed_pools[:5]}")  # Log first 5 pools for debug
        return pd.DataFrame(processed_pools)

    except requests.RequestException as e:
        logger.error(f"Request error fetching top pools from {url}: {e}", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Unexpected error in fetch_top_uniswap_pools: {e}", exc_info=True)
        return pd.DataFrame()

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