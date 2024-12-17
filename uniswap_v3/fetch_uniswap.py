import requests
import logging
import pandas as pd
from utils import add_timestamp

# Logger setup for debugging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("debug.log")
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Uniswap Graph API URL for Arbitrum
API_KEY = "e12c2830e44d2ed329aa22ec5a73fb81"  # Replace with your Graph Gateway API key
UNISWAP_ARBITRUM_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

# Fetch Top Pools from Uniswap
def fetch_top_uniswap_pools(url, first=50, order_by="totalValueLockedUSD", order_direction="desc"):
    """
    Fetch top pools from Uniswap using The Graph API.

    Args:
        url (str): The Graph API URL for Uniswap.
        first (int): Number of pools to fetch.
        order_by (str): Field to order by.
        order_direction (str): Sort direction ("asc" or "desc").

    Returns:
        list: List of pool data dictionaries, including recent trade volume.
    """
    # Define token pairs to exclude
    excluded_pairs = {"ease.org / ez-cvxsteCRV", "ease.org / ez-yvCurve-IronBank", "ease.org / ez-SLP-WBTC-WETH"}

    query = f"""
    {{
      pools(first: {first}, orderBy: {order_by}, orderDirection: {order_direction}) {{
        id
        token0 {{
          symbol
        }}
        token1 {{
          symbol
        }}
        token0Price
        token1Price
        totalValueLockedToken0
        totalValueLockedToken1
        totalValueLockedUSD
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

        # Add recent volume for each pool
        filtered_pools = []
        for pool in pools:
            pool["token0_symbol"] = pool["token0"]["symbol"]
            pool["token1_symbol"] = pool["token1"]["symbol"]
            pool["token_pair"] = f"{pool['token0_symbol']} / {pool['token1_symbol']}"

            # Skip excluded pairs
            if pool["token_pair"] in excluded_pairs:
                continue

            # Fetch recent trade volume (e.g., last 15 seconds)
            recent_volume = fetch_pool_volume_details(pool["id"], url, interval="15 seconds")
            pool["recentVolumeUSD"] = recent_volume["volumeUSD"]

            filtered_pools.append(pool)

        logger.info(f"Fetched {len(filtered_pools)} pools from Uniswap with recent volume.")
        return filtered_pools
    except Exception as e:
        logger.error(f"Error fetching top pools from {url}: {str(e)}")
        return []

# Fetch detailed pool data for a specific pool ID
def fetch_pool_details(pool_id):
    """
    Fetch detailed information about a specific Uniswap pool.

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
        feeTier
        liquidity
        sqrtPrice
        tick
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
            logger.debug(f"Successfully fetched details for pool {pool_id}: {data['data']['pool']}")
            return data["data"]["pool"]
        else:
            logger.error(f"Invalid response for pool {pool_id}: {data}")
            return None
    except Exception as e:
        logger.error(f"Error fetching pool details for {pool_id}: {str(e)}")
        return None

# Save Uniswap Data to CSV
def save_uniswap_data_to_csv(pools, filename):
    """
    Save Uniswap pool data to a CSV file.

    Args:
        pools (list): List of pool data dictionaries.
        filename (str): File path to save the CSV.
    """
    if pools:
        # Convert to DataFrame and rearrange columns to bring token symbols to the front
        df = pd.DataFrame(pools)
        token_columns = ["token0_symbol", "token1_symbol"]
        other_columns = [col for col in df.columns if col not in token_columns]
        df = df[token_columns + other_columns]
        df = add_timestamp(df)
        df.to_csv(filename, index=False)
        logger.info(f"Uniswap pools data saved to {filename}")
    else:
        logger.error("No Uniswap pools data to save.")

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
