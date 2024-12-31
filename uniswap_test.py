import logging
import pandas as pd
from uniswap_v3.fetch_uniswap import fetch_top_uniswap_pools
from math import sqrt
from decimal import Decimal

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("test_debug.log"),
    ],
)
logger = logging.getLogger(__name__)

# Uniswap configurations
API_KEY = "e12c2830e44d2ed329aa22ec5a73fb81"  # Replace with your Graph Gateway API key
UNISWAP_ARBITRUM_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

# Calculate token prices from sqrtPrice

def calculate_uniswap_price(sqrt_price, decimals0, decimals1):
    try:
        price_token1_per_token0 = (Decimal(sqrt_price) / (2 ** 96)) ** 2
        adjusted_price = float(price_token1_per_token0) * 10 ** (decimals0 - decimals1)
        return adjusted_price
    except Exception as e:
        logger.error(f"Error calculating Uniswap price: {e}")
        return 0

# Fetch and process Uniswap pool data
def main():
    try:
        logger.info("Fetching top Uniswap pools...")
        uniswap_pools = fetch_top_uniswap_pools(UNISWAP_ARBITRUM_URL, first=10)  # Fetch top 10 pools

        if not uniswap_pools.empty:
            logger.info(f"Fetched {len(uniswap_pools)} pools.")
        else:
            logger.warning("No pools fetched from Uniswap API.")
            return

        # Calculate and display derived token prices
        logger.info("Calculating token prices...")
        uniswap_pools["Token1 Price"] = uniswap_pools.apply(
            lambda row: calculate_uniswap_price(
                row["sqrtPrice"], row["token0_decimals"], row["token1_decimals"]
            ),
            axis=1,
        )
        uniswap_pools["Token0 Price"] = uniswap_pools["Token1 Price"].apply(
            lambda x: 1 / x if x > 0 else 0
        )

        print("Top Uniswap Pools with Calculated Token Prices:")
        print(uniswap_pools[["pair", "Token0 Price", "Token1 Price", "totalValueLockedUSD"]])

        # Save results
        uniswap_pools.to_csv("uniswap_pools_with_prices.csv", index=False)
        logger.info("Uniswap pools with prices saved to uniswap_pools_with_prices.csv.")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main()
