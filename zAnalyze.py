import pandas as pd
import os
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)

# Directory for data
DATA_DIRECTORY = "data"

def load_csv_file(filename):
    filepath = os.path.join(DATA_DIRECTORY, filename)
    try:
        df = pd.read_csv(filepath)
        logging.info(f"Loaded {filename} successfully.")
        return df
    except Exception as e:
        logging.error(f"Error loading {filename}: {e}")
        return pd.DataFrame()

def process_uniswap_data(uniswap_pools):
    try:
        # Extract token symbols from the nested dictionaries in `token0` and `token1`
        uniswap_pools['token0_symbol'] = uniswap_pools['token0'].apply(eval).apply(lambda x: x.get('symbol'))
        uniswap_pools['token1_symbol'] = uniswap_pools['token1'].apply(eval).apply(lambda x: x.get('symbol'))
        uniswap_pools['token0_id'] = uniswap_pools['token0'].apply(eval).apply(lambda x: x.get('id'))
        uniswap_pools['token1_id'] = uniswap_pools['token1'].apply(eval).apply(lambda x: x.get('id'))
        
        # Drop original `token0` and `token1` columns for clarity
        uniswap_pools = uniswap_pools.drop(columns=['token0', 'token1'])

        logging.info("Processed Uniswap pools data successfully.")
        return uniswap_pools
    except Exception as e:
        logging.error(f"Error processing Uniswap data: {e}")
        return pd.DataFrame()

def analyze_arbitrage():
    # Load the data
    pyth_prices = load_csv_file("pyth_all_prices.csv")
    aave_data = load_csv_file("arbitrum_aave_data.csv")
    uniswap_pools = load_csv_file("uniswap_top_pools.csv")
    
    # Ensure the dataframes are not empty
    if pyth_prices.empty or aave_data.empty or uniswap_pools.empty:
        logging.error("One or more datasets are empty. Analysis cannot proceed.")
        return pd.DataFrame()

    # Process Uniswap pools data to extract nested symbols
    uniswap_pools = process_uniswap_data(uniswap_pools)

    # Clean and prepare Pyth prices
    pyth_prices.rename(columns={"symbol": "token", "Price": "pyth_price"}, inplace=True)

    # Clean and prepare Aave data
    aave_data.rename(columns={"symbol": "token", "totalLiquidityUSD": "aave_liquidity_usd"}, inplace=True)
    aave_data["aave_liquidity_usd"] = pd.to_numeric(aave_data["aave_liquidity_usd"], errors="coerce")
    aave_data["aave_borrow_rate"] = pd.to_numeric(aave_data["variableBorrowRate"], errors="coerce")

    # Clean and prepare Uniswap pools data
    uniswap_pools["pool_tvl_usd"] = pd.to_numeric(uniswap_pools["totalValueLockedUSD"], errors="coerce")
    uniswap_pools["volume_usd"] = pd.to_numeric(uniswap_pools["volumeUSD"], errors="coerce")

    # Identify arbitrage opportunities by merging datasets
    merged_data = pd.merge(aave_data, pyth_prices, left_on="token", right_on="token", how="inner")
    merged_data = pd.merge(merged_data, uniswap_pools, left_on="token", right_on="token0_symbol", how="inner")

    # Calculate arbitrage potential (example: Pyth price vs. Uniswap TVL ratio)
    merged_data["arbitrage_potential"] = (
        merged_data["pyth_price"] - merged_data["pool_tvl_usd"]
    ).abs() / merged_data["pool_tvl_usd"]

    # Filter for significant arbitrage opportunities (arbitrary threshold)
    arbitrage_opportunities = merged_data[merged_data["arbitrage_potential"] > 0.1]

    # Organize the results
    results = arbitrage_opportunities[
        [
            "token",
            "pyth_price",
            "aave_liquidity_usd",
            "aave_borrow_rate",
            "pool_tvl_usd",
            "volume_usd",
            "arbitrage_potential",
        ]
    ].sort_values(by="arbitrage_potential", ascending=False)

    # Save the results to a new CSV file
    output_file = os.path.join(DATA_DIRECTORY, "arbitrage_opportunities.csv")
    results.to_csv(output_file, index=False)
    logging.info(f"Arbitrage analysis saved to {output_file}")

    return results

if __name__ == "__main__":
    results = analyze_arbitrage()
    if not results.empty:
        logging.info("Top arbitrage opportunities:")
        print(results.head(10))
    else:
        logging.info("No significant arbitrage opportunities found.")
