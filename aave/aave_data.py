# aave_data.py

import requests
import json
import asyncio
import csv
import pandas as pd
import aiohttp
import os
import logging
from .aave_data_definitions import aave_data
#from trading_logic.substitutes import substitute_tokens

# Your API keys
ETHERSCAN_API_KEY = 'FZIA4N5JMAIC5ZYDGS6PTFHEHHMKBFHXN2'
ARBISCAN_API_KEY = 'your_arbiscan_api_key_here'

# Updated fetch_aave_data function
async def fetch_aave_data(session, use_local=False):
    """
    Fetch data from Aave API or local file and return as a processed DataFrame.

    Args:
        session (aiohttp.ClientSession): The aiohttp session for API requests.
        use_local (bool): Flag to load data from a local file.

    Returns:
        DataFrame: A DataFrame containing processed Aave reserve data.
    """
    if use_local:
        try:
            with open('./data/aave_data.json', 'r') as file:
                aave_data = json.load(file)
            logging.info("Aave data loaded from local file.")
            logging.debug(f"Raw local Aave data: {aave_data}")
            reserves_df = pd.DataFrame(aave_data.get("reserves", []))
        except FileNotFoundError:
            logging.error("Local Aave data file not found.")
            return pd.DataFrame()
    else:
        url = "https://aave-api-v2.aave.com/data/markets-data"
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                aave_data = await response.json()
                logging.info("Aave data fetched successfully from API.")
                #logging.debug(f"Raw API Aave data: {aave_data}")
                reserves_df = pd.DataFrame(aave_data.get("reserves", []))
        except aiohttp.ClientError as e:
            logging.error(f"Error fetching Aave data: {e}")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Unexpected error while fetching Aave data: {e}")
            return pd.DataFrame()

    try:
        # Convert relevant columns to numeric types
        numeric_columns = [
            "totalLiquidity", "totalLiquidityUSD", "totalBorrows",
            "totalBorrowsUSD", "liquidityRate", "variableBorrowRate",
            "stableBorrowRate"
        ]
        for column in numeric_columns:
            if column in reserves_df.columns:
                reserves_df[column] = pd.to_numeric(reserves_df[column], errors="coerce")

        # Add calculated 'availableLiquidity' column
        if not reserves_df.empty:
            reserves_df["availableLiquidity"] = reserves_df["totalLiquidity"] - reserves_df["totalBorrows"]

        # Add token names for clarity
        for index, reserve in reserves_df.iterrows():
            token_address = reserve.get("underlyingAsset")
            reserves_df.at[index, "token_name"] = (
                await get_token_name_ethereum(session, token_address)
                if token_address else "Unknown"
            )

        # Debugging the processed DataFrame
        logging.debug(f"Processed reserves with numeric conversion and token names:\n{reserves_df.head()}")

        return reserves_df

    except KeyError as e:
        logging.error(f"KeyError while processing Aave data: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Unexpected error while processing Aave data: {e}")
        return pd.DataFrame()
    
def get_best_tokens_for_flash_loans(aave_data):
    try:
        if 'reserves' not in aave_data:
            logging.error("Reserves not found in the Aave data")
            return None

        reserves_df = pd.DataFrame(aave_data['reserves'])

        best_token = None
        lowest_borrow_rate = float('inf')

        for index, market in reserves_df.iterrows():
            symbol = market['symbol']
            borrow_rate = float(market['variableBorrowRate'])
            available_liquidity = float(market['totalLiquidity'])
            borrowing_enabled = market['borrowingEnabled']
            is_active = market['isActive']

            logging.debug(f"Token: {symbol}, Borrow Rate: {borrow_rate}, Available Liquidity: {available_liquidity}, Borrowing Enabled: {borrowing_enabled}, Is Active: {is_active}")

            if borrowing_enabled and is_active and available_liquidity > 100000 and borrow_rate > 0:
                if borrow_rate < lowest_borrow_rate:
                    best_token = {
                        'symbol': symbol,
                        'borrow_rate': borrow_rate,
                        'available_liquidity': available_liquidity
                    }
                    lowest_borrow_rate = borrow_rate

        logging.info(f"Best token for flash loans: {best_token}")
        return best_token

    except Exception as e:
        logging.error(f"Error in get_best_borrow_token: {e}")
        return None
    
def save_best_tokens_to_csv(best_tokens, filename):
    try:
        df = pd.DataFrame(best_tokens)
        df.to_csv(filename, index=False)
        logging.info(f"Best tokens saved to {filename}.")
    except Exception as e:
        logging.error(f"Error saving best tokens to CSV: {e}")
    
def save_aave_data_to_csv(aave_data, file_path):
    # Ensure the data directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Extract column names from the Aave data
    columns = set()
    for chain_id, tokens in aave_data.items():
        if isinstance(tokens, dict):  # Check if tokens is a dictionary
            for token, data in tokens.items():
                columns.update(data.keys())
        elif isinstance(tokens, list):  # Check if tokens is a list
            for token_data in tokens:
                columns.update(token_data.keys())
    
    columns = list(columns)
    columns.sort()  # Optional: Sort the columns for readability
    
    # Ensure 'symbol' is the first column
    if 'symbol' in columns:
        columns.remove('symbol')
    columns = ['symbol'] + columns
    
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['chain_id', 'token'] + columns)
        writer.writeheader()
        
        for chain_id, tokens in aave_data.items():
            if isinstance(tokens, dict):
                for token, data in tokens.items():
                    row = {'chain_id': chain_id, 'token': token}
                    row.update(data)
                    writer.writerow(row)
            elif isinstance(tokens, list):
                for token_data in tokens:
                    row = {'chain_id': chain_id, 'token': token_data.get('token', 'unknown')}
                    row.update(token_data)
                    writer.writerow(row)
    
    print(f"Aave data saved to {file_path}")

# Function to get token name using Etherscan API for Ethereum
async def get_token_name_ethereum(session, token_address):
    url = f'https://api.etherscan.io/api?module=token&action=tokeninfo&contractaddress={token_address}&apikey={ETHERSCAN_API_KEY}'
    async with session.get(url) as response:
        response_json = await response.json()
        if response_json['status'] == '1' and 'result' in response_json:
            return response_json['result'][0]['tokenName']
    return 'Unknown'

# Function to get token name using Arbiscan API for Arbitrum
async def get_token_name_arbitrum(session, token_address):
    url = f'https://api.arbiscan.io/api?module=token&action=tokeninfo&contractaddress={token_address}&apikey={ARBISCAN_API_KEY}'
    async with session.get(url) as response:
        response_json = await response.json()
        if response_json['status'] == '1' and 'result' in response_json:
            return response_json['result'][0]['tokenName']
    return 'Unknown'

# Helper function to get token name based on chain_id
async def get_token_name(session, chain_id, token_address):
    if chain_id == 'ethereum':
        return await get_token_name_ethereum(session, token_address)
    elif chain_id == 'arbitrum':
        return await get_token_name_arbitrum(session, token_address)
    return 'Unknown'