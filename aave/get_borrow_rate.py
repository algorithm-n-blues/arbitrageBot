import logging
logger = logging.getLogger(__name__)

def get_borrow_rate(asset_symbol, chain, aave_data):
    # List of equivalent symbols for ETH and WETH
    equivalent_symbols = {
        "WETH": ["WETH", "ETH", "weETH", "rETH"],
        "ETH": ["ETH", "WETH", "weETH", "rETH"]
    }

    # Get the list of symbols to check, defaulting to the asset_symbol itself
    symbols_to_check = equivalent_symbols.get(asset_symbol, [asset_symbol])
    
    try:
        chain_data = aave_data[chain]
        for symbol in symbols_to_check:
            if symbol in chain_data:
                return float(chain_data[symbol]["Borrow APY, Variable (%)"]), symbol
    except KeyError as e:
        logger.error(f"Key error: {e}")
    except ValueError as e:
        logger.error(f"Value error: {e}")
    
    logger.error(f"Borrow rate not found for asset: {asset_symbol} or its equivalents on chain: {chain}")
    return None, None