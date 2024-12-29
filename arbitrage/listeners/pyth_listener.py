import aiohttp
import asyncio
import logging

# Logger configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PYTH_API_URL = "https://pyth.network/v2/updates/price/stream"

async def handle_price_update(data):
    # Process incoming price updates
    logger.info(f"Price Update: {data}")

async def listen_to_pyth():
    async with aiohttp.ClientSession() as session:
        async with session.get(PYTH_API_URL) as response:
            logger.info("Listening to Pyth price updates...")
            async for line in response.content:
                try:
                    data = line.decode("utf-8")
                    await handle_price_update(data)
                except Exception as e:
                    logger.error(f"Error processing Pyth update: {e}")

if __name__ == "__main__":
    asyncio.run(listen_to_pyth())
