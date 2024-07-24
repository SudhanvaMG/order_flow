"""
Connect to Binance websocket to get live order book update data and save it as a text file.
Make a REST call to Binance API to get the order book snapshot and save it.
"""

import asyncio
from websockets import connect
import aiofiles
import json
import httpx
import datetime as dt

async def update_order_book(pair: str) -> None:
    """
    Fetches the order book snapshot and live updates for a given trading pair from Binance.

    Args:
        pair (str): The trading pair (e.g., 'BTC/USDT').
    """

    # Format the pair and create URIs
    formatted_pair = pair.replace("/", "").lower()
    websocket_uri = f"wss://fstream.binance.com/stream?streams={formatted_pair}@depth"
    snapshot_uri = f"https://fapi.binance.com/fapi/v1/depth?symbol={formatted_pair}&limit=1000"
    today = dt.datetime.now().date()

    # Fetch and save the order book snapshot
    async with httpx.AsyncClient() as client:
        response = await client.get(snapshot_uri)
        data = response.json()
        async with aiofiles.open(f"{formatted_pair}_order_book_snapshot_{today}.txt", mode="w") as f:
            await f.write(json.dumps(data, indent=4))

    # Connect to the WebSocket for live updates
    async with connect(websocket_uri) as websocket:
        async for message in websocket:
            async with aiofiles.open(f"{formatted_pair}_order_book_updates_{today}.txt", mode="a") as f:
                await f.write(message + "\n")

if __name__ == "__main__":
    asyncio.run(update_order_book("BTC/USDT"))