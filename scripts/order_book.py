import asyncio
import json
import requests
import websockets
from rich.console import Console
from rich.table import Table
from rich.live import Live

console = Console()

# Initialize storage for bids and asks
order_book_storage = {
    "bids": {},
    "asks": {}
}

async def fetch_order_book():
    uri = "wss://fstream.binance.com/stream?streams=btcusdt@depth"
    async with websockets.connect(uri) as websocket:
        while True:
            response = await websocket.recv()
            order_book_data = json.loads(response)
            process_order_book(order_book_data['data'])

def process_order_book(order_book_data):
    global order_book_storage

    def update_local_order_book(side, price, quantity):
        if quantity == 0:
            if price in order_book_storage[side]:
                del order_book_storage[side][price]
        else:
            order_book_storage[side][price] = quantity

    # Initial snapshot
    if 'lastUpdateId' not in order_book_storage:
        snapshot = get_order_book_snapshot()
        order_book_storage['lastUpdateId'] = snapshot['lastUpdateId']
        for bid in snapshot['bids']:
            order_book_storage['bids'][float(bid[0])] = float(bid[1])
        for ask in snapshot['asks']:
            order_book_storage['asks'][float(ask[0])] = float(ask[1])

    # Drop any event where u is < lastUpdateId in the snapshot
    if order_book_data['u'] < order_book_storage['lastUpdateId']:
        return

    # Ensure the first processed event has U <= lastUpdateId AND u >= lastUpdateId
    if order_book_data['U'] <= order_book_storage['lastUpdateId'] and order_book_data['u'] >= order_book_storage['lastUpdateId']:
        for bid in order_book_data['b']:
            update_local_order_book('bids', float(bid[0]), float(bid[1]))
        for ask in order_book_data['a']:
            update_local_order_book('asks', float(ask[0]), float(ask[1]))
        order_book_storage['lastUpdateId'] = order_book_data['u']
    elif order_book_data['pu'] == order_book_storage['lastUpdateId']:
        for bid in order_book_data['b']:
            update_local_order_book('bids', float(bid[0]), float(bid[1]))
        for ask in order_book_data['a']:
            update_local_order_book('asks', float(ask[0]), float(ask[1]))
        order_book_storage['lastUpdateId'] = order_book_data['u']

def get_order_book_snapshot():
    response = requests.get('https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000')
    return response.json()

def aggregate_order_book(storage, interval=10):
    aggregated_bids = {}
    aggregated_asks = {}

    for price, quantity in storage["bids"].items():
        grouped_price = int(price // interval * interval)
        if grouped_price in aggregated_bids:
            aggregated_bids[grouped_price] += quantity
        else:
            aggregated_bids[grouped_price] = quantity

    for price, quantity in storage["asks"].items():
        grouped_price = int(price // interval * interval)
        if grouped_price in aggregated_asks:
            aggregated_asks[grouped_price] += quantity
        else:
            aggregated_asks[grouped_price] = quantity

    return aggregated_bids, aggregated_asks

def create_order_book_table():
    global order_book_storage
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Bids", style="blue")
    table.add_column("Asks", style="red")
    table.add_column("Imbalance", style="yellow")

    aggregated_bids, aggregated_asks = aggregate_order_book(order_book_storage)

    sorted_bids = sorted(aggregated_bids.items(), key=lambda x: x[0], reverse=True)
    sorted_asks = sorted(aggregated_asks.items(), key=lambda x: x[0])

    max_rows = max(len(sorted_bids), len(sorted_asks))

    imbalance_threshold = 50  # Set a threshold for significant imbalance

    for i in range(max_rows):
        bid_str = f"{sorted_bids[i][0]:.2f} | {sorted_bids[i][1]:.4f}" if i < len(sorted_bids) else ""
        ask_str = f"{sorted_asks[i][0]:.2f} | {sorted_asks[i][1]:.4f}" if i < len(sorted_asks) else ""
        imbalance = sorted_bids[i][1] - sorted_asks[i][1] if i < len(sorted_bids) and i < len(sorted_asks) else 0
        imbalance_str = f"{imbalance:.4f}" if imbalance != 0 else ""
        bid = sorted_bids[i][1] if i < len(sorted_bids) else 0
        ask = sorted_asks[i][1] if i < len(sorted_asks) else 0
        if abs(imbalance) > imbalance_threshold or bid > 50 or ask > 50:
            table.add_row(bid_str, ask_str, imbalance_str, style="bold bright_yellow")
        else:
            table.add_row(bid_str, ask_str, imbalance_str)
    
    return table

async def main():
    async with websockets.connect("wss://fstream.binance.com/stream?streams=btcusdt@depth") as websocket:
        with Live(console=console, refresh_per_second=1) as live:
            while True:
                response = await websocket.recv()
                order_book_data = json.loads(response)
                process_order_book(order_book_data['data'])
                table = create_order_book_table()
                live.update(table)

if __name__ == "__main__":
    asyncio.run(main())