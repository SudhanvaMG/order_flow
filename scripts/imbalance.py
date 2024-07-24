import ccxt
import pandas as pd

# Initialize the Binance USDM exchange
exchange = ccxt.binanceusdm()

def fetch_orderbook_imbalance(symbol: str, limit: int = 1000, imbalance_threshold: float = 0.01) -> float:
    """
    Fetch the order book for a given symbol and calculate the order book imbalance.

    Args:
        symbol (str): The trading pair symbol (e.g., 'SOL/USDT').
        limit (int, optional): The number of order book levels to fetch. Default is 1000.
        imbalance_threshold (float, optional): The percentage threshold for calculating the imbalance. Default is 0.01 (1%).

    Returns:
        float: The calculated order book imbalance.
    """

    # Fetch the order book data from the exchange
    orderbook = exchange.fetch_order_book(symbol, limit=limit)
    print(f"Length of bids: {len(orderbook['bids'])} and asks: {len(orderbook['asks'])}")

    # Convert order book data to DataFrames
    asks = pd.DataFrame(orderbook['asks'], columns=['price', 'quantity'], dtype=float)
    bids = pd.DataFrame(orderbook['bids'], columns=['price', 'quantity'], dtype=float)

    # Add side labels to the data
    bids["side"] = "bid"
    asks["side"] = "ask"

    # Concatenate and sort the data
    orderbook_df = pd.concat([bids, asks]).sort_values("price", ascending=False).to_string()

    # Calculate mid price and thresholds
    mid_price = (max(bids.price) + min(asks.price)) / 2
    ask_threshold = mid_price * (1 + imbalance_threshold)
    bid_threshold = mid_price * (1 - imbalance_threshold)

    print(f"Ask threshold: {ask_threshold}")
    print(f"Mid price: {mid_price}")
    print(f"Bid threshold: {bid_threshold}")

    print(f"Max ask price: {max(asks.price)}")
    print(f"Min bid price: {min(bids.price)}")

    # Calculate ask and bid volumes within the thresholds
    ask_volume = asks.quantity[asks.price < ask_threshold].sum()
    bid_volume = bids.quantity[bids.price > bid_threshold].sum()

    print(f"Ask volume: {ask_volume}")
    print(f"Bid volume: {bid_volume}")

    imbalance = bid_volume - ask_volume

    return imbalance

if __name__ == "__main__":
    # Fetch and print the order book imbalance for a given symbol
    imbalance = fetch_orderbook_imbalance("SOL/USDT")
    print(f"Order Book Imbalance for SOL/USDT: {imbalance}")