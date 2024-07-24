import ccxt
import logging

logging.basicConfig(level=logging.INFO)

class OrderBook:
    def __init__(self, symbol):
        self.exchange = ccxt.binanceusdm()
        self.asks = dict()
        self.bids = dict()
        self.symbol = symbol

    def map_orderbook(self):
        data = self.exchange.fetch_order_book(self.symbol, limit = 1000)
        
        # Function to group prices into 20 USDT intervals
        def get_ask_bucket(price):
            return (price // 20) * 20
        
        def get_bid_bucket(price):
            return (price // 20) * 20

        ask_buckets = {}
        bid_buckets = {}

        # Group asks
        for ask in data['asks']:
            bucket = get_ask_bucket(ask[0])
            if bucket in ask_buckets:
                ask_buckets[bucket].append(ask)
            else:
                ask_buckets[bucket] = [ask]

        # Group bids
        for bid in data['bids']:
            bucket = get_bid_bucket(bid[0])
            if bucket in bid_buckets:
                bid_buckets[bucket].append(bid)
            else:
                bid_buckets[bucket] = [bid]

        # Process ask buckets
        for bucket, asks in ask_buckets.items():
            highest_price = max(ask[0] for ask in asks)
            total_quantity = sum(ask[1] for ask in asks)
            if highest_price in self.asks:
                self.asks[highest_price] = {
                    "amount": total_quantity,
                    "difference": total_quantity - self.asks[highest_price]["amount"]
                }
            else:
                self.asks[highest_price] = {
                    "amount": total_quantity,
                    "difference": 0
                }

        # Process bid buckets
        for bucket, bids in bid_buckets.items():
            lowest_price = min(bid[0] for bid in bids)
            total_quantity = sum(bid[1] for bid in bids)
            if lowest_price in self.bids:
                self.bids[lowest_price] = {
                    "amount": total_quantity,
                    "difference":  total_quantity - self.bids[lowest_price]["amount"]
                }
            else:
                self.bids[lowest_price] = {
                    "amount": total_quantity,
                    "difference": 0
                }

        logging.info('Orderbook mapped successfully')

    def log_orderbook(self):
        logging.info(f'Orderbook for {self.symbol}')
        logging.info('Bids:')
        for price, value in self.bids.items():
            logging.info(f'{price} -->> Amount: {value["amount"]} -->> Difference: {value["difference"]}')

        logging.info('Asks:')
        for price, value in self.asks.items():
            logging.info(f'{price} -->> Amount: {value["amount"]} -->> Difference: {value["difference"]}')
        


orderbook = OrderBook('BTC/USDT')
for i in range(10):
    orderbook.map_orderbook()
    orderbook.log_orderbook()
