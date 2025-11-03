import csv
import os
from logger import logger

class TradeLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        self._initialize_csv()

    def _initialize_csv(self):
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'symbol', 'action', 'price', 'quantity'])

    def log_trade(self, timestamp, symbol, action, price, quantity):
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, symbol, action, price, quantity])
        logger.info(f"Logged trade: {timestamp}, {symbol}, {action}, {price}, {quantity}")
