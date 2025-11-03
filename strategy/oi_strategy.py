from datetime import datetime, timedelta
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from logger import logger
import yaml
import time
from trade_logger import TradeLogger

class OIStrategy:
    """
    Open Interest (OI) based Options Trading Strategy
    """

    def __init__(self, broker, config, order_manager):
        # Assign config values as instance variables with 'strat_var_' prefix
        for k, v in config.items():
            setattr(self, f'strat_var_{k}', v)
        # External dependencies
        self.broker = broker
        self.order_manager = order_manager
        self.broker.download_instruments()
        self.instruments = self.broker.instruments_df
        self.nifty_options = self.instruments[self.instruments['name'] == 'NIFTY']
        self.nifty_price_history = pd.DataFrame(columns=['price'])
        if self.strat_var_paper_trade:
            self.trade_logger = TradeLogger(self.strat_var_trade_log_file)


    def on_ticks_update(self, ticks):
        """
        Main strategy execution method called on each tick update
        """
        now = datetime.now()
        nifty_price = ticks['last_price']
        self.nifty_price_history.loc[now] = [nifty_price]

        # Trim history to keep it manageable
        self.nifty_price_history = self.nifty_price_history.last('5T') # 5 minutes of data

        if len(self.nifty_price_history) < 2:
            return

        price_change = nifty_price - self.nifty_price_history['price'].iloc[-2]

        if abs(price_change) > self.strat_var_gamma_threshold:
            option_type = 'CE' if price_change > 0 else 'PE'
            instrument = self._find_nearest_expiry_instrument(nifty_price, option_type)
            if instrument:
                self._place_order(instrument['tradingsymbol'], self.strat_var_quantity, 'BUY', nifty_price)

    def _find_nearest_expiry_instrument(self, ltp, option_type):
        """
        Finds the nearest expiry instrument for a given option type and LTP.
        """
        # ATM Strike
        atm_strike = round(ltp / 50) * 50

        # Filter for the option type
        options = self.nifty_options[self.nifty_options['instrument_type'] == option_type]
        options = options[options['strike'] == atm_strike]

        # Find the one with the closest expiry
        options['expiry'] = pd.to_datetime(options['expiry'])
        options = options[options['expiry'] > datetime.now()]
        options = options.sort_values(by='expiry')

        if not options.empty:
            return options.iloc[0].to_dict()
        return None


    def _place_order(self, symbol, quantity, transaction_type, price):
        """
        Places an order with the broker or logs it for paper trading.
        """
        if self.strat_var_paper_trade:
            self.trade_logger.log_trade(datetime.now(), symbol, transaction_type, price, quantity)
        else:
            # Implement actual order placement logic here
            logger.info(f"LIVE TRADE: {transaction_type} {quantity} of {symbol}")
            # self.broker.place_order(...)

# if __name__ == "__main__":
#     import time
#     import argparse
#     from dispatcher import DataDispatcher
#     from orders import OrderTracker
#     from brokers.zerodha import ZerodhaBroker
#     from queue import Queue
#     import random
#     import traceback
#     import warnings
#     warnings.filterwarnings("ignore")

#     import logging
#     logger.setLevel(logging.INFO)

#     config_file = os.path.join(os.path.dirname(__file__), "configs/oi_strategy.yml")
#     with open(config_file, 'r') as f:
#         config = yaml.safe_load(f)['default']

#     parser = argparse.ArgumentParser(description="OI Strategy")
#     parser.add_argument('--config-file', type=str, default=config_file, help='Path to YAML configuration file')
#     args = parser.parse_args()

#     if os.getenv("BROKER_TOTP_ENABLE") == "true":
#         logger.info("Using TOTP login flow")
#         broker = ZerodhaBroker(without_totp=False)
#     else:
#         logger.info("Using normal login flow")
#         broker = ZerodhaBroker(without_totp=True)

#     order_tracker = OrderTracker()

#     try:
#         quote_data = broker.get_quote("NSE:NIFTY 50")
#         instrument_token = quote_data["NSE:NIFTY 50"]['instrument_token']
#         logger.info(f"✓ Index instrument token obtained: {instrument_token}")
#     except Exception as e:
#         logger.error(f"Failed to get instrument token for NSE:NIFTY 50: {e}")
#         sys.exit(1)

#     dispatcher = DataDispatcher()
#     dispatcher.register_main_queue(Queue())

#     def on_ticks(ws, ticks):
#         logger.debug("Received ticks: {}".format(ticks))
#         dispatcher.dispatch(ticks)

#     def on_connect(ws, response):
#         logger.info("Websocket connected successfully: {}".format(response))
#         ws.subscribe([instrument_token])
#         logger.info(f"✓ Subscribed to instrument token: {instrument_token}")
#         ws.set_mode(ws.MODE_FULL, [instrument_token])

#     def on_order_update(ws, data):
#         logger.info("Order update received: {}".format(data))

#     broker.on_ticks = on_ticks
#     broker.on_connect = on_connect
#     broker.on_order_update = on_order_update

#     broker.connect_websocket()

#     strategy = OIStrategy(broker, config, order_tracker)

#     try:
#         while True:
#             try:
#                 tick_data = dispatcher._main_queue.get()
#                 symbol_data = tick_data[0]
#                 strategy.on_ticks_update(symbol_data)
#             except KeyboardInterrupt:
#                 logger.info("SHUTDOWN REQUESTED - Stopping strategy...")
#                 break
#             except Exception as tick_error:
#                 logger.error(f"Error processing tick data: {tick_error}")
#                 logger.error("Continuing with next tick...")
#                 continue
#     except Exception as fatal_error:
#         logger.error("FATAL ERROR in main trading loop:")
#         logger.error(f"Error: {fatal_error}")
#         traceback.print_exc()
#     finally:
#         logger.info("STRATEGY SHUTDOWN COMPLETE")
