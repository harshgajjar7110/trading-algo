from datetime import datetime, timedelta
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from logger import logger
import yaml
import time

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
        self.oi_data = {}
        self.nifty_price_history = pd.DataFrame(columns=['price'])
        self.last_fetch_time = 0

    def on_ticks_update(self, ticks):
        """
        Main strategy execution method called on each tick update
        """
        now = datetime.now()
        nifty_price = ticks['last_price']
        self.nifty_price_history.loc[now] = [nifty_price]

        # Trim history to keep it manageable
        self.nifty_price_history = self.nifty_price_history.last('3H')

        self.nifty_price_changes = self.calculate_nifty_price_changes()

        if time.time() - self.last_fetch_time < 60:
            return

        self.last_fetch_time = time.time()

        atm_strike = round(nifty_price / 50) * 50
        strikes = [atm_strike + i * 50 for i in range(-2, 3)]

        red_cells = 0
        total_cells = 0

        for strike in strikes:
            for option_type in ['CE', 'PE']:
                instrument = self.nifty_options[(self.nifty_options['strike'] == strike) & (self.nifty_options['instrument_type'] == option_type)].iloc[0]
                instrument_token = instrument['instrument_token']

                from_date = now - timedelta(hours=3)
                to_date = now

                historical_data = self.broker.historical_data(instrument_token, from_date, to_date, "minute", oi=True)

                if historical_data:
                    df = pd.DataFrame(historical_data)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')

                    self.oi_data[instrument['tradingsymbol']] = self.calculate_oi_changes(df)
                    red_cells += self.count_red_cells(self.oi_data[instrument['tradingsymbol']])
                    total_cells += len(self.oi_data[instrument['tradingsymbol']])

        if total_cells > 0 and (red_cells / total_cells) > 0.3:
            self.play_alert_sound()

        self.log_tables()

    def calculate_oi_changes(self, df):
        """
        Calculates the percentage and absolute change in OI for different time intervals.
        """
        changes = {}
        now = df.index[-1]

        for interval in [3, 5, 10, 15, 30, 180]:
            past_time = now - timedelta(minutes=interval)
            past_oi = df['oi'].asof(past_time)
            current_oi = df['oi'].iloc[-1]

            if past_oi is not None and past_oi != 0:
                oi_change = current_oi - past_oi
                oi_change_pct = (oi_change / past_oi) * 100
                changes[f'{interval}m'] = (oi_change_pct, oi_change)
            else:
                changes[f'{interval}m'] = (0, 0)

        return changes

    def calculate_nifty_price_changes(self):
        """
        Calculates the percentage and absolute change in NIFTY price for different time intervals.
        """
        changes = {}
        now = self.nifty_price_history.index[-1]

        for interval in [3, 5, 10, 15, 30, 180]:
            past_time = now - timedelta(minutes=interval)
            past_price = self.nifty_price_history['price'].asof(past_time)
            current_price = self.nifty_price_history['price'].iloc[-1]

            if past_price is not None and past_price != 0:
                price_change = current_price - past_price
                price_change_pct = (price_change / past_price) * 100
                changes[f'{interval}m'] = (price_change_pct, price_change)
            else:
                changes[f'{interval}m'] = (0, 0)

        return changes

    def count_red_cells(self, oi_changes):
        """
        Counts the number of cells that should be colored red based on the given thresholds.
        """
        red_cells = 0
        thresholds = self.strat_var_color_thresholds

        for interval, (oi_change_pct, _) in oi_changes.items():
            if oi_change_pct > thresholds[interval]:
                red_cells += 1

        return red_cells

    def play_alert_sound(self):
        """
        Plays an alert sound.
        """
        logger.info("ALERT: More than 30% of the cells are color-coded red.")

    def log_tables(self):
        """
        Logs the OI and NIFTY data tables to the console.
        """
        logger.info("NIFTY Price Changes:")
        logger.info(pd.DataFrame(self.nifty_price_changes, index=['% Change', 'Abs Change']))

        logger.info("OI Changes:")
        df = pd.DataFrame(self.oi_data)
        logger.info(df)

if __name__ == "__main__":
    import time
    import argparse
    from dispatcher import DataDispatcher
    from orders import OrderTracker
    from brokers.zerodha import ZerodhaBroker
    from queue import Queue
    import random
    import traceback
    import warnings
    warnings.filterwarnings("ignore")

    import logging
    logger.setLevel(logging.INFO)

    config_file = os.path.join(os.path.dirname(__file__), "configs/oi_strategy.yml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)['default']

    parser = argparse.ArgumentParser(description="OI Strategy")
    parser.add_argument('--config-file', type=str, default=config_file, help='Path to YAML configuration file')
    args = parser.parse_args()

    if os.getenv("BROKER_TOTP_ENABLE") == "true":
        logger.info("Using TOTP login flow")
        broker = ZerodhaBroker(without_totp=False)
    else:
        logger.info("Using normal login flow")
        broker = ZerodhaBroker(without_totp=True)

    order_tracker = OrderTracker()

    try:
        quote_data = broker.get_quote("NSE:NIFTY 50")
        instrument_token = quote_data["NSE:NIFTY 50"]['instrument_token']
        logger.info(f"✓ Index instrument token obtained: {instrument_token}")
    except Exception as e:
        logger.error(f"Failed to get instrument token for NSE:NIFTY 50: {e}")
        sys.exit(1)

    dispatcher = DataDispatcher()
    dispatcher.register_main_queue(Queue())

    def on_ticks(ws, ticks):
        logger.debug("Received ticks: {}".format(ticks))
        dispatcher.dispatch(ticks)

    def on_connect(ws, response):
        logger.info("Websocket connected successfully: {}".format(response))
        ws.subscribe([instrument_token])
        logger.info(f"✓ Subscribed to instrument token: {instrument_token}")
        ws.set_mode(ws.MODE_FULL, [instrument_token])

    def on_order_update(ws, data):
        logger.info("Order update received: {}".format(data))

    broker.on_ticks = on_ticks
    broker.on_connect = on_connect
    broker.on_order_update = on_order_update

    broker.connect_websocket()

    strategy = OIStrategy(broker, config, order_tracker)

    try:
        while True:
            try:
                tick_data = dispatcher._main_queue.get()
                symbol_data = tick_data[0]
                strategy.on_ticks_update(symbol_data)
            except KeyboardInterrupt:
                logger.info("SHUTDOWN REQUESTED - Stopping strategy...")
                break
            except Exception as tick_error:
                logger.error(f"Error processing tick data: {tick_error}")
                logger.error("Continuing with next tick...")
                continue
    except Exception as fatal_error:
        logger.error("FATAL ERROR in main trading loop:")
        logger.error(f"Error: {fatal_error}")
        traceback.print_exc()
    finally:
        logger.info("STRATEGY SHUTDOWN COMPLETE")
