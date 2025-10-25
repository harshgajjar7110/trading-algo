from datetime import datetime, timedelta
import backtrader as bt
from brokers.zerodha import ZerodhaBroker
import pandas as pd
import yaml
import os

class SurvivorBacktest(bt.Strategy):
    params = (
        ('pe_gap', 20),
        ('ce_gap', 20),
        ('pe_reset_gap', 30),
        ('ce_reset_gap', 30),
    )

    def __init__(self):
        self.nifty = self.datas[0]
        self.nifty_pe_last_value = self.nifty.close[0]
        self.nifty_ce_last_value = self.nifty.close[0]
        self.pe_reset_gap_flag = 0
        self.ce_reset_gap_flag = 0
        self.pe_signals = 0
        self.ce_signals = 0

    def next(self):
        current_price = self.nifty.close[0]

        # PE trade logic
        price_diff_pe = round(current_price - self.nifty_pe_last_value, 0)
        if price_diff_pe > self.p.pe_gap:
            sell_multiplier = int(price_diff_pe / self.p.pe_gap)
            self.nifty_pe_last_value += self.p.pe_gap * sell_multiplier
            self.log(f'SELL PE Signal @ {self.datas[0].datetime.date(0)}: Price {current_price}')
            self.pe_signals += 1
            self.pe_reset_gap_flag = 1
            self.buy()

        # CE trade logic
        price_diff_ce = round(self.nifty_ce_last_value - current_price, 0)
        if price_diff_ce > self.p.ce_gap:
            sell_multiplier = int(price_diff_ce / self.p.ce_gap)
            self.nifty_ce_last_value -= self.p.ce_gap * sell_multiplier
            self.log(f'SELL CE Signal @ {self.datas[0].datetime.date(0)}: Price {current_price}')
            self.ce_signals += 1
            self.ce_reset_gap_flag = 1
            self.sell()

        # Reset logic
        if (self.nifty_pe_last_value - current_price) > self.p.pe_reset_gap and self.pe_reset_gap_flag:
            self.nifty_pe_last_value = current_price + self.p.pe_reset_gap
            self.close()

        if (current_price - self.nifty_ce_last_value) > self.p.ce_reset_gap and self.ce_reset_gap_flag:
            self.nifty_ce_last_value = current_price - self.p.ce_reset_gap
            self.close()

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

if __name__ == '__main__':
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(600000.0)

    config_file = os.path.join(os.path.dirname(__file__), "strategy/configs/survivor.yml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)['default']

    broker = ZerodhaBroker(without_totp=True)

    # Fetch historical data
    instrument_token = 256265 # NIFTY 50
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=40)

    historical_data = broker.historical_data(instrument_token, from_date, to_date, "minute")

    if historical_data:
        df = pd.DataFrame(historical_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')

        data = bt.feeds.PandasData(dataname=df)

        cerebro.adddata(data)
        cerebro.addstrategy(SurvivorBacktest,
                            pe_gap=config['pe_gap'],
                            ce_gap=config['ce_gap'],
                            pe_reset_gap=config['pe_reset_gap'],
                            ce_reset_gap=config['ce_reset_gap'])

        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')

        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

        results = cerebro.run()

        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

        trade_analyzer = results[0].analyzers.trade_analyzer.get_analysis()

        if trade_analyzer.total.total > 0:
            print("\n--- Trade Analysis ---")
            for i, (trade_id, trade) in enumerate(trade_analyzer.trades.items()):
                print(f"\nTrade {i+1}:")
                print(f"  - Status: {'Open' if trade.isopen else 'Closed'}")
                print(f"  - Entry Date: {trade.open_datetime}")
                if not trade.isopen:
                    print(f"  - Exit Date: {trade.close_datetime}")
                print(f"  - PnL: {trade.pnl:.2f}")

                initial_value = trade.price * trade.size
                if initial_value != 0:
                    roi = (trade.pnl / initial_value) * 100
                    print(f"  - ROI: {roi:.2f}%")
                else:
                    print("  - ROI: N/A (no initial value)")

        strat = results[0]
        print(f"\nPE Signals: {strat.pe_signals}")
        print(f"CE Signals: {strat.ce_signals}")

    else:
        print("Could not fetch historical data.")
