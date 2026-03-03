"""
Volume Profile Based Option Selling Backtest - Nifty 50
======================================================

This script provides a framework for backtesting volume profile-based
option selling strategies on Nifty 50 options.

Author: Your Name
Date: February 2025
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    'symbol': 'NIFTY',
    'lot_size': 25,
    'capital': 100000,
    'risk_per_trade': 0.02,  # 2%
    'transaction_cost': 50,   # per trade
    'slippage': 0.0025,       # 0.25%
    'start_date': '2022-01-01',
    'end_date': '2024-12-31'
}

# ============================================================================
# VOLUME PROFILE CALCULATION
# ============================================================================

def calculate_volume_profile(df, lookback_period=24, value_area_pct=0.70):
    """
    Calculate Volume Profile metrics (POC, VAH, VAL)

    Parameters:
    -----------
    df : DataFrame with 'close', 'high', 'low', 'volume' columns
    lookback_period : int - Number of periods to look back
    value_area_pct : float - Percentage for value area (default 70%)

    Returns:
    --------
    dict with POC, VAH, VAL, HVN_levels
    """
    # Create price bins
    price_min = df['low'].min()
    price_max = df['high'].max()
    num_bins = 50
    bin_size = (price_max - price_min) / num_bins

    # Initialize volume array for each bin
    volume_by_price = np.zeros(num_bins)

    # Distribute volume across price bins
    for idx, row in df.iterrows():
        low_bin = int((row['low'] - price_min) / bin_size)
        high_bin = int((row['high'] - price_min) / bin_size)

        low_bin = max(0, min(low_bin, num_bins - 1))
        high_bin = max(0, min(high_bin, num_bins - 1))

        # Distribute volume equally across bins
        bins_covered = high_bin - low_bin + 1
        volume_per_bin = row['volume'] / bins_covered

        for b in range(low_bin, high_bin + 1):
            volume_by_price[b] += volume_per_bin

    # Find POC (Point of Control)
    poc_bin = np.argmax(volume_by_price)
    poc = price_min + (poc_bin + 0.5) * bin_size

    # Calculate Value Area
    total_volume = np.sum(volume_by_price)
    target_volume = total_volume * value_area_pct

    # Expand from POC outward until we reach target volume
    current_volume = volume_by_price[poc_bin]
    vah_bin = poc_bin
    val_bin = poc_bin

    while current_volume < target_volume:
        # Check volume above and below
        vol_above = volume_by_price[vah_bin + 1] if vah_bin < num_bins - 1 else 0
        vol_below = volume_by_price[val_bin - 1] if val_bin > 0 else 0

        if vol_above >= vol_below and vah_bin < num_bins - 1:
            vah_bin += 1
            current_volume += vol_above
        elif val_bin > 0:
            val_bin -= 1
            current_volume += vol_below
        else:
            break

    vah = price_min + (vah_bin + 0.5) * bin_size
    val = price_min + (val_bin + 0.5) * bin_size

    # Find High Volume Nodes (top 20% of bins)
    volume_threshold = np.percentile(volume_by_price, 80)
    hvn_bins = np.where(volume_by_price >= volume_threshold)[0]
    hvn_levels = [price_min + (b + 0.5) * bin_size for b in hvn_bins]

    return {
        'poc': poc,
        'vah': vah,
        'val': val,
        'hvn_levels': hvn_levels,
        'volume_by_price': volume_by_price
    }


# ============================================================================
# STRIKE SELECTION METHODS
# ============================================================================

def select_strike_poc_based(spot, poc, dte, option_type='CE', buffer_override=None):
    """
    Select strike based on POC with DTE-adjusted buffer
    """
    if buffer_override:
        buffer = buffer_override
    else:
        if dte <= 2:
            buffer = 0.015
        elif dte <= 7:
            buffer = 0.020
        else:
            buffer = 0.025

    if option_type == 'CE':
        strike = poc + (spot * buffer)
    else:
        strike = poc - (spot * buffer)

    # Round to nearest 50 for Nifty
    strike = round(strike / 50) * 50
    return strike


def select_strike_vah_val_based(spot, vah, val, option_type='CE', buffer=0.007):
    """
    Select strike based on VAH/VAL with buffer
    """
    if option_type == 'CE':
        strike = vah + (spot * buffer)
    else:
        strike = val - (spot * buffer)

    strike = round(strike / 50) * 50
    return strike


def select_strike_hvn_confluence(spot, hvn_levels, fib_level, option_type='CE', buffer=0.01):
    """
    Select strike based on HVN and Fibonacci confluence
    """
    # Find HVN closest to Fib level
    closest_hvn = min(hvn_levels, key=lambda x: abs(x - fib_level))

    if option_type == 'CE':
        strike = closest_hvn + (spot * buffer)
    else:
        strike = closest_hvn - (spot * buffer)

    strike = round(strike / 50) * 50
    return strike


# ============================================================================
# BACKTEST ENGINE
# ============================================================================

class VolumeProfileBacktest:
    def __init__(self, config):
        self.config = config
        self.trades = []
        self.equity_curve = []

    def load_data(self, spot_data_path, option_data_path):
        """
        Load spot and option data
        Expected columns:
        - spot: date, open, high, low, close, volume
        - options: date, strike, option_type, open, high, low, close, volume, oi
        """
        self.spot_data = pd.read_csv(spot_data_path, parse_dates=['date'])
        self.option_data = pd.read_csv(option_data_path, parse_dates=['date'])

        self.spot_data.set_index('date', inplace=True)
        self.option_data.set_index('date', inplace=True)

    def get_option_premium(self, date, strike, option_type, expiry):
        """
        Get option premium for given parameters
        """
        try:
            option_row = self.option_data[
                (self.option_data.index == date) &
                (self.option_data['strike'] == strike) &
                (self.option_data['option_type'] == option_type) &
                (self.option_data['expiry'] == expiry)
            ]

            if len(option_row) > 0:
                return option_row['close'].iloc[0]
            else:
                return None
        except:
            return None

    def check_entry_conditions(self, date, vp, spot_price):
        """
        Check if entry conditions are met
        """
        # Time filter (10:00 AM - 2:30 PM)
        if date.hour < 10 or (date.hour == 14 and date.minute > 30) or date.hour > 14:
            return False

        # Check if we have valid volume profile levels
        if vp['poc'] is None or vp['vah'] is None or vp['val'] is None:
            return False

        return True

    def run_backtest(self, approach='POC_BASED', start_date=None, end_date=None):
        """
        Run backtest for specified approach

        Approaches:
        - 'POC_BASED': POC-based strike selection
        - 'VAH_VAL_BASED': VAH/VAL-based strike selection
        - 'HVN_CONFLUENCE': HVN + Fibonacci confluence
        """
        if start_date:
            mask = self.spot_data.index >= start_date
            self.spot_data = self.spot_data[mask]

        if end_date:
            mask = self.spot_data.index <= end_date
            self.spot_data = self.spot_data[mask]

        current_capital = self.config['capital']

        for i in range(24, len(self.spot_data) - 1):
            current_date = self.spot_data.index[i]

            # Calculate Volume Profile from last 24 periods
            lookback_data = self.spot_data.iloc[i-24:i]
            vp = calculate_volume_profile(lookback_data)

            spot_price = self.spot_data['close'].iloc[i]

            # Check entry conditions
            if not self.check_entry_conditions(current_date, vp, spot_price):
                continue

            # Get next expiry (simplified - assume weekly expiry)
            days_to_expiry = 5 - current_date.weekday()
            if days_to_expiry <= 0:
                days_to_expiry += 7
            expiry_date = current_date + timedelta(days=days_to_expiry)

            # Select strikes based on approach
            if approach == 'POC_BASED':
                call_strike = select_strike_poc_based(spot_price, vp['poc'], days_to_expiry, 'CE')
                put_strike = select_strike_poc_based(spot_price, vp['poc'], days_to_expiry, 'PE')

            elif approach == 'VAH_VAL_BASED':
                call_strike = select_strike_vah_val_based(spot_price, vp['vah'], vp['val'], 'CE')
                put_strike = select_strike_vah_val_based(spot_price, vp['vah'], vp['val'], 'PE')

            elif approach == 'HVN_CONFLUENCE':
                # Calculate Fibonacci level (example: 61.8% retracement)
                swing_high = lookback_data['high'].max()
                swing_low = lookback_data['low'].min()
                fib_618 = swing_high - (swing_high - swing_low) * 0.618

                call_strike = select_strike_hvn_confluence(spot_price, vp['hvn_levels'], fib_618, 'CE')
                put_strike = select_strike_hvn_confluence(spot_price, vp['hvn_levels'], fib_618, 'PE')

            else:
                continue

            # Get option premiums
            call_premium = self.get_option_premium(current_date, call_strike, 'CE', expiry_date)
            put_premium = self.get_option_premium(current_date, put_strike, 'PE', expiry_date)

            if call_premium is None or put_premium is None:
                continue

            # Execute trade
            total_premium = call_premium + put_premium

            # Calculate position size
            risk_amount = current_capital * self.config['risk_per_trade']
            stop_loss_points = total_premium * 2  # 200% of premium
            position_size = int(risk_amount / (stop_loss_points * self.config['lot_size']))

            if position_size < 1:
                position_size = 1

            # Simulate trade outcome
            # (Simplified - you would track until exit in real implementation)
            trade_result = self.simulate_trade(
                current_date, expiry_date, 
                call_strike, put_strike,
                call_premium, put_premium,
                position_size, spot_price, vp
            )

            self.trades.append(trade_result)
            current_capital += trade_result['pnl']
            self.equity_curve.append({
                'date': current_date,
                'equity': current_capital
            })

        return self.calculate_metrics()

    def simulate_trade(self, entry_date, expiry_date, call_strike, put_strike,
                       call_premium, put_premium, position_size, entry_spot, vp):
        """
        Simulate trade outcome
        (Simplified - assumes holding to expiry or stop loss)
        """
        # This is a placeholder - implement actual trade simulation
        # based on historical option data

        total_premium = call_premium + put_premium

        # Simplified PnL calculation
        # In reality, you would track daily MTM until exit
        pnl_per_lot = total_premium * 0.6  # Assume 60% capture rate

        total_pnl = pnl_per_lot * position_size * self.config['lot_size']
        total_pnl -= self.config['transaction_cost'] * 2  # Entry + Exit

        return {
            'entry_date': entry_date,
            'expiry_date': expiry_date,
            'call_strike': call_strike,
            'put_strike': put_strike,
            'call_premium': call_premium,
            'put_premium': put_premium,
            'position_size': position_size,
            'entry_spot': entry_spot,
            'poc': vp['poc'],
            'vah': vp['vah'],
            'val': vp['val'],
            'pnl': total_pnl,
            'status': 'Win' if total_pnl > 0 else 'Loss'
        }

    def calculate_metrics(self):
        """
        Calculate performance metrics
        """
        if not self.trades:
            return {}

        df_trades = pd.DataFrame(self.trades)

        total_trades = len(df_trades)
        winning_trades = len(df_trades[df_trades['pnl'] > 0])
        losing_trades = len(df_trades[df_trades['pnl'] <= 0])

        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0

        gross_profit = df_trades[df_trades['pnl'] > 0]['pnl'].sum()
        gross_loss = df_trades[df_trades['pnl'] <= 0]['pnl'].sum()

        profit_factor = abs(gross_profit / gross_loss) if gross_loss != 0 else float('inf')

        avg_win = gross_profit / winning_trades if winning_trades > 0 else 0
        avg_loss = abs(gross_loss) / losing_trades if losing_trades > 0 else 0

        win_loss_ratio = avg_win / avg_loss if avg_loss != 0 else 0

        # Calculate max drawdown
        equity_df = pd.DataFrame(self.equity_curve)
        if len(equity_df) > 0:
            equity_df['peak'] = equity_df['equity'].cummax()
            equity_df['drawdown'] = (equity_df['equity'] - equity_df['peak']) / equity_df['peak'] * 100
            max_drawdown = equity_df['drawdown'].min()
        else:
            max_drawdown = 0

        return {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'gross_profit': gross_profit,
            'gross_loss': gross_loss,
            'net_pnl': gross_profit + gross_loss,
            'profit_factor': profit_factor,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'win_loss_ratio': win_loss_ratio,
            'max_drawdown': max_drawdown
        }

    def plot_equity_curve(self):
        """
        Plot equity curve
        """
        if not self.equity_curve:
            print("No equity curve data to plot")
            return

        equity_df = pd.DataFrame(self.equity_curve)

        plt.figure(figsize=(12, 6))
        plt.plot(equity_df['date'], equity_df['equity'])
        plt.title('Equity Curve')
        plt.xlabel('Date')
        plt.ylabel('Capital (₹)')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig('/mnt/okcomputer/output/equity_curve.png')
        plt.show()

    def export_results(self, filename='backtest_results.csv'):
        """
        Export trades to CSV
        """
        if not self.trades:
            print("No trades to export")
            return

        df_trades = pd.DataFrame(self.trades)
        df_trades.to_csv(f'/mnt/okcomputer/output/{filename}', index=False)
        print(f"Results exported to {filename}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    # Initialize backtest
    backtest = VolumeProfileBacktest(CONFIG)

    # Load data (update paths to your data files)
    # backtest.load_data('nifty_spot.csv', 'nifty_options.csv')

    # Run backtest for different approaches
    approaches = ['POC_BASED', 'VAH_VAL_BASED', 'HVN_CONFLUENCE']

    results = {}
    for approach in approaches:
        print(f"\nRunning backtest for: {approach}")
        # metrics = backtest.run_backtest(approach=approach)
        # results[approach] = metrics
        # print(f"Results: {metrics}")

    # Compare approaches
    # comparison_df = pd.DataFrame(results).T
    # print("\nApproach Comparison:")
    # print(comparison_df)

    # Plot equity curve
    # backtest.plot_equity_curve()

    # Export results
    # backtest.export_results()
