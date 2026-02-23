"""
Enhanced Survivor Strategy - Advanced NIFTY Options Selling

Enhancements over base strategy:
1. Technical indicator filters (RSI, EMA, ADX)
2. Dynamic gap adjustment based on ATR
3. Position limits and risk management
4. Stop-loss and time-based exits
5. Volatility-adjusted position sizing
6. ✅ Automatic broker-level SL order placement (NEW)
   - Places SL orders immediately after fresh orders
   - Configurable SL percentage (default 60%)
   - Supports STOP_LIMIT and STOP order types
   - Tracks SL order IDs per position
   - Cancels SL orders on manual exit
7. ✅ SL Order Reconciliation at Startup (NEW)
   - Detects open positions from broker at algo startup
   - Places SL orders for unprotected positions automatically
   - Persists position-SL mapping across restarts
   - Zerodha-only implementation
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
import pandas as pd
import numpy as np
from datetime import datetime, time as dt_time, timedelta
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum

from logger import strategy_logger as logger
from brokers import BrokerGateway, OrderRequest, Exchange, OrderType, TransactionType, ProductType
from strategy.survivor import SurvivorStrategy
from strategy.position_manager import PositionManager, SLReconciliationResult


class TrendBias(Enum):
    """Market trend classification"""
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class VolatilityRegime(Enum):
    """Volatility classification based on ATR/VIX"""
    LOW = "low"        # ATR < 50% of average
    NORMAL = "normal"  # ATR 50-150% of average
    HIGH = "high"      # ATR > 150% of average


@dataclass
class TechnicalIndicators:
    """Container for technical indicator values"""
    rsi: float = 50.0
    ema: float = 0.0
    adx: float = 0.0
    atr: float = 0.0
    trend: TrendBias = TrendBias.NEUTRAL
    volatility_regime: VolatilityRegime = VolatilityRegime.NORMAL


@dataclass
class PositionInfo:
    """Track individual position details"""
    symbol: str
    entry_price: float
    entry_time: datetime
    quantity: int
    option_type: str
    current_pnl: float = 0.0
    highest_pnl: float = 0.0  # For trailing stop
    sl_order_id: Optional[str] = None  # Track SL order ID
    sl_price: float = 0.0  # SL trigger price
    sl_limit_price: float = 0.0  # SL limit price (for STOP_LIMIT orders)
    profit_target_price: float = 0.0  # Target price for profit taking (60% of premium)


class EnhancedSurvivorStrategy(SurvivorStrategy):
    """
    Enhanced Survivor Strategy with advanced risk management and filters.
    
    NEW FEATURES:
    =============
    1. Technical Filters:
       - RSI filter: Avoid selling into extremes
       - EMA filter: Trade with the trend
       - ADX filter: Only trade strong trends
       
    2. Dynamic Gap Adjustment:
       - Adjusts pe_gap/ce_gap based on ATR
       - Wider gaps in high volatility
       
    3. Position Management:
       - Max positions per side
       - Consecutive loss tracking
       - Position sizing based on volatility
       
    4. Risk Management:
       - ✅ Automatic broker-level SL orders (NEW)
       - Configurable SL percentage per position
       - SL order tracking and correlation
       - Trailing stop-loss option
       - Max daily loss limit
       - Time-based square off
       
    5. Enhanced Logging:
       - Trade journaling
       - Performance metrics
       - Filter rejection tracking
       
    6. SL Order Management:
       - Places SL orders immediately after fresh order execution
       - Configurable SL percentage (default: 60% of entry price)
       - Supports STOP (SL-M) and STOP_LIMIT (SL) order types
       - Tracks SL order IDs linked to positions
       - Automatically cancels SL orders on manual position exit
       - Monitors broker SL execution status
    """
    
    def __init__(self, broker, config, order_tracker):
        # Log ENHANCED strategy initialization BEFORE calling parent
        logger.info("=" * 70)
        logger.info("[ENHANCED STRATEGY] Initializing EnhancedSurvivorStrategy v2.0")
        logger.info("=" * 70)
        logger.info(f"[ENHANCED] Config: sl_enabled={config.get('sl_enabled', True)}, "
                   f"sl_reconcile_on_start={config.get('sl_reconcile_on_start', True)}, "
                   f"sl_percentage={config.get('sl_percentage', 60)}%")
        
        super().__init__(broker, config, order_tracker)
        
        # Technical indicator settings
        self.rsi_period = config.get('rsi_period', 14)
        self.rsi_min = config.get('rsi_min', 30)  # Don't sell CE below this
        self.rsi_max = config.get('rsi_max', 70)  # Don't sell PE above this
        self.ema_period = config.get('ema_period', 20)
        self.adx_period = config.get('adx_period', 14)
        self.adx_threshold = config.get('adx_threshold', 25)
        self.atr_period = config.get('atr_period', 14)
        
        # Entry filter mode
        self.entry_filter_type = config.get('entry_filter_type', 'NONE')  # NONE, RSI, EMA, ADX, ALL
        
        # Position limits
        self.max_positions_per_side = config.get('max_positions_per_side', 3)
        self.max_total_positions = config.get('max_total_positions', 5)
        self.max_consecutive_losses = config.get('max_consecutive_losses', 2)
        
        # Risk management
        self.sl_enabled = config.get('sl_enabled', False)  # Enable automatic SL order placement
        self.sl_percentage = config.get('sl_percentage', 60)  # SL at 60% of entry price
        self.sl_order_type_str = config.get('sl_order_type', 'STOP_LIMIT')  # STOP or STOP_LIMIT
        self.sl_limit_buffer = config.get('sl_limit_buffer', 5.0)  # Points buffer for limit price
        self.stop_loss_multiplier = config.get('stop_loss_multiplier', 2.0)  # Legacy: Exit if premium doubles
        self.trailing_stop_enabled = config.get('trailing_stop_enabled', False)
        self.trailing_stop_distance = config.get('trailing_stop_distance', 0.5)  # 50% of max profit
        self.max_daily_loss_percent = config.get('max_daily_loss_percent', -3.0)
        self.square_off_time = config.get('square_off_time', '15:15')
        # print config value
        # Profit target settings
        self.profit_target_enabled = config.get('profit_target_enabled', True)  # Enable profit target
        self.profit_target_percent = config.get('profit_target_percent', 60)  # Exit when 60% premium collected as profit
        self.profit_check_interval = config.get('profit_check_interval', 20)  # Check profit every 5 minutes (300 seconds)
        self.last_profit_check_time = datetime.now()  # Track last profit check time
        
        print(',,,,==>',config)
        
        # Dynamic gap adjustment
        self.enable_dynamic_gaps = config.get('enable_dynamic_gaps', False)
        self.atr_multiplier_pe = config.get('atr_multiplier_pe', 2.0)
        self.atr_multiplier_ce = config.get('atr_multiplier_ce', 2.0)
        self.atr_history_days = config.get('atr_history_days', 5)
        
        # Position sizing
        self.volatility_sizing = config.get('volatility_sizing', False)
        self.high_vol_size_reduction = config.get('high_vol_size_reduction', 0.5)
        
        # State tracking
        self.positions: Dict[str, PositionInfo] = {}
        self.pe_positions_count = 0
        self.ce_positions_count = 0
        self.consecutive_losses = 0
        self.daily_pnl = 0.0
        self.starting_capital = 0.0
        self.daily_trades = []
        self.rejected_trades = []  # Track why trades were rejected
        
        # Historical data for indicators
        self.price_history: List[float] = []
        self.max_history_size = 1000
        
        # Load historical data for indicators
        self._load_historical_data()
        
        # Initialize Position Manager for SL reconciliation
        self.sl_reconcile_on_start = config.get('sl_reconcile_on_start', True)
        self.position_manager: Optional[PositionManager] = None
        if self.sl_enabled and self.sl_reconcile_on_start:
            state_file = config.get('sl_state_file', 'artifacts/survivor_enhanced_position_state.json')
            self.position_manager = PositionManager(
                broker=broker,
                config=config,
                state_file=state_file
            )
            logger.info(f"PositionManager initialized for SL reconciliation (state_file: {state_file})")
        
    def reconcile_sl_at_startup(self) -> Optional[SLReconciliationResult]:
        """
        Run SL reconciliation at strategy startup.
        
        This should be called once at the beginning of the trading session
        to ensure all open positions have SL protection.
        
        Returns:
            SLReconciliationResult if reconciliation was run, None otherwise
        """
        if not self.position_manager:
            logger.info("PositionManager not initialized, skipping SL reconciliation")
            return None
        
        if not self.sl_enabled:
            logger.info("SL orders disabled, skipping reconciliation")
            return None
        
        if not self.sl_reconcile_on_start:
            logger.info("SL reconciliation on start disabled")
            return None
        
        logger.info("=" * 60)
        logger.info("STARTING SL ORDER RECONCILIATION")
        logger.info("=" * 60)
        
        result = self.position_manager.reconcile_at_startup()
        
        # Update local positions dict with reconciled SL order IDs
        for pos_id, pos_state in self.position_manager.get_all_positions().items():
            if pos_state.symbol in self.positions:
                # Update existing position with SL info from reconciliation
                self.positions[pos_state.symbol].sl_order_id = pos_state.sl_order_id
                self.positions[pos_state.symbol].sl_price = pos_state.sl_trigger_price
                self.positions[pos_state.symbol].sl_limit_price = pos_state.sl_limit_price
                logger.info(
                    f"Updated position {pos_state.symbol} with reconciled SL: "
                    f"{pos_state.sl_order_id}"
                )
        
        # Log reconciliation summary
        logger.info("=" * 60)
        logger.info("SL RECONCILIATION COMPLETE")
        logger.info(f"  Total positions found: {result.total_positions}")
        logger.info(f"  New SL orders placed: {result.new_sl_placed}")
        logger.info(f"  Errors: {len(result.errors)}")
        if result.errors:
            for error in result.errors:
                logger.warning(f"  - {error}")
        logger.info("=" * 60)
        
        return result
    
    def _load_historical_data(self):
        """Load historical data for technical indicators"""
        try:
            # Get historical data for indicator calculation
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - pd.Timedelta(days=self.atr_history_days)).strftime('%Y-%m-%d')
            
            history = self.broker.get_history(
                self.strat_var_index_symbol,
                interval='5m',
                start=start_date,
                end=end_date
            )
            
            if history and len(history) > 0:
                self.price_history = [candle['close'] for candle in history]
                logger.info(f"Loaded {len(self.price_history)} historical candles for indicator calculation")
            else:
                logger.warning("Could not load historical data, indicators will be disabled initially")
                
        except Exception as e:
            logger.error(f"Error loading historical data: {e}")
            
    def _calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        """Calculate Relative Strength Index"""
        if len(prices) < period + 1:
            return 50.0  # Neutral if insufficient data
            
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
            
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)
        
    def _calculate_ema(self, prices: List[float], period: int = 20) -> float:
        """Calculate Exponential Moving Average"""
        if len(prices) < period:
            return prices[-1] if prices else 0.0
            
        prices_array = np.array(prices)
        ema = pd.Series(prices_array).ewm(span=period, adjust=False).mean()
        return float(ema.iloc[-1])
        
    def _calculate_atr(self, high_low_close: List[Dict], period: int = 14) -> float:
        """Calculate Average True Range"""
        if len(high_low_close) < period + 1:
            return 0.0
            
        tr_list = []
        for i in range(1, len(high_low_close)):
            high = high_low_close[i]['high']
            low = high_low_close[i]['low']
            prev_close = high_low_close[i-1]['close']
            
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            
            tr_list.append(max(tr1, tr2, tr3))
            
        return float(np.mean(tr_list[-period:])) if tr_list else 0.0
        
    def _get_technical_indicators(self, current_price: float) -> TechnicalIndicators:
        """Calculate all technical indicators"""
        indicators = TechnicalIndicators()
        
        # Update price history
        self.price_history.append(current_price)
        if len(self.price_history) > self.max_history_size:
            self.price_history = self.price_history[-self.max_history_size:]
            
        if len(self.price_history) < self.rsi_period + 1:
            return indicators  # Return defaults if insufficient data
            
        # Calculate RSI
        indicators.rsi = self._calculate_rsi(self.price_history, self.rsi_period)
        
        # Calculate EMA
        indicators.ema = self._calculate_ema(self.price_history, self.ema_period)
        
        # Determine trend
        if current_price > indicators.ema * 1.005:
            indicators.trend = TrendBias.BULLISH
        elif current_price < indicators.ema * 0.995:
            indicators.trend = TrendBias.BEARISH
        else:
            indicators.trend = TrendBias.NEUTRAL
            
        # Calculate ATR (simplified - would need OHLC data)
        indicators.atr = np.std(self.price_history[-self.atr_period:]) * np.sqrt(self.atr_period)
        
        # Classify volatility regime
        avg_price = np.mean(self.price_history[-self.atr_period:])
        atr_percent = (indicators.atr / avg_price) * 100 if avg_price > 0 else 0
        
        if atr_percent < 0.3:
            indicators.volatility_regime = VolatilityRegime.LOW
        elif atr_percent > 0.8:
            indicators.volatility_regime = VolatilityRegime.HIGH
        else:
            indicators.volatility_regime = VolatilityRegime.NORMAL
            
        return indicators
        
    def _check_entry_filters(self, option_type: str, indicators: TechnicalIndicators) -> Tuple[bool, str]:
        """
        Check if entry passes all configured filters
        
        Returns:
            (passed: bool, reason: str)
        """
        if self.entry_filter_type == 'NONE':
            return True, "No filters configured"
            
        filters_passed = []
        filters_failed = []
        
        # RSI Filter
        if self.entry_filter_type in ['RSI', 'ALL']:
            if option_type == 'PE':
                # Don't sell PE (bet against upside) if already overbought
                if indicators.rsi > self.rsi_max:
                    filters_failed.append(f"RSI {indicators.rsi:.1f} > {self.rsi_max}")
                else:
                    filters_passed.append(f"RSI {indicators.rsi:.1f}")
            else:  # CE
                # Don't sell CE (bet against downside) if already oversold
                if indicators.rsi < self.rsi_min:
                    filters_failed.append(f"RSI {indicators.rsi:.1f} < {self.rsi_min}")
                else:
                    filters_passed.append(f"RSI {indicators.rsi:.1f}")
                    
        # EMA Filter - Trade with trend
        if self.entry_filter_type in ['EMA', 'ALL']:
            if option_type == 'PE':
                # Only sell PE in uptrend or neutral
                if indicators.trend == TrendBias.BEARISH:
                    filters_failed.append(f"Trend is {indicators.trend.value}")
                else:
                    filters_passed.append(f"Trend {indicators.trend.value}")
            else:  # CE
                # Only sell CE in downtrend or neutral
                if indicators.trend == TrendBias.BULLISH:
                    filters_failed.append(f"Trend is {indicators.trend.value}")
                else:
                    filters_passed.append(f"Trend {indicators.trend.value}")
                    
        # ADX Filter - Only trade strong trends
        if self.entry_filter_type in ['ADX', 'ALL']:
            if indicators.adx < self.adx_threshold:
                filters_failed.append(f"ADX {indicators.adx:.1f} < {self.adx_threshold}")
            else:
                filters_passed.append(f"ADX {indicators.adx:.1f}")
                
        if filters_failed:
            return False, f"Filters failed: {', '.join(filters_failed)}"
        return True, f"Filters passed: {', '.join(filters_passed)}"
        
    def _get_dynamic_gap(self, base_gap: float, indicators: TechnicalIndicators) -> float:
        """Calculate dynamic gap based on volatility"""
        if not self.enable_dynamic_gaps:
            return base_gap
            
        # Adjust gap based on volatility regime
        if indicators.volatility_regime == VolatilityRegime.HIGH:
            return base_gap * 1.5  # 50% wider in high volatility
        elif indicators.volatility_regime == VolatilityRegime.LOW:
            return base_gap * 0.8  # 20% tighter in low volatility
        return base_gap
        
    def _get_position_size(self, base_quantity: int, indicators: TechnicalIndicators) -> int:
        """Calculate position size based on volatility"""
        if not self.volatility_sizing:
            return base_quantity
            
        # Reduce size in high volatility
        if indicators.volatility_regime == VolatilityRegime.HIGH:
            return int(base_quantity * self.high_vol_size_reduction)
        
        # Increase size slightly in low volatility (optional)
        if indicators.volatility_regime == VolatilityRegime.LOW:
            return int(base_quantity * 1.1)
            
        return base_quantity
        
    def _check_position_limits(self, option_type: str) -> Tuple[bool, str]:
        """Check if we can add new positions"""
        current_pe = self.pe_positions_count
        current_ce = self.ce_positions_count
        total = current_pe + current_ce
        
        if total >= self.max_total_positions:
            return False, f"Max total positions ({self.max_total_positions}) reached"
            
        if option_type == 'PE' and current_pe >= self.max_positions_per_side:
            return False, f"Max PE positions ({self.max_positions_per_side}) reached"
            
        if option_type == 'CE' and current_ce >= self.max_positions_per_side:
            return False, f"Max CE positions ({self.max_positions_per_side}) reached"
            
        if self.consecutive_losses >= self.max_consecutive_losses:
            return False, f"Max consecutive losses ({self.max_consecutive_losses}) reached"
            
        return True, "Position limits OK"
        
    def _check_stop_losses(self):
        """
        Check positions for stop-loss conditions only.
        
        NOTE: When sl_enabled is True, broker-level SL orders are already placed,
        so the broker will automatically exit positions. This method primarily:
        1. Monitors positions without broker SL (fallback/legacy mode)
        2. Tracks position P&L for reporting
        3. Handles trailing stop logic (if enabled)
        
        Profit target checking is handled separately by _check_profit_targets()
        which runs on a configured interval (default: every 5 minutes).
        """
        positions_to_exit = []
        
        for symbol, position in self.positions.items():
            try:
                quote = self.broker.get_quote(f"NFO:{symbol}")
                current_price = quote.last_price
                
                # Calculate P&L for short position
                pnl_per_unit = position.entry_price - current_price
                position.current_pnl = pnl_per_unit * position.quantity
                
                # Update highest P&L for trailing stop
                if position.current_pnl > position.highest_pnl:
                    position.highest_pnl = position.current_pnl
                
                # Skip manual SL check if broker SL order is active
                if self.sl_enabled and position.sl_order_id:
                    # Check if SL order is still open
                    sl_order = self.broker.get_order(position.sl_order_id)
                    if sl_order and sl_order.get('status') in ['OPEN', 'PENDING', 'TRIGGER PENDING']:
                        # SL order is active, broker will handle exit
                        continue
                    elif sl_order and sl_order.get('status') in ['COMPLETE', 'FILLED']:
                        # SL was hit and executed by broker
                        logger.warning(f"SL executed by broker for {symbol}. Removing from tracking.")
                        positions_to_exit.append((symbol, position, "BROKER_SL_HIT"))
                        continue
                    else:
                        # SL order status unknown or cancelled, fall back to manual check
                        logger.warning(f"SL order {position.sl_order_id} for {symbol} not active. Status: {sl_order.get('status') if sl_order else 'N/A'}")
                
                # Manual SL check (fallback when sl_enabled is False or SL order failed)
                if not self.sl_enabled or not position.sl_order_id:
                    # Check legacy stop-loss (premium doubled = 100% loss on short)
                    stop_price = position.entry_price * self.stop_loss_multiplier
                    if current_price >= stop_price:
                        positions_to_exit.append((symbol, position, "STOP_LOSS"))
                        continue
                
                # Check trailing stop (always check, regardless of broker SL)
                if self.trailing_stop_enabled:
                    trailing_trigger = position.highest_pnl * (1 - self.trailing_stop_distance)
                    if position.current_pnl < trailing_trigger and position.highest_pnl > 0:
                        positions_to_exit.append((symbol, position, "TRAILING_STOP"))
                        
            except Exception as e:
                logger.error(f"Error checking stop-loss for {symbol}: {e}")
                
        # Exit positions
        for symbol, position, reason in positions_to_exit:
            logger.warning(f"Exiting {symbol} due to {reason}. P&L: ₹{position.current_pnl:.2f}")
            self._exit_position(symbol, position)
            
    def _check_profit_targets(self):
        """
        Check positions for profit target conditions.
        
        This method is called on a configured interval (default: every 5 minutes)
        to check if any positions have reached their profit target.
        
        When profit target is hit:
        1. Position is exited immediately (market buy order)
        2. Associated SL order is cancelled
        3. Position is removed from tracking
        
        Returns:
            List of tuples (symbol, position, profit_percent) for positions that hit target
        """
        if not self.profit_target_enabled:
            return []
            
        # Check if enough time has passed since last profit check
        current_time = datetime.now()
        time_since_last_check = (current_time - self.last_profit_check_time).total_seconds()
        
        if time_since_last_check < self.profit_check_interval:
            return []
            
        # Update last check time
        self.last_profit_check_time = current_time
        
        positions_to_exit = []
        
        for symbol, position in self.positions.items():
            try:
                quote = self.broker.get_quote(f"NFO:{symbol}")
                current_price = quote.last_price
                
                # Check profit target
                if position.profit_target_price > 0 and current_price <= position.profit_target_price:
                    profit_percent = (position.entry_price - current_price) / position.entry_price * 100
                    logger.info(
                        f"PROFIT TARGET HIT for {symbol}: "
                        f"entry=₹{position.entry_price:.2f}, current=₹{current_price:.2f}, "
                        f"target=₹{position.profit_target_price:.2f} ({profit_percent:.1f}% profit, "
                        f"configured: {self.profit_target_percent}%)"
                    )
                    positions_to_exit.append((symbol, position, "PROFIT_TARGET"))
                    
            except Exception as e:
                logger.error(f"Error checking profit target for {symbol}: {e}")
                
        # Exit positions that hit profit target
        for symbol, position, reason in positions_to_exit:
            logger.warning(f"Exiting {symbol} due to {reason}. P&L: ₹{position.current_pnl:.2f}")
            self._exit_position(symbol, position)
            
        if positions_to_exit:
            logger.info(f"Profit target check complete: {len(positions_to_exit)} position(s) exited")
            
        return positions_to_exit
        
    def check_profit_targets_at_startup(self):
        """
        Check profit targets immediately at strategy startup.
        
        This is called once when the strategy starts to check if any existing
        positions have already hit their profit target. This ensures that
        positions that became profitable while the strategy was offline
        are exited immediately.
        
        Returns:
            List of tuples (symbol, position, profit_percent) for positions that hit target
        """
        if not self.profit_target_enabled:
            logger.info("Profit target is disabled, skipping startup profit check")
            return []
            
        if not self.positions:
            logger.info("No positions to check at startup")
            return []
            
        logger.info("=" * 60)
        logger.info("CHECKING PROFIT TARGETS AT STARTUP")
        logger.info(f"Positions to check: {len(self.positions)}")
        logger.info(f"Profit target: {self.profit_target_percent}%")
        logger.info("=" * 60)
        
        positions_to_exit = []
        
        for symbol, position in list(self.positions.items()):
            try:
                quote = self.broker.get_quote(f"NFO:{symbol}")
                current_price = quote.last_price
                
                # Calculate current profit percentage
                profit_percent = (position.entry_price - current_price) / position.entry_price * 100
                
                logger.info(
                    f"Position {symbol}: entry=₹{position.entry_price:.2f}, "
                    f"current=₹{current_price:.2f}, profit={profit_percent:.1f}%, "
                    f"target={self.profit_target_percent}%"
                )
                
                # Check if profit target reached
                if position.profit_target_price > 0 and current_price <= position.profit_target_price:
                    logger.info(f"  ✓ PROFIT TARGET ALREADY HIT - Will exit now")
                    positions_to_exit.append((symbol, position, "PROFIT_TARGET_STARTUP"))
                else:
                    logger.info(f"  ✗ Profit target not yet reached (need {self.profit_target_percent}%)")
                    
            except Exception as e:
                logger.error(f"Error checking profit target at startup for {symbol}: {e}")
                
        # Exit positions that hit profit target
        for symbol, position, reason in positions_to_exit:
            logger.warning(f"Exiting {symbol} at startup due to {reason}. P&L: ₹{position.current_pnl:.2f}")
            self._exit_position(symbol, position)
            
        logger.info("=" * 60)
        logger.info(f"Startup profit check complete: {len(positions_to_exit)} position(s) exited")
        logger.info("=" * 60)
        
        return positions_to_exit
            
    def _calculate_sl_prices(self, entry_price: float) -> Tuple[float, float]:
        """
        Calculate SL trigger and limit prices based on entry price and configured SL percentage.
        
        For SHORT positions (options selling):
        - SL trigger = entry_price * (1 + sl_percentage/100)
        - SL limit = trigger_price (same as trigger, rounded to tick size)
        
        Args:
            entry_price: Entry price of the position
            
        Returns:
            Tuple of (trigger_price, limit_price)
        """
        # Calculate SL trigger price (adverse move for shorts)
        sl_multiplier = 1 + (self.sl_percentage / 100)
        trigger_price = entry_price * sl_multiplier
        
        # Round to nearest tick size (0.05 for NIFTY options)
        tick_size = 0.05
        trigger_price = round(trigger_price / tick_size) * tick_size
        
        # Limit price is same as trigger price
        limit_price = trigger_price
        
        return trigger_price, limit_price
    
    def _place_sl_order(self, symbol: str, quantity: int, entry_price: float,
                        option_type: str, fresh_order_id: str) -> Optional[str]:
        """
        Place a Stop-Loss order for a freshly entered position.
        
        Args:
            symbol: Trading symbol
            quantity: Quantity to cover
            entry_price: Entry price for SL calculation
            option_type: 'PE' or 'CE'
            fresh_order_id: ID of the fresh order (for tracking)
            
        Returns:
            SL order ID if successful, None otherwise
        """
        logger.info("=" * 80)
        logger.info(f"[DIAGNOSTIC] _place_sl_order called for {symbol}")
        logger.info(f"[DIAGNOSTIC]   -> quantity={quantity}, entry_price={entry_price}")
        logger.info(f"[DIAGNOSTIC]   -> option_type={option_type}, fresh_order_id={fresh_order_id}")
        logger.info(f"[DIAGNOSTIC]   -> sl_enabled={self.sl_enabled}")
        
        if not self.sl_enabled:
            logger.info(f"[DIAGNOSTIC] SL orders disabled. No SL placed for {symbol}")
            return None
        
        # CRITICAL: Check if SL order already exists for this symbol
        if symbol in self.positions:
            existing_pos = self.positions[symbol]
            if existing_pos.sl_order_id:
                # Verify if the SL order is still active at broker
                try:
                    sl_order = self.broker.get_order(existing_pos.sl_order_id)
                    if sl_order and sl_order.get('status') in ['OPEN', 'PENDING', 'TRIGGER PENDING', 'AMO REQ', 'PUT ORDER REQ RECEIVED']:
                        logger.warning(
                            f"[SL DUPLICATE PREVENTION] Active SL order already exists for {symbol}. "
                            f"Order ID: {existing_pos.sl_order_id}, Status: {sl_order.get('status')}. "
                            f"Skipping duplicate SL order placement."
                        )
                        return existing_pos.sl_order_id
                    else:
                        logger.warning(
                            f"[SL DUPLICATE PREVENTION] Existing SL order {existing_pos.sl_order_id} for {symbol} "
                            f"is not active (Status: {sl_order.get('status') if sl_order else 'N/A'}). "
                            f"Will place new SL order."
                        )
                except Exception as e:
                    logger.warning(
                        f"[SL DUPLICATE PREVENTION] Could not verify SL order status for {symbol}: {e}. "
                        f"Proceeding with caution."
                    )
        
        try:
            # Calculate SL prices
            trigger_price, limit_price = self._calculate_sl_prices(entry_price)
            logger.info(f"[DIAGNOSTIC]   -> Calculated SL prices: trigger={trigger_price}, limit={limit_price}")
            
            # Determine order type
            if self.sl_order_type_str == "STOP":
                order_type = OrderType.STOP  # SL-M (Stop Loss Market)
                sl_price = trigger_price
            else:
                order_type = OrderType.STOP_LIMIT  # SL (Stop Loss Limit)
                sl_price = limit_price
            
            # Create SL order request (BUY to cover short position)
            req = OrderRequest(
                symbol=symbol,
                exchange=Exchange.NFO,
                transaction_type=TransactionType.BUY,  # Buy to cover short
                quantity=quantity,
                product_type=ProductType.MARGIN,
                order_type=order_type,
                price=sl_price if order_type == OrderType.STOP_LIMIT else 0,
                stop_price=trigger_price,
                tag=f"SE_SL_{str(fresh_order_id)[:8]}"
            )
            
            # Place SL order
            logger.info(f"[DIAGNOSTIC]   -> Placing SL order with broker...")
            order_resp = self.broker.place_order(req)
            
            if order_resp.status == "ok":
                sl_order_id = str(order_resp.order_id)
                logger.info(
                    f"[DIAGNOSTIC]   -> SL order PLACED successfully: ID={sl_order_id}"
                )
                logger.info(
                    f"SL order placed for {symbol}: ID={sl_order_id}, "
                    f"Trigger=₹{trigger_price}, Limit=₹{limit_price} "
                    f"({self.sl_percentage}% SL)"
                )
                
                # Update PositionManager with the new SL order
                if self.position_manager:
                    self.position_manager.update_position_sl(
                        symbol=symbol,
                        sl_order_id=sl_order_id,
                        sl_trigger=trigger_price,
                        sl_limit=limit_price
                    )
                
                return sl_order_id
            else:
                logger.error(f"[DIAGNOSTIC]   -> SL order FAILED: {order_resp.message}")
                logger.error(f"Failed to place SL order for {symbol}: {order_resp.message}")
                return None
                
        except Exception as e:
            logger.error(f"[DIAGNOSTIC]   -> SL order EXCEPTION: {e}")
            logger.error(f"Error placing SL order for {symbol}: {e}")
            return None
    
    def _cancel_sl_order(self, sl_order_id: str, symbol: str) -> bool:
        """
        Cancel a Stop-Loss order when position is manually exited.
        
        Args:
            sl_order_id: ID of the SL order to cancel
            symbol: Trading symbol for logging
            
        Returns:
            True if cancelled successfully, False otherwise
        """
        if not sl_order_id:
            return True
            
        try:
            order_resp = self.broker.cancel_order(sl_order_id)
            if order_resp.status == "ok":
                logger.info(f"SL order {sl_order_id} cancelled for {symbol}")
                return True
            else:
                logger.warning(f"Failed to cancel SL order {sl_order_id} for {symbol}: {order_resp.message}")
                return False
        except Exception as e:
            logger.error(f"Error cancelling SL order {sl_order_id} for {symbol}: {e}")
            return False
    
    def _exit_position(self, symbol: str, position: PositionInfo):
        """Exit a position by buying back the option"""
        try:
            # First, cancel any existing SL order for this position
            if position.sl_order_id:
                self._cancel_sl_order(position.sl_order_id, symbol)
            
            # Place buy order to close short position
            req = OrderRequest(
                symbol=symbol,
                exchange=Exchange.NFO,
                transaction_type=TransactionType.BUY,
                quantity=position.quantity,
                product_type=ProductType.MARGIN,
                order_type=OrderType.MARKET,
                price=0,
                tag="Survivor_SL"
            )
            
            order_resp = self.broker.place_order(req)
            
            if order_resp.status == "ok":
                # Update tracking
                if position.current_pnl < 0:
                    self.consecutive_losses += 1
                else:
                    self.consecutive_losses = 0
                    
                self.daily_pnl += position.current_pnl
                
                # Update position count
                if position.option_type == 'PE':
                    self.pe_positions_count -= 1
                else:
                    self.ce_positions_count -= 1
                    
                # Remove from PositionManager
                if self.position_manager:
                    self.position_manager.remove_position(symbol)
                
                # DEBUG: Log before removing
                logger.info(f"[POSITION TRACKING] Removing {symbol} from positions. Current count: {len(self.positions)}")
                
                del self.positions[symbol]
                
                logger.info(f"[POSITION TRACKING] After removal: {len(self.positions)} positions remaining: {list(self.positions.keys())}")
                logger.info(f"Position {symbol} exited. P&L: ₹{position.current_pnl:.2f}")
            else:
                logger.error(f"Failed to exit position {symbol}: {order_resp.message}")
                
        except Exception as e:
            logger.error(f"Error exiting position {symbol}: {e}")
            
    def _check_time_based_exit(self) -> bool:
        """Check if it's time for square off"""
        if not self.square_off_time:
            return False
            
        try:
            hour, minute = map(int, self.square_off_time.split(':'))
            current_time = datetime.now().time()
            
            if current_time >= dt_time(hour, minute):
                logger.info(f"Time-based exit triggered at {self.square_off_time}")
                return True
        except ValueError:
            logger.error(f"Invalid square_off_time format: {self.square_off_time}")
            
        return False
        
    def _square_off_all_positions(self):
        """Square off all open positions"""
        logger.info("Squaring off all positions...")
        
        for symbol, position in list(self.positions.items()):
            self._exit_position(symbol, position)
            
    def _check_daily_loss_limit(self) -> bool:
        """Check if daily loss limit reached"""
        if self.starting_capital == 0:
            return False
            
        loss_percent = (self.daily_pnl / self.starting_capital) * 100
        if loss_percent <= self.max_daily_loss_percent:
            logger.error(f"Daily loss limit reached: {loss_percent:.2f}%")
            return True
            
        return False
        
    def _record_trade(self, option_type: str, symbol: str, quantity: int, 
                      price: float, indicators: TechnicalIndicators, 
                      status: str, reason: str = ""):
        """Record trade for analysis"""
        trade = {
            'timestamp': datetime.now().isoformat(),
            'option_type': option_type,
            'symbol': symbol,
            'quantity': quantity,
            'price': price,
            'rsi': indicators.rsi,
            'ema': indicators.ema,
            'trend': indicators.trend.value,
            'atr': indicators.atr,
            'volatility_regime': indicators.volatility_regime.value,
            'status': status,
            'reason': reason
        }
        
        if status == 'REJECTED':
            self.rejected_trades.append(trade)
        else:
            self.daily_trades.append(trade)
            
    def on_ticks_update(self, ticks):
        """
        Enhanced tick processing with risk management
        """
        current_price = ticks['last_price'] if 'last_price' in ticks else ticks['ltp']
        
        # Get technical indicators
        indicators = self._get_technical_indicators(current_price)
        
        # Log indicator values periodically
        # if len(self.daily_trades) % 10 == 0:
            # logger.info(f"Indicators - RSI: {indicators.rsi:.1f}, EMA: {indicators.ema:.2f}, "
            #            f"Trend: {indicators.trend.value}, ATR: {indicators.atr:.2f}, "
            #            f"Vol: {indicators.volatility_regime.value}")
        
        # Check time-based exit
        if self._check_time_based_exit():
            self._square_off_all_positions()
            return
            
        # Check stop-losses on existing positions (runs every tick)
        if self.positions:
            self._check_stop_losses()
            
        # Check profit targets on configured interval (default: every 5 minutes)
        print("====================s==",  self.positions , self.profit_target_enabled)
        if self.positions and self.profit_target_enabled:
            print("========g=======fg=======")
            self._check_profit_targets()
            
        # Check daily loss limit
        if self._check_daily_loss_limit():
            self._square_off_all_positions()
            return
            
        # Process trading opportunities with enhanced logic
        self._handle_pe_trade_enhanced(current_price, indicators)
        self._handle_ce_trade_enhanced(current_price, indicators)
        
        # Apply reset logic
        self._reset_reference_values(current_price)
        
    def _handle_pe_trade_enhanced(self, current_price: float, indicators: TechnicalIndicators):
        """Enhanced PE trade handling with filters"""
        # Check position limits first
        can_trade, reason = self._check_position_limits('PE')
        if not can_trade:
            logger.debug(f"PE trade blocked: {reason}")
            return
            
        # Apply dynamic gap
        effective_pe_gap = self._get_dynamic_gap(self.strat_var_pe_gap, indicators)
        
        # Check price movement
        if current_price <= self.nifty_pe_last_value:
            return
            
        price_diff = round(current_price - self.nifty_pe_last_value, 0)
        logger.info(f"[PE TRACKING] Current: {current_price}, Last PE ref: {self.nifty_pe_last_value}, Diff: {price_diff}, Need: {effective_pe_gap}")
        
        if price_diff <= effective_pe_gap:
            return
            
        # Check entry filters
        filters_passed, filter_msg = self._check_entry_filters('PE', indicators)
        if not filters_passed:
            logger.info(f"PE trade rejected: {filter_msg}")
            self._record_trade('PE', '', 0, 0, indicators, 'REJECTED', filter_msg)
            return
            
        logger.info(f"PE trade filters passed: {filter_msg}")
        
        # Calculate position size
        sell_multiplier = int(price_diff / effective_pe_gap)
        if self._check_sell_multiplier_breach(sell_multiplier):
            return
            
        base_quantity = sell_multiplier * self.strat_var_pe_quantity
        final_quantity = self._get_position_size(base_quantity, indicators)
        
        # Update reference
        self.nifty_pe_last_value += effective_pe_gap * sell_multiplier
        
        # Find and execute trade
        temp_gap = self.strat_var_pe_symbol_gap
        while True:
            instrument = self._find_nifty_symbol_from_gap("PE", current_price, gap=temp_gap)
            if not instrument:
                logger.warning(f"No suitable PE instrument found with gap {temp_gap}")
                return
                
            symbol_code = f"{self.strat_var_exchange}:{instrument['symbol']}"
            quote = self.broker.get_quote(symbol_code)
            
            if quote.last_price < self.strat_var_min_price_to_sell:
                temp_gap -= self.lot_size
                continue
                
            # Execute the trade
            logger.info(f"Execute PE sell @ {instrument['symbol']} × {final_quantity}, "
                       f"Premium: ₹{quote.last_price}, Multiplier: {sell_multiplier}")
            
            order_success = self._place_order_enhanced(
                instrument['symbol'], final_quantity, quote.last_price, 'PE', indicators
            )
            
            if order_success:
                self.pe_positions_count += 1
                self.pe_reset_gap_flag = 1
                
            break
            
    def _handle_ce_trade_enhanced(self, current_price: float, indicators: TechnicalIndicators):
        """Enhanced CE trade handling with filters"""
        # Check position limits
        can_trade, reason = self._check_position_limits('CE')
        if not can_trade:
            logger.debug(f"CE trade blocked: {reason}")
            return
            
        # Apply dynamic gap
        effective_ce_gap = self._get_dynamic_gap(self.strat_var_ce_gap, indicators)
        
        # Check price movement
        if current_price >= self.nifty_ce_last_value:
            return
            
        price_diff = round(self.nifty_ce_last_value - current_price, 0)
        if price_diff <= effective_ce_gap:
            return
            
        # Check entry filters
        filters_passed, filter_msg = self._check_entry_filters('CE', indicators)
        if not filters_passed:
            logger.info(f"CE trade rejected: {filter_msg}")
            self._record_trade('CE', '', 0, 0, indicators, 'REJECTED', filter_msg)
            return
            
        logger.info(f"CE trade filters passed: {filter_msg}")
        
        # Calculate position size
        sell_multiplier = int(price_diff / effective_ce_gap)
        if self._check_sell_multiplier_breach(sell_multiplier):
            return
            
        base_quantity = sell_multiplier * self.strat_var_ce_quantity
        final_quantity = self._get_position_size(base_quantity, indicators)
        
        # Update reference
        self.nifty_ce_last_value -= effective_ce_gap * sell_multiplier
        
        # Find and execute trade
        temp_gap = self.strat_var_ce_symbol_gap
        while True:
            instrument = self._find_nifty_symbol_from_gap("CE", current_price, gap=temp_gap)
            if not instrument:
                logger.warning(f"No suitable CE instrument found with gap {temp_gap}")
                return
                
            symbol_code = f"{self.strat_var_exchange}:{instrument['symbol']}"
            quote = self.broker.get_quote(symbol_code)
            
            if quote.last_price < self.strat_var_min_price_to_sell:
                temp_gap -= self.lot_size
                continue
                
            # Execute the trade
            logger.info(f"Execute CE sell @ {instrument['symbol']} × {final_quantity}, "
                       f"Premium: ₹{quote.last_price}, Multiplier: {sell_multiplier}")
            
            order_success = self._place_order_enhanced(
                instrument['symbol'], final_quantity, quote.last_price, 'CE', indicators
            )
            
            if order_success:
                self.ce_positions_count += 1
                self.ce_reset_gap_flag = 1
                
            break
            
    def _place_order_enhanced(self, symbol: str, quantity: int, price: float,
                              option_type: str, indicators: TechnicalIndicators) -> bool:
        """Enhanced order placement with position tracking and automatic SL order"""
        try:
            # CRITICAL: Check if position already exists for this symbol
            # This prevents duplicate orders and duplicate SL orders
            if symbol in self.positions:
                existing_pos = self.positions[symbol]
                logger.warning(
                    f"[DUPLICATE PREVENTION] Position already exists for {symbol}. "
                    f"Entry: ₹{existing_pos.entry_price}, Qty: {existing_pos.quantity}, "
                    f"SL Order: {existing_pos.sl_order_id or 'N/A'}. "
                    f"Skipping new order placement."
                )
                return False
            
            # Also check position manager if available (for extra safety)
            if self.position_manager:
                position_id = f"{symbol}_NFO_NRML"
                all_managed_positions = self.position_manager.get_all_positions()
                if position_id in all_managed_positions:
                    managed_pos = all_managed_positions[position_id]
                    if managed_pos.sl_order_id:
                        logger.warning(
                            f"[DUPLICATE PREVENTION] Position already tracked in PositionManager for {symbol}. "
                            f"SL Order: {managed_pos.sl_order_id}. Skipping new order placement."
                        )
                        return False
            
            # Step 0: Calculate SL prices FIRST (before placing any orders)
            trigger_price, limit_price = self._calculate_sl_prices(price)
            
            # Show SL calculation preview
            logger.info("=" * 60)
            logger.info(f"SL CALCULATION PREVIEW for {symbol}")
            logger.info("=" * 60)
            logger.info(f"Entry Price: ₹{price:.2f}")
            logger.info(f"SL Percentage: {self.sl_percentage}%")
            logger.info(f"SL Trigger Price: ₹{trigger_price:.2f}")
            logger.info(f"SL Limit Price: ₹{limit_price:.2f}")
            logger.info(f"SL Order Type: {self.sl_order_type.value}")
            logger.info("=" * 60)
            
            # Step 1: Place fresh order (SELL for short options)
            req = OrderRequest(
                symbol=symbol,
                exchange=Exchange.NFO,
                transaction_type=TransactionType.SELL,
                quantity=quantity,
                product_type=ProductType.MARGIN,
                order_type=OrderType.MARKET,
                price=0,
                tag=self.strat_var_tag
            )
            
            order_resp = self.broker.place_order(req)
            
            if order_resp.status != "ok":
                logger.error(f"Order placement failed: {order_resp.message}")
                return False
            
            fresh_order_id = str(order_resp.order_id)
            logger.info(f"Fresh order placed successfully: {fresh_order_id}")
            
            # Step 2: Place SL order immediately (if enabled)
            sl_order_id = None
            if self.sl_enabled:
                sl_order_id = self._place_sl_order(
                    symbol=symbol,
                    quantity=quantity,
                    entry_price=price,
                    option_type=option_type,
                    fresh_order_id=fresh_order_id
                )
                
                if sl_order_id:
                    logger.info(
                        f"SL protection active for {symbol}: {self.sl_percentage}% "
                        f"(Trigger: ₹{trigger_price}, Limit: ₹{limit_price})"
                    )
                else:
                    logger.warning(
                        f"SL order failed for {symbol}. Position is UNPROTECTED! "
                        f"Consider manual exit or position monitoring."
                    )
            else:
                logger.info(f"SL orders disabled. Position {symbol} has no automatic protection.")
            
            # Calculate profit target price (60% of premium collected)
            if self.profit_target_enabled:
                profit_target_price = price * (1 - self.profit_target_percent / 100)
                # Round to nearest tick size (0.05 for NIFTY options)
                tick_size = 0.05
                profit_target_price = round(profit_target_price / tick_size) * tick_size
                logger.info(
                    f"Profit target set for {symbol}: {self.profit_target_percent}% "
                    f"(Entry: ₹{price:.2f} → Target: ₹{profit_target_price:.2f})"
                )
            else:
                profit_target_price = 0.0
            
            # Step 4: Track the position with SL and profit target info
            self.positions[symbol] = PositionInfo(
                symbol=symbol,
                entry_price=price,
                entry_time=datetime.now(),
                quantity=quantity,
                option_type=option_type,
                sl_order_id=sl_order_id,
                sl_price=trigger_price,
                sl_limit_price=limit_price,
                profit_target_price=profit_target_price
            )
            
            # DEBUG: Log positions dict after adding
            logger.info(f"[POSITION TRACKING] Added {symbol} to positions. Total positions: {len(self.positions)}")
            logger.info(f"[POSITION TRACKING] Current positions: {list(self.positions.keys())}")
            
            # BUG FIX: Reset profit check time so new position gets checked immediately
            # This ensures profit targets are evaluated for the new position without waiting
            self.last_profit_check_time = datetime.now() - timedelta(seconds=self.profit_check_interval + 1)
            logger.info(f"[PROFIT FIX] Reset profit check timer for new position {symbol}")
            
            # Step 5: Record the trade
            self._record_trade(option_type, symbol, quantity, price, indicators, 'EXECUTED')
            
            # Step 6: Log position summary
            profit_target_str = f"PT: {self.profit_target_percent}% @ ₹{profit_target_price:.2f}" if self.profit_target_enabled else "PT: Disabled"
            logger.info(
                f"Position established - Symbol: {symbol}, Qty: {quantity}, "
                f"Entry: ₹{price}, SL: {self.sl_percentage}% @ ₹{trigger_price}, "
                f"{profit_target_str}, SL Order: {sl_order_id or 'N/A'}"
            )
            return True
                
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return False
            
    def get_strategy_stats(self) -> Dict:
        """Get comprehensive strategy statistics"""
        # Count positions with broker SL protection
        protected_positions = sum(1 for p in self.positions.values() if p.sl_order_id)
        unprotected_positions = len(self.positions) - protected_positions
        
        return {
            'positions': {
                'pe_count': self.pe_positions_count,
                'ce_count': self.ce_positions_count,
                'total': len(self.positions),
                'protected_by_sl': protected_positions,
                'unprotected': unprotected_positions,
                'details': [
                    {
                        'symbol': p.symbol,
                        'type': p.option_type,
                        'entry': p.entry_price,
                        'current_pnl': p.current_pnl,
                        'sl_price': p.sl_price,
                        'sl_limit_price': p.sl_limit_price,
                        'sl_order_id': p.sl_order_id,
                        'protected': p.sl_order_id is not None
                    }
                    for p in self.positions.values()
                ]
            },
            'daily_stats': {
                'pnl': self.daily_pnl,
                'trades_taken': len(self.daily_trades),
                'trades_rejected': len(self.rejected_trades),
                'consecutive_losses': self.consecutive_losses
            },
            'risk_management': {
                'sl_enabled': self.sl_enabled,
                'sl_percentage': self.sl_percentage,
                'sl_order_type': self.sl_order_type_str,
                'sl_limit_buffer': self.sl_limit_buffer,
                'stop_loss_multiplier': self.stop_loss_multiplier,
                'trailing_stop_enabled': self.trailing_stop_enabled
            },
            'filters': {
                'entry_filter_type': self.entry_filter_type,
                'rejection_reasons': [
                    t['reason'] for t in self.rejected_trades[-10:]
                ]
            }
        }


# =============================================================================
# MAIN EXECUTION BLOCK
# =============================================================================

if __name__ == "__main__":
    """
    Main execution block for Enhanced Survivor Strategy.
    
    This block:
    1. Loads configuration from survivor_enhanced.yml
    2. Sets up broker connection
    3. Initializes the strategy with PositionManager
    4. Runs SL reconciliation at startup
    5. Starts the main trading loop
    """
    import time
    import yaml
    import sys
    import os
    from queue import Queue
    import traceback
    import warnings
    warnings.filterwarnings("ignore")
    
    from dispatcher import DataDispatcher
    from orders import OrderTracker
    from brokers import BrokerGateway
    from logger import strategy_logger as logger
    
    import logging
    logger.setLevel(logging.INFO)
    
    # ==========================================================================
    # SECTION 1: CONFIGURATION LOADING
    # ==========================================================================
    
    config_file = os.path.join(os.path.dirname(__file__), "configs/survivor_enhanced.yml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)['default']
    print(config)
    logger.info("=" * 80)
    logger.info("ENHANCED SURVIVOR STRATEGY - STARTING")
    logger.info("=" * 80)
    
    # Log key configuration
    logger.info(f"Configuration loaded from: {config_file}")
    logger.info(f"Symbol: {config['symbol_initials']}")
    logger.info(f"SL Enabled: {config.get('sl_enabled', True)}")
    logger.info(f"SL Percentage: {config.get('sl_percentage', 60)}%")
    logger.info(f"SL Reconcile on Start: {config.get('sl_reconcile_on_start', True)}")
    logger.info(f"Tick Logging: {config.get('log_tick_data', False)}")
    logger.info(f"Profit Target Enabled: {config.get('profit_target_enabled', True)}")
    logger.info(f"Profit Target: {config.get('profit_target_percent', 60)}%")
    logger.info(f"Profit Check Interval: {config.get('profit_check_interval', 300)}s ({config.get('profit_check_interval', 300)//60} minutes)")
    
    # ==========================================================================
    # SECTION 2: BROKER AND INFRASTRUCTURE SETUP
    # ==========================================================================
    
    # Create broker interface
    broker = BrokerGateway.from_name(os.getenv("BROKER_NAME", "zerodha"))
    
    # Create order tracking system
    order_tracker = OrderTracker()
    
    # Get instrument token for the underlying index
    try:
        quote_data = broker.get_quote(config['index_symbol'])
        instrument_token = config['index_symbol']
        logger.info(f"✓ Index instrument token obtained: {instrument_token}")
    except Exception as e:
        logger.error(f"Failed to get instrument token for {config['index_symbol']}: {e}")
        sys.exit(1)
    
    # Initialize data dispatcher
    dispatcher = DataDispatcher()
    dispatcher.register_main_queue(Queue())
    
    # ==========================================================================
    # SECTION 3: WEBSOCKET CALLBACK CONFIGURATION
    # ==========================================================================
    
    def on_ticks(ws, ticks):
        """Handle incoming tick data"""
        if config.get('log_tick_data', False):
            logger.debug("Received ticks: {}".format(ticks))
        if isinstance(ticks, list):
            dispatcher.dispatch(ticks)
        else:
            if "symbol" in ticks:
                dispatcher.dispatch(ticks)
    
    def on_connect(ws, response):
        """Handle websocket connection"""
        logger.info("Websocket connected successfully: {}".format(response))
    
    def on_order_update(ws, data):
        """Handle order updates"""
        logger.info(f"Order update received: {data}")
    
    # Assign callbacks
    broker.on_ticks = on_ticks
    broker.on_connect = on_connect
    broker.on_order_update = on_order_update
    
    # ==========================================================================
    # SECTION 4: STRATEGY INITIALIZATION
    # ==========================================================================
    
    # Start websocket connection
    broker.connect_websocket(on_ticks=on_ticks, on_connect=on_connect)
    broker.symbols_to_subscribe([instrument_token])
    broker.connect_order_websocket(on_order_update=on_order_update)
    time.sleep(10)
    
    # Initialize the strategy
    strategy = EnhancedSurvivorStrategy(broker, config, order_tracker)
    
    # ==========================================================================
    # SECTION 5: SL RECONCILIATION AT STARTUP
    # ==========================================================================
    
    # Run SL reconciliation to protect existing positions
    reconciliation_result = strategy.reconcile_sl_at_startup()
    
    if reconciliation_result:
        logger.info(f"SL Reconciliation Result:")
        logger.info(f"  - Total positions: {reconciliation_result.total_positions}")
        logger.info(f"  - New SL orders placed: {reconciliation_result.new_sl_placed}")
        logger.info(f"  - Errors: {len(reconciliation_result.errors)}")
    
    # ==========================================================================
    # SECTION 6: PROFIT TARGET CHECK AT STARTUP
    # ==========================================================================
    
    # Check if any existing positions have already hit their profit target
    # This handles cases where the strategy was offline and positions became profitable
    profit_check_result = strategy.check_profit_targets_at_startup()
    
    if profit_check_result:
        logger.info(f"Startup Profit Check Result:")
        logger.info(f"  - Positions exited: {len(profit_check_result)}")
    
    # ==========================================================================
    # SECTION 7: MAIN TRADING LOOP
    # ==========================================================================
    
    logger.info("=" * 80)
    logger.info("STARTING MAIN TRADING LOOP")
    logger.info("=" * 80)
    
    try:
        while True:
            try:
                # Get market data from dispatcher
                tick_data = dispatcher._main_queue.get()
                
                # Extract symbol data
                if isinstance(tick_data, list):
                    symbol_data = tick_data[0]
                else:
                    symbol_data = tick_data
                
                # Process tick through strategy
                if isinstance(symbol_data, dict) and ('last_price' in symbol_data or 'ltp' in symbol_data):
                    strategy.on_ticks_update(symbol_data)
                
            except KeyboardInterrupt:
                logger.info("SHUTDOWN REQUESTED - Stopping strategy...")
                break
                
            except Exception as tick_error:
                logger.error(f"Error processing tick data: {tick_error}", exc_info=True)
                continue
    
    except Exception as fatal_error:
        logger.error("FATAL ERROR in main trading loop:")
        logger.error(f"Error: {fatal_error}")
        traceback.print_exc()
        
    finally:
        logger.info("=" * 80)
        logger.info("STRATEGY SHUTDOWN COMPLETE")
        logger.info("=" * 80)
