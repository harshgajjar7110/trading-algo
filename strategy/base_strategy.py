"""
Base Strategy Module

Provides the BaseStrategy class that consolidates common functionality
across all trading strategies. This reduces code duplication and provides
a consistent interface for strategy implementation.

Usage:
    from strategy.base_strategy import BaseStrategy, StrategyConfig
    
    class MyStrategy(BaseStrategy):
        def __init__(self, config, broker, order_tracker=None):
            super().__init__(config, broker, order_tracker)
            # Strategy-specific initialization
        
        def on_tick(self, tick_data):
            # Implement strategy logic
            pass
"""

import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time, timedelta
from typing import Dict, List, Optional, Tuple, Any, Callable
from enum import Enum

# Import unified logger
from utils.logging import get_strategy_logger

# Import broker types
from brokers import BrokerGateway, OrderRequest, Exchange, OrderType, TransactionType, ProductType
from brokers.core.schemas import Position, Quote, OrderResponse

# Import OrderTracker
from orders import OrderTracker


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
class PositionInfo:
    """Standard position information container"""
    symbol: str
    entry_price: float
    entry_time: datetime
    quantity: int
    option_type: str
    current_pnl: float = 0.0
    highest_pnl: float = 0.0  # For trailing stop
    sl_order_id: Optional[str] = None  # Track SL order ID
    sl_price: float = 0.0  # SL trigger price
    sl_limit_price: float = 0.0  # SL limit price
    profit_target_price: float = 0.0  # Target price for profit taking


@dataclass
class StrategyConfig:
    """Standard strategy configuration container"""
    # Required parameters
    symbol_initials: str = ""
    exchange: str = "NFO"
    
    # Order parameters
    product_type: str = "NRML"
    order_type: str = "LIMIT"
    variety: str = "REGULAR"
    tag: str = "STRATEGY"
    
    # Position sizing
    lot_size: int = 1
    buy_quantity: int = 1
    sell_quantity: int = 1
    
    # Gap/price settings
    buy_gap: float = 0.0
    sell_gap: float = 0.0
    
    # Risk management - SL
    sl_enabled: bool = False
    sl_percentage: float = 60.0
    sl_order_type: str = "STOP_LIMIT"
    sl_limit_buffer: float = 5.0
    stop_loss_multiplier: float = 2.0
    trailing_stop_enabled: bool = False
    trailing_stop_distance: float = 0.5
    sl_reconcile_on_start: bool = True
    
    # Risk management - Profit target
    profit_target_enabled: bool = True
    profit_target_percent: float = 60.0
    profit_check_interval: int = 20
    
    # Risk management - Limits
    max_positions_per_side: int = 3
    max_total_positions: int = 5
    max_consecutive_losses: int = 2
    max_daily_loss_percent: float = -3.0
    
    # Time settings
    square_off_time: str = "15:15"
    cool_off_time: int = 60  # seconds
    
    # Technical indicators
    rsi_period: int = 14
    rsi_min: int = 30
    rsi_max: int = 70
    ema_period: int = 20
    adx_period: int = 14
    adx_threshold: int = 25
    atr_period: int = 14
    
    # Entry filter mode: NONE, RSI, EMA, ADX, ALL
    entry_filter_type: str = "NONE"
    
    # Dynamic gaps
    enable_dynamic_gaps: bool = False
    atr_multiplier_pe: float = 2.0
    atr_multiplier_ce: float = 2.0
    atr_history_days: int = 5
    
    # Volatility sizing
    volatility_sizing: bool = False
    high_vol_size_reduction: float = 0.5
    
    # Gap risk management
    gap_risk_enabled: bool = True  # Enable gap risk protection
    gap_skip_threshold: float = 1.5  # Skip trading if gap > 1.5%
    gap_reduce_threshold: float = 1.0  # Reduce size if gap > 1.0%
    monday_entry_delay: bool = True  # Delay entry on Monday until 10:00 AM
    monday_entry_hour: int = 10  # Monday entry hour
    monday_entry_minute: int = 0  # Monday entry minute
    
    # Day-based position sizing multipliers
    monday_position_multiplier: float = 0.5   # 50% on Monday
    tuesday_position_multiplier: float = 1.0  # 100% on Tuesday
    wednesday_position_multiplier: float = 1.0  # 100% on Wednesday
    thursday_position_multiplier: float = 0.75  # 75% on Thursday (expiry)
    friday_position_multiplier: float = 0.4   # 40% on Friday (weekend risk)
    
    # Reference values (0 = use current market price)
    pe_start_point: float = 0.0
    ce_start_point: float = 0.0
    index_symbol: str = "NSE:NIFTY 50"
    
    # Additional config (for extensibility)
    extra: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "StrategyConfig":
        """Create StrategyConfig from dictionary"""
        # Extract known fields
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        known_fields.discard('extra')  # Handle extra separately
        
        kwargs = {}
        extra = {}
        
        for key, value in config.items():
            if key in known_fields:
                kwargs[key] = value
            else:
                extra[key] = value
        
        kwargs['extra'] = extra
        return cls(**kwargs)


class BaseStrategy(ABC):
    """
    Base class for all trading strategies.
    
    This class consolidates common functionality including:
    - Broker initialization and management
    - Order tracking
    - Position management
    - Logging setup
    - Risk management (SL, profit targets)
    - Configuration management
    
    Subclasses must implement:
    - on_tick(): Main strategy logic
    - initialize(): Strategy-specific initialization
    
    Example:
        class MyStrategy(BaseStrategy):
            def initialize(self):
                # Strategy-specific setup
                pass
            
            def on_tick(self, tick_data):
                # Strategy logic
                pass
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        broker: BrokerGateway,
        order_tracker: Optional[OrderTracker] = None
    ):
        """
        Initialize the base strategy.
        
        Args:
            config: Strategy configuration dictionary
            broker: BrokerGateway instance for order placement
            order_tracker: Optional OrderTracker for order management
        """
        # Store raw config for backward compatibility
        self._raw_config = config
        
        # Parse config into typed structure
        self.config = StrategyConfig.from_dict(config)
        
        # Initialize unified logger
        strategy_name = self.__class__.__name__
        self.logger = get_strategy_logger(strategy_name)
        
        # Log initialization
        self.logger.info("=" * 70)
        self.logger.info(f"[{strategy_name.upper()}] Initializing {strategy_name}")
        self.logger.info("=" * 70)
        
        # Validate required config
        if not self.config.symbol_initials:
            raise ValueError("Config missing required 'symbol_initials' key")
        
        # External dependencies
        self.broker = broker
        self.order_tracker = order_tracker
        
        # Initialize broker instruments
        self._initialize_instruments()
        
        # Initialize base state
        self._initialize_state()
        
        # Strategy-specific initialization
        self.initialize()
        
        self.logger.info(f"{strategy_name} initialization complete")
    
    def _initialize_instruments(self):
        """Download and filter broker instruments"""
        self.logger.info("Downloading instruments...")
        self.broker.download_instruments()
        self.all_instruments = self.broker.get_instruments()
        
        # Filter instruments by symbol initials
        if 'symbol' in self.all_instruments.columns:
            self.instruments = self.all_instruments[
                self.all_instruments['symbol'].str.contains(self.config.symbol_initials, na=False)
            ]
        else:
            self.instruments = self.all_instruments
        
        if hasattr(self.instruments, 'shape') and self.instruments.shape[0] == 0:
            self.logger.warning(f"No instruments found for {self.config.symbol_initials}")
        elif len(self.instruments) == 0:
            self.logger.warning(f"No instruments found for {self.config.symbol_initials}")
        else:
            # Get lot size from instruments
            if 'lot_size' in self.instruments.columns:
                self.config.lot_size = int(self.instruments['lot_size'].iloc[0])
                self.logger.info(f"Lot size: {self.config.lot_size}")
        
        # Calculate strike difference
        self.strike_difference = self._get_strike_difference()
    
    def _initialize_state(self):
        """Initialize strategy state"""
        # Position tracking
        self.positions: Dict[str, PositionInfo] = {}
        self.pe_positions_count = 0
        self.ce_positions_count = 0
        
        # Risk tracking
        self.consecutive_losses = 0
        self.daily_pnl = 0.0
        self.starting_capital = 0.0
        self.daily_trades: List[Dict] = []
        self.rejected_trades: List[Dict] = []
        
        # State flags
        self.pe_reset_gap_flag = 0
        self.ce_reset_gap_flag = 0
        
        # Time tracking
        self.last_profit_check_time = datetime.now()
        
        # Historical data
        self.price_history: List[float] = []
        self.max_history_size = 1000
        
        # Reference values
        self._initialize_reference_values()
    
    def _initialize_reference_values(self):
        """Initialize PE/CE reference values based on config"""
        try:
            current_quote = self._get_index_quote()
            current_price = current_quote.last_price if hasattr(current_quote, 'last_price') else 0.0
        except Exception as e:
            self.logger.warning(f"Could not get index quote: {e}")
            current_price = 0.0
        
        # PE reference
        if self.config.pe_start_point == 0:
            self.nifty_pe_last_value = current_price
            self.logger.debug(f"PE Start Point is 0, using LTP: {self.nifty_pe_last_value}")
        else:
            self.nifty_pe_last_value = self.config.pe_start_point
        
        # CE reference
        if self.config.ce_start_point == 0:
            self.nifty_ce_last_value = current_price
            self.logger.debug(f"CE Start Point is 0, using LTP: {self.nifty_ce_last_value}")
        else:
            self.nifty_ce_last_value = self.config.ce_start_point
        
        self.logger.info(
            f"Reference values - PE: {self.nifty_pe_last_value}, "
            f"CE: {self.nifty_ce_last_value}"
        )
    
    def _get_index_quote(self) -> Quote:
        """Get quote for the configured index symbol"""
        return self.broker.get_quote(self.config.index_symbol)
    
    def _get_strike_difference(self) -> int:
        """Calculate strike difference from available instruments"""
        if not hasattr(self.instruments, 'columns'):
            return 50  # Default for NIFTY
        
        try:
            # Filter for CE instruments
            ce_instruments = self.instruments[
                self.instruments['symbol'].str.contains(self.config.symbol_initials, na=False) &
                self.instruments['symbol'].str.endswith('CE', na=False)
            ]
            
            if 'strike' not in ce_instruments.columns or len(ce_instruments) < 2:
                return 50  # Default
            
            # Sort by strike and get top 2
            ce_sorted = ce_instruments.sort_values('strike')
            top2 = ce_sorted.head(2)
            
            # Calculate difference
            diff = int(top2['strike'].iloc[1] - top2['strike'].iloc[0])
            return diff
        except Exception as e:
            self.logger.warning(f"Could not calculate strike difference: {e}")
            return 50  # Default for NIFTY
    
    # -------------------------------------------------------------------------
    # Abstract Methods - Must be implemented by subclasses
    # -------------------------------------------------------------------------
    
    @abstractmethod
    def initialize(self) -> None:
        """
        Strategy-specific initialization.
        Called after base initialization is complete.
        """
        pass
    
    @abstractmethod
    def on_tick(self, tick_data: Any) -> None:
        """
        Process tick data.
        Main strategy logic goes here.
        
        Args:
            tick_data: Tick data from the broker/data feed
        """
        pass
    
    # -------------------------------------------------------------------------
    # Order Placement Methods
    # -------------------------------------------------------------------------
    
    def place_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: TransactionType,
        order_type: Optional[OrderType] = None,
        price: float = 0.0,
        trigger_price: float = 0.0,
        tag: Optional[str] = None
    ) -> Optional[OrderResponse]:
        """
        Place an order through the broker.
        
        Args:
            symbol: Trading symbol
            quantity: Order quantity
            transaction_type: BUY or SELL
            order_type: Order type (defaults to config.order_type)
            price: Limit price (for limit orders)
            trigger_price: Trigger price (for SL orders)
            tag: Order tag for tracking
        
        Returns:
            OrderResponse if successful, None otherwise
        """
        try:
            # Use default values from config if not specified
            order_type_enum = order_type or OrderType[self.config.order_type]
            product_type_enum = ProductType[self.config.product_type]
            order_tag = tag or self.config.tag
            
            # Build order request
            request = OrderRequest(
                symbol=symbol,
                exchange=Exchange.NFO,
                transaction_type=transaction_type,
                order_type=order_type_enum,
                product_type=product_type_enum,
                quantity=abs(quantity),
                price=price,
                trigger_price=trigger_price,
                tag=order_tag
            )
            
            self.logger.info(
                f"Placing {transaction_type.value} order: {symbol} x {quantity} "
                f"@ {price if price else 'market'}"
            )
            
            # Place order
            response = self.broker.place_order(request)
            
            # Track order if order_tracker is available
            if self.order_tracker and hasattr(response, 'order_id'):
                self.order_tracker.add_order({
                    'order_id': response.order_id,
                    'symbol': symbol,
                    'quantity': quantity,
                    'transaction_type': transaction_type.value,
                    'price': price,
                    'timestamp': datetime.now().isoformat(),
                    'status': 'placed'
                })
            
            return response
            
        except Exception as e:
            self.logger.error(f"Error placing order: {e}")
            return None
    
    def place_market_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: TransactionType,
        tag: Optional[str] = None
    ) -> Optional[OrderResponse]:
        """Place a market order"""
        return self.place_order(
            symbol=symbol,
            quantity=quantity,
            transaction_type=transaction_type,
            order_type=OrderType.MARKET,
            tag=tag
        )
    
    def place_limit_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: TransactionType,
        price: float,
        tag: Optional[str] = None
    ) -> Optional[OrderResponse]:
        """Place a limit order"""
        return self.place_order(
            symbol=symbol,
            quantity=quantity,
            transaction_type=transaction_type,
            order_type=OrderType.LIMIT,
            price=price,
            tag=tag
        )
    
    def place_sl_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: TransactionType,
        trigger_price: float,
        limit_price: float = 0.0,
        tag: Optional[str] = None
    ) -> Optional[OrderResponse]:
        """
        Place a stop-loss order.
        
        Args:
            symbol: Trading symbol
            quantity: Order quantity
            transaction_type: BUY or SELL
            trigger_price: SL trigger price
            limit_price: SL limit price (0 for SL-M)
            tag: Order tag
        """
        order_type = OrderType.SL_LIMIT if limit_price > 0 else OrderType.SL
        
        return self.place_order(
            symbol=symbol,
            quantity=quantity,
            transaction_type=transaction_type,
            order_type=order_type,
            price=limit_price,
            trigger_price=trigger_price,
            tag=tag
        )
    
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order"""
        try:
            response = self.broker.cancel_order(order_id)
            success = hasattr(response, 'status') and response.status == 'ok'
            if success:
                self.logger.info(f"Order {order_id} cancelled successfully")
            else:
                self.logger.warning(f"Failed to cancel order {order_id}")
            return success
        except Exception as e:
            self.logger.error(f"Error cancelling order {order_id}: {e}")
            return False
    
    # -------------------------------------------------------------------------
    # Position Management Methods
    # -------------------------------------------------------------------------
    
    def get_positions(self) -> List[Position]:
        """Get all positions from broker"""
        try:
            return self.broker.get_positions()
        except Exception as e:
            self.logger.error(f"Error getting positions: {e}")
            return []
    
    def get_position_for_symbol(self, symbol: str) -> int:
        """Get current position quantity for a specific symbol"""
        try:
            positions = self.get_positions()
            for position in positions:
                if position.symbol == symbol:
                    self.logger.debug(f"Position for {symbol}: {position.quantity_total}")
                    return position.quantity_total
            return 0
        except Exception as e:
            self.logger.error(f"Error getting position for {symbol}: {e}")
            return 0
    
    def get_net_position(self) -> int:
        """Get net position across all symbols matching strategy initials"""
        try:
            positions = self.get_positions()
            net_qty = 0
            for pos in positions:
                if pos.symbol.startswith(self.config.symbol_initials):
                    net_qty += pos.quantity_total
            return net_qty
        except Exception as e:
            self.logger.error(f"Error calculating net position: {e}")
            return 0
    
    # -------------------------------------------------------------------------
    # Risk Management Methods
    # -------------------------------------------------------------------------
    
    def should_square_off(self) -> bool:
        """Check if positions should be squared off based on time"""
        try:
            square_off_time = datetime.strptime(self.config.square_off_time, "%H:%M").time()
            current_time = datetime.now().time()
            return current_time >= square_off_time
        except ValueError:
            self.logger.error(f"Invalid square_off_time format: {self.config.square_off_time}")
            return False
    
    def is_max_loss_reached(self) -> bool:
        """Check if max daily loss limit is reached"""
        if self.starting_capital == 0:
            return False
        
        loss_percent = (self.daily_pnl / self.starting_capital) * 100
        return loss_percent <= self.config.max_daily_loss_percent
    
    def can_open_new_position(self, option_type: str) -> bool:
        """
        Check if a new position can be opened.
        
        Args:
            option_type: "PE" or "CE"
        
        Returns:
            True if position can be opened, False otherwise
        """
        # Check max positions per side
        if option_type == "PE" and self.pe_positions_count >= self.config.max_positions_per_side:
            self.logger.debug(f"Max PE positions reached: {self.pe_positions_count}")
            return False
        
        if option_type == "CE" and self.ce_positions_count >= self.config.max_positions_per_side:
            self.logger.debug(f"Max CE positions reached: {self.ce_positions_count}")
            return False
        
        # Check total positions
        total_positions = self.pe_positions_count + self.ce_positions_count
        if total_positions >= self.config.max_total_positions:
            self.logger.debug(f"Max total positions reached: {total_positions}")
            return False
        
        # Check consecutive losses
        if self.consecutive_losses >= self.config.max_consecutive_losses:
            self.logger.warning(f"Max consecutive losses reached: {self.consecutive_losses}")
            return False
        
        # Check max daily loss
        if self.is_max_loss_reached():
            self.logger.warning("Max daily loss reached, stopping new positions")
            return False
        
        return True
    
    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------
    
    def is_market_open(self) -> bool:
        """Check if market is currently open"""
        now = datetime.now().time()
        market_open = dt_time(9, 15)
        market_close = dt_time(15, 30)
        return market_open <= now <= market_close
    
    def format_inr(self, amount: float) -> str:
        """Format amount as Indian Rupees"""
        return f"₹{amount:,.2f}"
    
    def get_instrument_quote(self, symbol: str) -> Optional[Quote]:
        """Get quote for a specific instrument"""
        try:
            return self.broker.get_quote(f"NFO:{symbol}")
        except Exception as e:
            self.logger.error(f"Error getting quote for {symbol}: {e}")
            return None
    
    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------
    
    def cleanup(self) -> None:
        """
        Cleanup resources before shutdown.
        Override in subclass for custom cleanup.
        """
        self.logger.info("Strategy cleanup called")
        # Cancel any pending orders if needed
        # Close any open positions if needed
