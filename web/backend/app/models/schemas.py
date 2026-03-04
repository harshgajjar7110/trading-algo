"""
Pydantic models for API requests and responses.
"""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# =============================================================================
# Enums
# =============================================================================

class StrategyStatus(str, Enum):
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    ERROR = "ERROR"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    COMPLETE = "COMPLETE"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


# =============================================================================
# Strategy Models
# =============================================================================

class StrategyState(BaseModel):
    """Current state of the running strategy."""
    status: StrategyStatus
    nifty_pe_last_value: Optional[float] = None
    nifty_ce_last_value: Optional[float] = None
    pe_reset_flag: bool = False
    ce_reset_flag: bool = False
    last_update: Optional[datetime] = None
    error_message: Optional[str] = None
    uptime_seconds: Optional[float] = None
    # Extended fields for strategy selection
    current_strategy: Optional[str] = None  # "Strategy Name (strategy_id)"
    instance_id: Optional[str] = None  # Unique instance ID


class StrategyStartRequest(BaseModel):
    """Request to start the strategy."""
    config_override: Optional[Dict[str, Any]] = None


class StrategyStartResponse(BaseModel):
    """Response after starting strategy."""
    success: bool
    message: str
    status: StrategyStatus


class StrategyStopResponse(BaseModel):
    """Response after stopping strategy."""
    success: bool
    message: str
    status: StrategyStatus


# =============================================================================
# Configuration Models
# =============================================================================

class StrategyConfigUpdate(BaseModel):
    """Partial configuration update."""
    # Core Parameters
    index_symbol: Optional[str] = None
    symbol_initials: Optional[str] = None

    # Gap Parameters
    pe_gap: Optional[int] = None
    ce_gap: Optional[int] = None
    pe_reset_gap: Optional[int] = None
    ce_reset_gap: Optional[int] = None

    # Strike Selection
    pe_symbol_gap: Optional[int] = None
    ce_symbol_gap: Optional[int] = None

    # Position Sizing
    pe_quantity: Optional[int] = None
    ce_quantity: Optional[int] = None

    # Risk Management
    min_price_to_sell: Optional[float] = None
    sell_multiplier_threshold: Optional[int] = None

    # Reference Points
    pe_start_point: Optional[int] = None
    ce_start_point: Optional[int] = None

    # Order Settings
    exchange: Optional[str] = None
    order_type: Optional[str] = None
    product_type: Optional[str] = None
    trans_type: Optional[str] = None

    # Entry Filters
    entry_filter_type: Optional[str] = None
    rsi_period: Optional[int] = None
    rsi_min: Optional[int] = None
    rsi_max: Optional[int] = None
    adx_period: Optional[int] = None
    adx_threshold: Optional[int] = None
    ema_period: Optional[int] = None

    # ============================================================
    # ENHANCED: ATR Settings
    # ============================================================
    atr_period: Optional[int] = None
    atr_history_days: Optional[int] = None

    # ============================================================
    # ENHANCED: Position Limits
    # ============================================================
    max_positions_per_side: Optional[int] = None
    max_total_positions: Optional[int] = None
    max_consecutive_losses: Optional[int] = None

    # ============================================================
    # ENHANCED: Stop-Loss Settings
    # ============================================================
    sl_enabled: Optional[bool] = None
    sl_percentage: Optional[int] = None
    sl_order_type: Optional[str] = None
    sl_limit_buffer: Optional[float] = None
    sl_reconcile_on_start: Optional[bool] = None

    # ============================================================
    # ENHANCED: Profit Target Settings
    # ============================================================
    profit_target_enabled: Optional[bool] = None
    profit_target_percent: Optional[int] = None
    profit_check_interval: Optional[int] = None

    # ============================================================
    # ENHANCED: Legacy Stop-Loss
    # ============================================================
    stop_loss_multiplier: Optional[float] = None

    # ============================================================
    # ENHANCED: Trailing Stop
    # ============================================================
    trailing_stop_enabled: Optional[bool] = None
    trailing_stop_distance: Optional[float] = None

    # ============================================================
    # ENHANCED: Daily Loss Limit
    # ============================================================
    max_daily_loss_percent: Optional[float] = None

    # ============================================================
    # ENHANCED: Time-based Square Off
    # ============================================================
    square_off_time: Optional[str] = None

    # ============================================================
    # ENHANCED: Dynamic Gap Adjustment
    # ============================================================
    enable_dynamic_gaps: Optional[bool] = None
    atr_multiplier_pe: Optional[float] = None
    atr_multiplier_ce: Optional[float] = None

    # ============================================================
    # ENHANCED: Volatility-based Position Sizing
    # ============================================================
    volatility_sizing: Optional[bool] = None
    high_vol_size_reduction: Optional[float] = None

    # ============================================================
    # ENHANCED: Position Initialization
    # ============================================================
    enable_position_init: Optional[bool] = None

    # ============================================================
    # ENHANCED: Logging
    # ============================================================
    log_tick_data: Optional[bool] = None


class ConfigValidationResponse(BaseModel):
    """Response for configuration validation."""
    valid: bool
    errors: List[str] = []
    warnings: List[str] = []


# =============================================================================
# Position Models
# =============================================================================

class Position(BaseModel):
    """Trading position."""
    symbol: str
    exchange: str
    quantity: int
    average_price: float
    current_price: Optional[float] = None
    pnl: Optional[float] = None
    pnl_percent: Optional[float] = None
    product_type: str
    side: str  # LONG or SHORT


class PositionsResponse(BaseModel):
    """Response with all positions."""
    positions: List[Position]
    total_pnl: float
    total_pnl_percent: float
    error: Optional[str] = None  # Error message if positions couldn't be fetched
    message: Optional[str] = None  # Info message about the data source


# =============================================================================
# Order Models
# =============================================================================

class Order(BaseModel):
    """Trading order."""
    order_id: str
    symbol: str
    exchange: str
    transaction_type: TransactionType
    quantity: int
    price: float
    status: OrderStatus
    order_type: str
    product_type: str
    timestamp: datetime
    tag: Optional[str] = None
    filled_quantity: Optional[int] = None
    average_price: Optional[float] = None


class OrdersResponse(BaseModel):
    """Response with orders."""
    orders: List[Order]
    count: int


class Trade(BaseModel):
    """Executed trade."""
    trade_id: str
    order_id: str
    symbol: str
    exchange: str
    transaction_type: TransactionType
    quantity: int
    price: float
    timestamp: datetime
    tag: Optional[str] = None


class TradesResponse(BaseModel):
    """Response with trades."""
    trades: List[Trade]
    count: int


# =============================================================================
# Market Data Models
# =============================================================================

class Quote(BaseModel):
    """Market quote."""
    symbol: str
    exchange: str
    last_price: float
    change: Optional[float] = None
    change_percent: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[int] = None
    high: Optional[float] = None
    low: Optional[float] = None
    open: Optional[float] = None
    close: Optional[float] = None
    timestamp: Optional[datetime] = None


class NiftyData(BaseModel):
    """NIFTY index data with strategy context."""
    quote: Quote
    pe_reference: Optional[float] = None
    ce_reference: Optional[float] = None
    pe_gap_to_trigger: Optional[float] = None
    ce_gap_to_trigger: Optional[float] = None


# =============================================================================
# Account Models
# =============================================================================

class Funds(BaseModel):
    """Account funds."""
    equity: float
    available_cash: float
    used_margin: float
    net: float


# =============================================================================
# WebSocket Models
# =============================================================================

class WSMessage(BaseModel):
    """WebSocket message structure."""
    type: str
    data: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class PriceUpdate(BaseModel):
    """Price update for WebSocket."""
    symbol: str
    last_price: float
    change: Optional[float] = None
    timestamp: datetime


class OrderUpdate(BaseModel):
    """Order update for WebSocket."""
    order_id: str
    symbol: str
    status: OrderStatus
    quantity: int
    filled_quantity: Optional[int] = None
    price: Optional[float] = None
    timestamp: datetime


# =============================================================================
# Error Models
# =============================================================================

class APIError(BaseModel):
    """API error response."""
    code: str
    message: str
    details: Optional[Dict[str, Any]] = None


class HTTPError(BaseModel):
    """HTTP error response."""
    error: APIError


# =============================================================================
# Payoff Analysis Models
# =============================================================================

class OptionType(str, Enum):
    CALL = "CE"
    PUT = "PE"


class PayoffPoint(BaseModel):
    """A single point on the payoff curve."""
    underlying_price: float
    pnl: float
    pnl_percent: float


class PositionPayoff(BaseModel):
    """Payoff analysis for a single position."""
    symbol: str
    option_type: OptionType  # CE or PE
    strike_price: float
    side: str  # LONG or SHORT
    quantity: int
    premium: float  # Entry price (average_price)
    breakeven: float
    breakeven_percent: float  # Distance from current price to breakeven as %
    max_profit: Optional[float] = None  # None means unlimited
    max_loss: Optional[float] = None  # None means unlimited
    current_underlying_price: float
    current_pnl: float
    payoff_curve: List[PayoffPoint]  # P&L at various underlying prices


class PortfolioPayoff(BaseModel):
    """Combined payoff analysis for all positions."""
    positions: List[PositionPayoff]
    combined_payoff_curve: List[PayoffPoint]  # Combined P&L
    combined_breakeven_points: List[float]  # All breakeven points for combined positions
    total_max_profit: Optional[float] = None
    total_max_loss: Optional[float] = None
    current_nifty_price: float
    total_current_pnl: float
    price_range_start: float
    price_range_end: float


# =============================================================================
# Greeks Models
# =============================================================================

class PositionGreeks(BaseModel):
    """Greeks for a single option position."""
    symbol: str
    option_type: str  # CE or PE
    strike: float
    quantity: int
    entry_price: float
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float
    expiry_date: Optional[str] = None
    index_name: Optional[str] = None


class PortfolioGreeks(BaseModel):
    """Aggregate Greeks for the entire portfolio."""
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float


class GreeksInterpretation(BaseModel):
    """Human-readable interpretations of portfolio Greeks."""
    delta: str
    gamma: str
    theta: str
    vega: str
    rho: str


class GreeksResponse(BaseModel):
    """Response containing portfolio Greeks calculation."""
    positions: List[PositionGreeks]
    portfolio: PortfolioGreeks
    interpretation: GreeksInterpretation
    underlying_price: float
    calculation_date: str


class ScenarioResult(BaseModel):
    """Single scenario result for Greeks analysis."""
    price_change_pct: float
    price_change_points: float
    volatility_change: float
    days_forward: int
    estimated_pnl: float
    breakdown: Dict[str, float]


class ScenarioAnalysis(BaseModel):
    """Scenario analysis results for portfolio Greeks."""
    current_greeks: PortfolioGreeks
    scenarios: List[ScenarioResult]
    underlying_price: float
    days_forward: int


# =============================================================================
# Visual Engine Models
# =============================================================================

class FilterType(str, Enum):
    """Types of entry filters."""
    RSI = "RSI"
    EMA = "EMA"
    ADX = "ADX"
    TREND = "TREND"
    GAP = "GAP"


class FilterStatusType(str, Enum):
    """Status of a filter check."""
    PASS = "PASS"
    FAIL = "FAIL"
    PENDING = "PENDING"
    BLOCKED = "BLOCKED"
    DISABLED = "DISABLED"


class PredictionStatus(str, Enum):
    """Status of entry prediction."""
    READY = "READY"
    WAITING = "WAITING"
    BLOCKED = "BLOCKED"
    COOLDOWN = "COOLDOWN"


class TrendDirection(str, Enum):
    """Market trend direction."""
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"


class VolatilityRegime(str, Enum):
    """Volatility classification."""
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"


class SignalType(str, Enum):
    """Type of trading signal."""
    ENTRY = "ENTRY"
    REJECTION = "REJECTION"
    EXIT = "EXIT"


class FilterStatus(BaseModel):
    """Status of an individual entry filter."""
    name: str
    type: FilterType
    enabled: bool
    current_value: Optional[float] = None
    current_value_str: Optional[str] = None
    threshold: Optional[float] = None
    threshold_str: Optional[str] = None
    status: FilterStatusType
    message: str


class EntryPrediction(BaseModel):
    """Prediction for entry on a specific side (PE/CE)."""
    side: str  # "PE" or "CE"
    status: PredictionStatus
    current_price: Optional[float] = None
    trigger_price: Optional[float] = None
    distance_to_trigger: Optional[float] = None
    distance_percent: Optional[float] = None
    estimated_time: Optional[str] = None
    blocking_reasons: List[str] = []
    next_check: Optional[datetime] = None


class EntrySignal(BaseModel):
    """A trading signal (entry, rejection, or exit)."""
    timestamp: datetime
    side: str  # "PE" or "CE"
    type: SignalType
    price: float
    reason: str
    filters_passed: List[str] = []
    filters_failed: List[str] = []


class GapAssessment(BaseModel):
    """Gap risk assessment."""
    can_trade: bool
    message: str
    gap_percent: Optional[float] = None
    threshold_percent: Optional[float] = None


class MarketContext(BaseModel):
    """Current market context."""
    nifty_price: Optional[float] = None
    trend: TrendDirection = TrendDirection.NEUTRAL
    volatility_regime: VolatilityRegime = VolatilityRegime.NORMAL
    atr_value: Optional[float] = None
    gap_assessment: GapAssessment


class DailyStats(BaseModel):
    """Daily trading statistics."""
    trades_taken: int = 0
    trades_rejected: int = 0
    pnl: float = 0.0
    consecutive_losses: int = 0


class StrategyVisualState(BaseModel):
    """Complete visual state for the strategy dashboard."""
    # Algo Status
    is_running: bool
    uptime_seconds: Optional[float] = None
    current_strategy: Optional[str] = None
    last_update: Optional[datetime] = None

    # Active Filters
    filters: Dict[str, Any]  # {entry_filter_type: str, items: List[FilterStatus]}

    # Entry Predictions
    predictions: Dict[str, Optional[EntryPrediction]]  # {"pe": ..., "ce": ...}

    # Recent Signals
    signals: List[EntrySignal] = []

    # Market Context
    market_context: MarketContext

    # Daily Stats
    daily_stats: DailyStats

    # Error message if visual state unavailable
    error: Optional[str] = None
    message: Optional[str] = None


class VisualStateResponse(BaseModel):
    """API response for visual state endpoint."""
    success: bool
    data: Optional[StrategyVisualState] = None
    error: Optional[str] = None
    message: Optional[str] = None
