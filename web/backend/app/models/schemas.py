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
    index_symbol: Optional[str] = None
    symbol_initials: Optional[str] = None
    pe_gap: Optional[int] = None
    ce_gap: Optional[int] = None
    pe_reset_gap: Optional[int] = None
    ce_reset_gap: Optional[int] = None
    pe_symbol_gap: Optional[int] = None
    ce_symbol_gap: Optional[int] = None
    pe_quantity: Optional[int] = None
    ce_quantity: Optional[int] = None
    min_price_to_sell: Optional[float] = None
    sell_multiplier_threshold: Optional[int] = None
    pe_start_point: Optional[int] = None
    ce_start_point: Optional[int] = None
    exchange: Optional[str] = None
    order_type: Optional[str] = None
    product_type: Optional[str] = None
    trans_type: Optional[str] = None
    entry_filter_type: Optional[str] = None
    rsi_period: Optional[int] = None
    rsi_min: Optional[int] = None
    rsi_max: Optional[int] = None
    adx_period: Optional[int] = None
    adx_threshold: Optional[int] = None
    ema_period: Optional[int] = None


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
