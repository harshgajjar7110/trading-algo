"""
Configuration management for the Survivor Web Backend.
"""

import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Application
    APP_NAME: str = "Survivor Trading API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # CORS
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://127.0.0.1:3000"]

    # Broker settings (inherited from main project)
    BROKER_NAME: str = "zerodha"

    # Strategy config path
    STRATEGY_CONFIG_PATH: str = str(
        Path(__file__).parent.parent.parent.parent
        / "strategy"
        / "configs"
        / "survivor.yml"
    )

    # WebSocket
    WS_HEARTBEAT_INTERVAL: int = 30  # seconds

    class Config:
        env_file = ".env"
        extra = "ignore"


# Global settings instance
settings = Settings()


class StrategyConfig(BaseModel):
    """Strategy configuration model matching survivor.yml structure."""

    # Core Parameters
    index_symbol: str = "NSE:NIFTY 50"
    symbol_initials: str = "NIFTY26310"

    # Gap Parameters
    pe_gap: int = 40
    ce_gap: int = 40
    pe_reset_gap: int = 30
    ce_reset_gap: int = 30

    # Strike Selection
    pe_symbol_gap: int = 600
    ce_symbol_gap: int = 600

    # Position Sizing
    pe_quantity: int = 65
    ce_quantity: int = 65

    # Risk Management
    min_price_to_sell: float = 15.0
    sell_multiplier_threshold: int = 1

    # Reference Points
    pe_start_point: int = 0
    ce_start_point: int = 0

    # Order Settings
    exchange: str = "NFO"
    order_type: str = "MARKET"
    product_type: str = "NRML"
    trans_type: str = "SELL"
    tag: str = "Survivor"

    # Entry Filters
    entry_filter_type: str = "NONE"
    rsi_period: int = 14
    rsi_min: int = 50
    rsi_max: int = 50
    adx_period: int = 14
    adx_threshold: int = 25
    ema_period: int = 20
    history_period_days: int = 5

    # ============================================================
    # ENHANCED: ATR Settings
    # ============================================================
    atr_period: int = 14
    atr_history_days: int = 5

    # ============================================================
    # ENHANCED: Position Limits
    # ============================================================
    max_positions_per_side: int = 3
    max_total_positions: int = 5
    max_consecutive_losses: int = 2

    # ============================================================
    # ENHANCED: Stop-Loss Settings
    # ============================================================
    sl_enabled: bool = True
    sl_percentage: int = 60
    sl_order_type: str = "STOP_LIMIT"
    sl_limit_buffer: float = 0.05
    sl_reconcile_on_start: bool = True
    # ============================================================
    # ENHANCED: Profit Target Settings
    # ============================================================
    profit_target_enabled: bool = True
    profit_target_percent: int = 60
    profit_check_interval: int = 60  # Check every 60 seconds

    # ============================================================
    # ENHANCED: Volatility Sizing
    # ============================================================
    volatility_sizing: bool = True
    high_vol_size_reduction: float = 0.5

    # ============================================================
    # ENHANCED: Logging
    # ============================================================
    log_tick_data: bool = False

    # ============================================================
    # ENHANCED: Legacy Stop-Loss
    # ============================================================
    stop_loss_multiplier: float = 2.0

    # ============================================================
    # ENHANCED: Trailing Stop-Loss
    # ============================================================
    trailing_sl_enabled: bool = False
    trailing_sl_activation_percent: float = 30.0
    trailing_sl_distance_percent: float = 20.0
    trailing_sl_min_locked_percent: float = 15.0
    trailing_sl_lock_profit: bool = True

    # ============================================================
    # ENHANCED: Daily Loss Limit
    # ============================================================
    max_daily_loss_percent: float = -3.0

    # ============================================================
    # ENHANCED: Time-based Square Off
    # ============================================================
    square_off_time: str = "15:15"

    # ============================================================
    # ENHANCED: Dynamic Gap Adjustment
    # ============================================================
    enable_dynamic_gaps: bool = True
    atr_multiplier_pe: float = 2.0
    atr_multiplier_ce: float = 2.0

    # ============================================================
    # ENHANCED: ATR-Based Strike Selection
    # ============================================================
    enable_atr_strike_selection: bool = False
    atr_strike_min_distance: int = 700
    atr_strike_max_distance: int = 2500
    atr_strike_recalc_minutes: int = 15

    # ============================================================
    # ENHANCED: Position Initialization
    # ============================================================
    enable_position_init: bool = False

    class Config:
        extra = "ignore"
