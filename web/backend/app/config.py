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
    STRATEGY_CONFIG_PATH: str = str(Path(__file__).parent.parent.parent.parent / "strategy" / "configs" / "survivor.yml")
    
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
    symbol_initials: str = "NIFTY26FEB"
    
    # Gap Parameters
    pe_gap: int = 40
    ce_gap: int = 40
    pe_reset_gap: int = 30
    ce_reset_gap: int = 30
    
    # Strike Selection
    pe_symbol_gap: int = 800
    ce_symbol_gap: int = 800
    
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
    
    class Config:
        extra = "ignore"
