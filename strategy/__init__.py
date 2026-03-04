"""
Strategy module for trading algorithms.

This module provides:
- BaseStrategy: Abstract base class for all strategies
- SurvivorStrategy: NIFTY options selling strategy
- WaveStrategy: Wave-based trading strategy
- PositionManager: SL order reconciliation and position tracking
- OrderTracker: In-memory order tracking
- GapRiskManager: Gap risk management for option selling
- PreMarketData: Pre-market data fetching for gap analysis

Usage:
    from strategy import BaseStrategy, SurvivorStrategy, WaveStrategy
    from strategy import PositionManager, OrderTracker
    from strategy import GapRiskManager, PreMarketData
"""

from orders import OrderTracker

try:
    from strategy.base_strategy import BaseStrategy, StrategyConfig, TrendBias, VolatilityRegime
except ImportError:
    BaseStrategy = None  # BaseStrategy requires additional dependencies
    StrategyConfig = None
    TrendBias = None
    VolatilityRegime = None

try:
    from strategy.survivor import SurvivorStrategy
except ImportError:
    SurvivorStrategy = None

try:
    from strategy.wave import WaveStrategy
except ImportError:
    WaveStrategy = None

try:
    from strategy.position_manager import PositionManager, SLReconciliationResult
except ImportError:
    PositionManager = None
    SLReconciliationResult = None

try:
    from strategy.gap_risk_manager import GapRiskManager, GapRiskAssessment, DayRiskProfile
except ImportError:
    GapRiskManager = None
    GapRiskAssessment = None
    DayRiskProfile = None

try:
    from strategy.pre_market_data import PreMarketSnapshot, PreMarketDataService, PreMarketData
except ImportError:
    PreMarketSnapshot = None
    PreMarketDataService = None
    PreMarketData = None


__all__ = [
    # Strategies
    "BaseStrategy",
    "SurvivorStrategy",
    "WaveStrategy",
    "StrategyConfig",
    "TrendBias",
    "VolatilityRegime",
    # Managers
    "PositionManager",
    "SLReconciliationResult",
    "OrderTracker",
    # Gap Risk Management
    "GapRiskManager",
    "GapRiskAssessment",
    "DayRiskProfile",
    "PreMarketSnapshot",  # New preferred name
    "PreMarketData",  # Backward compatibility alias
    "PreMarketDataService",
]
