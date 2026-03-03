"""
Strategy module for trading algorithms.

This module provides:
- BaseStrategy: Abstract base class for all strategies
- SurvivorStrategy: NIFTY options selling strategy
- WaveStrategy: Wave-based trading strategy
- PositionManager: SL order reconciliation and position tracking
- OrderTracker: In-memory order tracking

Usage:
    from strategy import BaseStrategy, SurvivorStrategy, WaveStrategy
    from strategy import PositionManager, OrderTracker
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
]
