"""
Visual Engine API Routes

Provides endpoints for the Visual Engine dashboard that displays:
- Real-time entry predictions for PE/CE
- Active filter statuses (RSI, EMA, ADX, Gap Risk)
- Recent entry signals and rejections
- Market context and daily statistics
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    VisualStateResponse,
    StrategyVisualState,
    FilterStatus,
    FilterType,
    FilterStatusType,
    EntryPrediction,
    EntrySignal,
    SignalType,
    MarketContext,
    GapAssessment,
    DailyStats,
    TrendDirection,
    VolatilityRegime,
    PredictionStatus,
)
from app.services.strategy_manager import strategy_manager
from app.services.broker_service import broker_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/strategy", tags=["visual"])


@router.get("/visual-state", response_model=VisualStateResponse)
async def get_visual_state() -> VisualStateResponse:
    """
    Get current visual state for the strategy dashboard.
    
    Returns complete visual state including:
    - Entry predictions for PE and CE with trigger levels
    - Active filter statuses (RSI, EMA, ADX, Gap Risk)
    - Recent entry signals and rejections
    - Market context (trend, volatility, gap assessment)
    - Daily trading statistics
    
    Returns 503 if strategy is not running.
    """
    try:
        # Check if strategy is running
        state = strategy_manager.get_state()
        
        if state.status.value not in ["RUNNING", "STARTING"]:
            return VisualStateResponse(
                success=False,
                data=None,
                error="Strategy not running",
                message="Start the strategy to view visual state data"
            )
        
        # Try to get cached visual state from strategy manager
        visual_state_data = strategy_manager.get_visual_state()
        
        # If we have visual state data, enhance it with current NIFTY price
        if visual_state_data:
            try:
                # Fetch current NIFTY price from broker
                nifty_quote = broker_service.get_quote("NSE:NIFTY 50")
                if nifty_quote and nifty_quote.last_price:
                    visual_state_data["market_context"]["nifty_price"] = nifty_quote.last_price
                    
                    # Update predictions with current price
                    if visual_state_data.get("predictions"):
                        if visual_state_data["predictions"].get("pe"):
                            visual_state_data["predictions"]["pe"]["current_price"] = nifty_quote.last_price
                        if visual_state_data["predictions"].get("ce"):
                            visual_state_data["predictions"]["ce"]["current_price"] = nifty_quote.last_price
            except Exception as e:
                logger.warning(f"Could not fetch NIFTY price for visual state: {e}")
            
            # Validate and return the visual state
            try:
                validated_state = StrategyVisualState(**visual_state_data)
                return VisualStateResponse(
                    success=True,
                    data=validated_state,
                    error=None,
                    message="Visual state fetched successfully"
                )
            except Exception as validation_error:
                logger.error(f"Visual state validation error: {validation_error}")
                # Return the data anyway, even if validation fails
                return VisualStateResponse(
                    success=True,
                    data=None,
                    error=f"Data validation error: {str(validation_error)}",
                    message="Visual state data available but has validation issues"
                )
        
        # No visual state available yet - return a loading response
        return VisualStateResponse(
            success=True,
            data=None,
            error=None,
            message="Visual state is initializing. Please try again in a few seconds."
        )
        
    except Exception as e:
        logger.error(f"Error fetching visual state: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal error fetching visual state: {str(e)}"
        )


@router.get("/visual-state/simple")
async def get_visual_state_simple() -> Dict[str, Any]:
    """
    Get simplified visual state (for debugging and quick checks).
    
    Returns a simple dict without full Pydantic validation.
    """
    try:
        state = strategy_manager.get_state()
        visual_data = strategy_manager.get_visual_state()
        
        return {
            "strategy_running": state.status.value == "RUNNING",
            "current_strategy": state.current_strategy,
            "uptime_seconds": state.uptime_seconds,
            "has_visual_data": visual_data is not None,
            "visual_data_preview": {
                "filters_count": len(visual_data.get("filters", {}).get("items", [])) if visual_data else 0,
                "signals_count": len(visual_data.get("signals", [])) if visual_data else 0,
            } if visual_data else None
        }
    except Exception as e:
        return {
            "error": str(e),
            "strategy_running": False
        }


@router.post("/visual-state/refresh")
async def refresh_visual_state() -> VisualStateResponse:
    """
    Force refresh of visual state cache.
    
    This endpoint requests fresh visual state data from the strategy
    and updates the cache. Use this when you need the latest data
    and don't want to wait for the auto-refresh.
    """
    try:
        state = strategy_manager.get_state()
        
        if state.status.value != "RUNNING":
            return VisualStateResponse(
                success=False,
                data=None,
                error="Strategy not running",
                message="Start the strategy to refresh visual state"
            )
        
        # Note: In a future enhancement, we could send a message to the
        # strategy process to request fresh visual state. For now, we
        # just clear the cache so the next fetch gets fresh data.
        strategy_manager.update_visual_state_cache({})
        
        # Fetch fresh data
        visual_data = strategy_manager.get_visual_state()
        
        return VisualStateResponse(
            success=True,
            data=StrategyVisualState(**visual_data) if visual_data else None,
            error=None,
            message="Visual state refreshed"
        )
        
    except Exception as e:
        logger.error(f"Error refreshing visual state: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error refreshing visual state: {str(e)}"
        )