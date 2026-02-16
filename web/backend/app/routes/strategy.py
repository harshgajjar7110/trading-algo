"""
Strategy API Routes

Endpoints for controlling the trading strategy.
"""
from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    StrategyState, StrategyStatus,
    StrategyStartRequest, StrategyStartResponse,
    StrategyStopResponse
)
from app.services.strategy_manager import strategy_manager


router = APIRouter(prefix="/strategy", tags=["Strategy"])


@router.get("/status", response_model=StrategyState)
async def get_strategy_status():
    """
    Get current strategy status and state.
    
    Returns:
        StrategyState: Current strategy state including status, reference values, and flags
    """
    return strategy_manager.get_state()


@router.post("/start", response_model=StrategyStartResponse)
async def start_strategy(request: StrategyStartRequest = None):
    """
    Start the trading strategy.
    
    Args:
        request: Optional configuration overrides
        
    Returns:
        StrategyStartResponse: Result of start operation
    """
    state = strategy_manager.get_state()
    
    if state.status == StrategyStatus.RUNNING:
        return StrategyStartResponse(
            success=True,
            message="Strategy is already running",
            status=StrategyStatus.RUNNING
        )
    
    config_override = request.config_override if request else None
    success = strategy_manager.start(config_override)
    
    if success:
        return StrategyStartResponse(
            success=True,
            message="Strategy started successfully",
            status=StrategyStatus.RUNNING
        )
    else:
        return StrategyStartResponse(
            success=False,
            message="Failed to start strategy",
            status=strategy_manager.get_state().status
        )


@router.post("/stop", response_model=StrategyStopResponse)
async def stop_strategy():
    """
    Stop the trading strategy.
    
    Returns:
        StrategyStopResponse: Result of stop operation
    """
    state = strategy_manager.get_state()
    
    if state.status == StrategyStatus.STOPPED:
        return StrategyStopResponse(
            success=True,
            message="Strategy is already stopped",
            status=StrategyStatus.STOPPED
        )
    
    success = strategy_manager.stop()
    
    if success:
        return StrategyStopResponse(
            success=True,
            message="Strategy stopped successfully",
            status=StrategyStatus.STOPPED
        )
    else:
        return StrategyStopResponse(
            success=False,
            message="Failed to stop strategy",
            status=strategy_manager.get_state().status
        )


@router.post("/restart", response_model=StrategyStartResponse)
async def restart_strategy(request: StrategyStartRequest = None):
    """
    Restart the trading strategy.
    
    Args:
        request: Optional configuration overrides
        
    Returns:
        StrategyStartResponse: Result of restart operation
    """
    config_override = request.config_override if request else None
    success = strategy_manager.restart(config_override)
    
    if success:
        return StrategyStartResponse(
            success=True,
            message="Strategy restarted successfully",
            status=StrategyStatus.RUNNING
        )
    else:
        return StrategyStartResponse(
            success=False,
            message="Failed to restart strategy",
            status=strategy_manager.get_state().status
        )
