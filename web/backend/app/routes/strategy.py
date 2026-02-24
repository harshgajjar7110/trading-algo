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
    
    # Pre-flight check: Validate broker authentication before starting
    try:
        from app.services.broker_service import broker_service
        broker = broker_service._ensure_broker()
        funds = broker.get_funds()
        
        # Check for auth errors
        if funds.raw and isinstance(funds.raw, dict):
            if funds.raw.get("error") == "unauthenticated" or "access_token" in str(funds.raw.get("error", "")).lower():
                return StrategyStartResponse(
                    success=False,
                    message="Authentication failed. Please go to the Auth page and login with your broker.",
                    status=StrategyStatus.STOPPED
                )
    except Exception as e:
        error_str = str(e)
        if "api_key" in error_str.lower() or "access_token" in error_str.lower() or "unauthenticated" in error_str.lower():
            return StrategyStartResponse(
                success=False,
                message="Authentication failed. Please go to the Auth page and login with your broker.",
                status=StrategyStatus.STOPPED
            )
        # Other errors might be temporary, log but continue
        print(f"[Strategy] Pre-flight broker check warning: {e}")
    
    config_override = request.config_override if request else None
    result = strategy_manager.start(config_override=config_override, confirmed=True)
    
    # Get updated state (might contain error message)
    updated_state = strategy_manager.get_state()
    
    if result.get("success"):
        return StrategyStartResponse(
            success=True,
            message=result.get("message", "Strategy started successfully"),
            status=StrategyStatus.RUNNING
        )
    else:
        # Include error message if available
        error_msg = result.get("message") or updated_state.error_message or "Failed to start strategy"
        return StrategyStartResponse(
            success=False,
            message=error_msg,
            status=updated_state.status
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
    
    result = strategy_manager.stop()
    
    if result.get("success"):
        return StrategyStopResponse(
            success=True,
            message=result.get("message", "Strategy stopped successfully"),
            status=StrategyStatus.STOPPED
        )
    else:
        return StrategyStopResponse(
            success=False,
            message=result.get("message", "Failed to stop strategy"),
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
    result = strategy_manager.restart(config_override=config_override, confirmed=True)
    
    if result.get("success"):
        return StrategyStartResponse(
            success=True,
            message=result.get("message", "Strategy restarted successfully"),
            status=StrategyStatus.RUNNING
        )
    else:
        return StrategyStartResponse(
            success=False,
            message=result.get("message", "Failed to restart strategy"),
            status=strategy_manager.get_state().status
        )
