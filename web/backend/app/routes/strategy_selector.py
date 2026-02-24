"""
Strategy Selector API Routes

Provides endpoints for:
- Listing available strategies
- Previewing strategy configuration
- Starting strategies with confirmation flow
- Tracking which strategy is running
"""
from typing import Dict, Optional, Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services.strategy_manager import strategy_manager
from app.services.strategy_registry import StrategyRegistry

router = APIRouter(prefix="/strategy", tags=["strategy-selector"])


# =============================================================================
# Request/Response Models
# =============================================================================

class StrategyStartRequest(BaseModel):
    """Request to start a strategy"""
    strategy_id: str
    config_override: Optional[Dict[str, Any]] = None
    confirmed: bool = False  # Whether user confirmed the config


class StrategyConfigPreviewRequest(BaseModel):
    """Request to preview strategy configuration"""
    strategy_id: str
    config_override: Optional[Dict[str, Any]] = None


class StrategyConfigUpdateRequest(BaseModel):
    """Request to update strategy configuration"""
    config: Dict[str, Any]


# =============================================================================
# API Endpoints
# =============================================================================

@router.get("/available")
async def get_available_strategies():
    """
    Get list of available strategies with their metadata.
    
    Returns strategies with risk levels, recommended capital, and descriptions
    to help users choose which strategy to run.
    """
    return strategy_manager.get_available_strategies()


@router.get("/{strategy_id}/details")
async def get_strategy_details(strategy_id: str):
    """
    Get detailed information about a specific strategy.
    
    Includes full description, default parameters, and current configuration.
    """
    details = strategy_manager.get_strategy_details(strategy_id)
    if not details:
        raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_id}")
    return details


@router.post("/preview-config")
async def preview_strategy_config(request: StrategyConfigPreviewRequest):
    """
    Preview the configuration that will be used when starting a strategy.
    
    This allows users to see:
    - Final merged configuration
    - Validation errors (if any)
    - Differences from default parameters
    - Risk level and capital requirements
    
    Use this before starting to confirm parameters.
    """
    preview = strategy_manager.preview_strategy_config(
        request.strategy_id,
        request.config_override
    )
    
    if "error" in preview:
        raise HTTPException(status_code=400, detail=preview["error"])
    
    return preview


@router.post("/start")
async def start_strategy(request: StrategyStartRequest):
    """
    Start a trading strategy.
    
    If 'confirmed' is False, returns a preview of the configuration
    and requires user confirmation before actually starting.
    
    If 'confirmed' is True, starts the strategy with the given configuration.
    """
    result = strategy_manager.start(
        strategy_id=request.strategy_id,
        config_override=request.config_override,
        confirmed=request.confirmed
    )
    
    if result.get("requires_confirmation"):
        # Return 200 with confirmation required flag
        return result
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("message"))
    
    return result


@router.post("/stop")
async def stop_strategy():
    """Stop the currently running strategy"""
    result = strategy_manager.stop()
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("message"))
    
    return result


@router.post("/restart")
async def restart_strategy(request: StrategyStartRequest):
    """Restart the strategy (stop then start)"""
    result = strategy_manager.restart(
        strategy_id=request.strategy_id,
        config_override=request.config_override,
        confirmed=request.confirmed
    )
    
    if result.get("requires_confirmation"):
        return result
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("message"))
    
    return result


@router.get("/current")
async def get_current_strategy():
    """
    Get information about the currently running strategy.
    
    Returns None if no strategy is running.
    """
    info = strategy_manager.get_current_strategy_info()
    
    if not info:
        return {
            "running": False,
            "message": "No strategy is currently running"
        }
    
    return {
        "running": True,
        **info
    }


@router.get("/compare")
async def compare_strategies():
    """
    Get comparison data for all strategies.
    
    Useful for strategy selection UI showing side-by-side comparison.
    """
    registry = StrategyRegistry()
    return registry.get_strategy_comparison()


@router.get("/validate-config")
async def validate_strategy_config(
    strategy_id: str,
    config: str  # JSON string of config
):
    """
    Validate a configuration without starting the strategy.
    
    Returns list of validation errors (empty if valid).
    """
    import json
    
    try:
        config_dict = json.loads(config)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in config parameter")
    
    registry = StrategyRegistry()
    errors = registry.validate_config(strategy_id, config_dict)
    
    return {
        "strategy_id": strategy_id,
        "is_valid": len(errors) == 0,
        "errors": errors
    }


@router.get("/status")
async def get_strategy_status():
    """
    Get current strategy status including which strategy is running.
    
    This is the main endpoint for the dashboard to poll for status.
    """
    state = strategy_manager.get_state()
    current = strategy_manager.get_current_strategy_info()
    
    return {
        "state": state,
        "current_strategy": current,
        "available_strategies": StrategyRegistry().get_strategy_names()
    }
