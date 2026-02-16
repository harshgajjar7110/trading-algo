"""
Configuration API Routes

Endpoints for managing strategy configuration.
"""
from typing import Dict, Any
from fastapi import APIRouter, HTTPException

from app.config import StrategyConfig
from app.models.schemas import StrategyConfigUpdate, ConfigValidationResponse
from app.services.strategy_manager import strategy_manager


router = APIRouter(prefix="/config", tags=["Configuration"])


@router.get("", response_model=StrategyConfig)
async def get_configuration():
    """
    Get current strategy configuration.
    
    Returns:
        StrategyConfig: Current configuration values
    """
    return strategy_manager.get_config()


@router.put("", response_model=StrategyConfig)
async def update_configuration(updates: StrategyConfigUpdate):
    """
    Update strategy configuration.
    
    Args:
        updates: Partial configuration updates
        
    Returns:
        StrategyConfig: Updated configuration
    """
    # Convert to dict, excluding None values
    update_dict = {k: v for k, v in updates.model_dump().items() if v is not None}
    
    if not update_dict:
        return strategy_manager.get_config()
    
    updated_config = strategy_manager.update_config(update_dict)
    return updated_config


@router.post("/validate", response_model=ConfigValidationResponse)
async def validate_configuration(config: StrategyConfig):
    """
    Validate a configuration without saving it.
    
    Args:
        config: Configuration to validate
        
    Returns:
        ConfigValidationResponse: Validation result with errors and warnings
    """
    errors = []
    warnings = []
    
    # Validate symbol_initials
    if not config.symbol_initials or len(config.symbol_initials) < 9:
        errors.append("symbol_initials must be at least 9 characters (e.g., NIFTY26FEB)")
    
    # Validate gap parameters
    if config.pe_gap <= 0:
        errors.append("pe_gap must be positive")
    if config.ce_gap <= 0:
        errors.append("ce_gap must be positive")
        
    # Validate quantities
    if config.pe_quantity <= 0:
        errors.append("pe_quantity must be positive")
    if config.ce_quantity <= 0:
        errors.append("ce_quantity must be positive")
        
    # Validate symbol gaps
    if config.pe_symbol_gap <= 0:
        errors.append("pe_symbol_gap must be positive")
    if config.ce_symbol_gap <= 0:
        errors.append("ce_symbol_gap must be positive")
        
    # Validate risk parameters
    if config.min_price_to_sell <= 0:
        errors.append("min_price_to_sell must be positive")
    if config.sell_multiplier_threshold <= 0:
        errors.append("sell_multiplier_threshold must be positive")
    
    # Add warnings for potentially risky configurations
    if config.sell_multiplier_threshold > 5:
        warnings.append("sell_multiplier_threshold > 5 may result in large position sizes")
    
    if config.min_price_to_sell < 10:
        warnings.append("min_price_to_sell < 10 may result in trading illiquid options")
    
    if config.pe_gap < 20 or config.ce_gap < 20:
        warnings.append("Gap values < 20 may result in frequent trading")
    
    return ConfigValidationResponse(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings
    )


@router.post("/reset", response_model=StrategyConfig)
async def reset_configuration():
    """
    Reset configuration to defaults from YAML file.
    
    Returns:
        StrategyConfig: Default configuration
    """
    strategy_manager._load_config()
    return strategy_manager.get_config()
