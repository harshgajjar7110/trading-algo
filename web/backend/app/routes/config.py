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

    # ============================================================
    # ENHANCED CONFIGURATION VALIDATION
    # ============================================================

    # Validate position limits
    if config.max_positions_per_side < 1:
        errors.append("max_positions_per_side must be at least 1")
    if config.max_total_positions < config.max_positions_per_side:
        errors.append("max_total_positions must be >= max_positions_per_side")
    if config.max_consecutive_losses < 0:
        errors.append("max_consecutive_losses must be non-negative")

    # Validate stop-loss settings
    if config.sl_percentage < 10 or config.sl_percentage > 200:
        errors.append("sl_percentage should be between 10 and 200")
    if config.sl_limit_buffer < 0:
        errors.append("sl_limit_buffer must be non-negative")

    # Validate profit target
    if config.profit_target_enabled:
        if config.profit_target_percent < 10 or config.profit_target_percent > 100:
            errors.append("profit_target_percent should be between 10 and 100")

    # Validate trailing stop
    if config.trailing_stop_enabled:
        if config.trailing_stop_distance <= 0 or config.trailing_stop_distance > 1:
            errors.append("trailing_stop_distance should be between 0 and 1 (e.g., 0.5 = 50%)")

    # Validate daily loss limit
    if config.max_daily_loss_percent > 0:
        errors.append("max_daily_loss_percent should be negative (e.g., -3.0 for -3%)")
    if config.max_daily_loss_percent < -20:
        warnings.append("max_daily_loss_percent < -20% is very risky")

    # Validate time format for square_off_time
    if config.square_off_time:
        try:
            parts = config.square_off_time.split(':')
            if len(parts) != 2:
                errors.append("square_off_time must be in HH:MM format")
            else:
                hour, minute = int(parts[0]), int(parts[1])
                if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                    errors.append("square_off_time has invalid hour or minute")
        except ValueError:
            errors.append("square_off_time must be in HH:MM format")

    # Validate ATR multipliers
    if config.enable_dynamic_gaps:
        if config.atr_multiplier_pe <= 0:
            errors.append("atr_multiplier_pe must be positive when dynamic gaps are enabled")
        if config.atr_multiplier_ce <= 0:
            errors.append("atr_multiplier_ce must be positive when dynamic gaps are enabled")

    # Validate volatility sizing
    if config.volatility_sizing:
        if config.high_vol_size_reduction <= 0 or config.high_vol_size_reduction > 1:
            errors.append("high_vol_size_reduction should be between 0 and 1 (e.g., 0.5 = 50%)")

    # Enhanced warnings
    if config.sl_enabled and config.sl_percentage > 100:
        warnings.append("sl_percentage > 100% may cause immediate SL hits on volatile days")

    if config.max_positions_per_side > 5:
        warnings.append("max_positions_per_side > 5 increases risk exposure")

    if config.enable_dynamic_gaps and (config.atr_multiplier_pe < 1 or config.atr_multiplier_ce < 1):
        warnings.append("ATR multipliers < 1 may result in tighter gaps and more trades")

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
