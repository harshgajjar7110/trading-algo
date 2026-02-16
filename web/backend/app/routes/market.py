"""
Market Data API Routes

Endpoints for market data and quotes.
"""
from fastapi import APIRouter, HTTPException

from app.models.schemas import Quote, NiftyData, Funds
from app.services.broker_service import broker_service
from app.services.strategy_manager import strategy_manager


router = APIRouter(prefix="/market", tags=["Market Data"])


@router.get("/quote/{symbol:path}", response_model=Quote)
async def get_quote(symbol: str):
    """
    Get quote for a symbol.
    
    Args:
        symbol: Trading symbol (e.g., NSE:NIFTY 50, NIFTY26FEB24300PE)
        
    Returns:
        Quote: Current market quote
    """
    return broker_service.get_quote(symbol)


@router.get("/nifty", response_model=NiftyData)
async def get_nifty_data():
    """
    Get NIFTY index data with strategy context.
    
    Returns:
        NiftyData: NIFTY quote with PE/CE reference values and gap triggers
    """
    state = strategy_manager.get_state()
    config = strategy_manager.get_config()
    
    return broker_service.get_nifty_data(
        pe_reference=state.nifty_pe_last_value,
        ce_reference=state.nifty_ce_last_value,
        pe_gap=config.pe_gap,
        ce_gap=config.ce_gap
    )


# =============================================================================
# Account
# =============================================================================

@router.get("/funds", response_model=Funds)
async def get_funds():
    """
    Get account funds and margin.
    
    Returns:
        Funds: Account equity, available cash, and margin
    """
    return broker_service.get_funds()
