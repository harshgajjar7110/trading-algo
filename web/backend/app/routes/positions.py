"""
Positions and Orders API Routes

Endpoints for managing positions and orders.
"""
from fastapi import APIRouter, HTTPException

from app.models.schemas import PositionsResponse, OrdersResponse, TradesResponse
from app.services.broker_service import broker_service


router = APIRouter(tags=["Trading"])


# =============================================================================
# Positions
# =============================================================================

@router.get("/positions", response_model=PositionsResponse)
async def get_positions():
    """
    Get all current positions.
    
    Returns:
        PositionsResponse: List of positions with total PnL
    """
    return broker_service.get_positions()


# =============================================================================
# Orders
# =============================================================================

@router.get("/orders", response_model=OrdersResponse)
async def get_orders():
    """
    Get all orders.
    
    Returns:
        OrdersResponse: List of orders
    """
    return broker_service.get_orders()


@router.get("/trades", response_model=TradesResponse)
async def get_trades():
    """
    Get all executed trades.
    
    Returns:
        TradesResponse: List of trades
    """
    return broker_service.get_trades()
