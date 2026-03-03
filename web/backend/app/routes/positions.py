"""
Positions and Orders API Routes

Endpoints for managing positions and orders.
Fetches live data from broker when strategy is running.
"""

from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    PositionsResponse,
    OrdersResponse,
    TradesResponse,
    Position,
    Order,
    OrderStatus,
    TransactionType,
)
from app.services.live_data_manager import live_data_manager
from app.services.broker_service import broker_service


router = APIRouter(tags=["Trading"])


# =============================================================================
# Positions
# =============================================================================


@router.get("/positions", response_model=PositionsResponse)
async def get_positions():
    """
    Get all current positions from live data stream.

    Returns live positions when strategy is running.
    Returns empty list with message when strategy is stopped.

    Returns:
        PositionsResponse: List of positions with total PnL
    """
    if not live_data_manager.is_streaming:
        # Strategy not running - return empty with message
        return PositionsResponse(
            positions=[],
            total_pnl=0.0,
            total_pnl_percent=0.0,
            message="Strategy not running - no live data available. Start the strategy to see positions.",
        )

    # Get live positions from stream
    positions_data = live_data_manager.get_positions()

    if not positions_data:
        return PositionsResponse(
            positions=[],
            total_pnl=0.0,
            total_pnl_percent=0.0,
            message="No positions found",
        )

    positions = []
    total_pnl = 0.0

    for pos_data in positions_data:
        # Calculate PnL percentage
        pnl = pos_data.get("pnl", 0.0)
        pnl_percent = 0.0
        avg_price = pos_data.get("average_price", 0)
        qty = pos_data.get("quantity", 0)

        if avg_price and avg_price > 0 and qty != 0:
            position_value = avg_price * abs(qty)
            if position_value > 0:
                pnl_percent = (pnl / position_value) * 100

        position = Position(
            symbol=pos_data.get("symbol", ""),
            exchange=pos_data.get("exchange", "NFO"),
            quantity=qty,
            average_price=avg_price,
            current_price=None,  # Will be updated via quotes
            pnl=pnl,
            pnl_percent=pnl_percent,
            product_type=pos_data.get("product_type", "NRML"),
            side="SHORT" if qty < 0 else "LONG",
        )
        positions.append(position)
        total_pnl += pnl

    return PositionsResponse(
        positions=positions,
        total_pnl=total_pnl,
        total_pnl_percent=0.0,  # Would need account value
    )


@router.post("/positions/refresh")
async def refresh_positions():
    """
    Force refresh positions from broker.

    Returns:
        dict: Refresh status
    """
    success = await live_data_manager.force_refresh()

    if success:
        return {
            "status": "success",
            "message": "Positions refreshed successfully",
            "timestamp": live_data_manager.last_update.isoformat()
            if live_data_manager.last_update
            else None,
        }
    else:
        raise HTTPException(
            status_code=503,
            detail="Failed to refresh positions. Strategy may not be running.",
        )


# =============================================================================
# Orders
# =============================================================================


@router.get("/orders", response_model=OrdersResponse)
async def get_orders():
    """
    Get all orders from live data stream.

    Returns live orders when strategy is running.
    Returns empty list with message when strategy is stopped.

    Returns:
        OrdersResponse: List of orders
    """
    if not live_data_manager.is_streaming:
        # Strategy not running - return empty with message
        return OrdersResponse(
            orders=[],
            count=0,
            message="Strategy not running - no live data available. Start the strategy to see orders.",
        )

    # Get live orders from stream
    orders_data = live_data_manager.get_orders()

    if not orders_data:
        return OrdersResponse(orders=[], count=0, message="No orders found")

    orders = []
    for order_data in orders_data:
        # Map status
        status_str = order_data.get("status", "PENDING").upper()
        try:
            status = OrderStatus(status_str)
        except ValueError:
            status = OrderStatus.PENDING

        # Map transaction type
        txn_type_str = order_data.get("transaction_type", "BUY").upper()
        try:
            txn_type = TransactionType(txn_type_str)
        except ValueError:
            txn_type = TransactionType.BUY

        order = Order(
            order_id=order_data.get("order_id", ""),
            symbol=order_data.get("symbol", ""),
            exchange=order_data.get("exchange", "NFO"),
            transaction_type=txn_type,
            quantity=order_data.get("quantity", 0),
            price=order_data.get("price", 0),
            status=status,
            order_type=order_data.get("order_type", "MARKET"),
            product_type=order_data.get("product_type", "NRML"),
            timestamp=order_data.get("timestamp", ""),
            tag=order_data.get("tag"),
        )
        orders.append(order)

    return OrdersResponse(orders=orders, count=len(orders))


@router.get("/trades", response_model=TradesResponse)
async def get_trades():
    """
    Get all executed trades.

    Note: Trades are fetched directly from broker (not from live stream)
    as they represent historical executed trades.

    Returns:
        TradesResponse: List of trades
    """
    return broker_service.get_trades()
