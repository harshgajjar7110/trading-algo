"""
Mock Broker for Testing

Provides a mock implementation of the BrokerGateway for unit testing.
Simulates broker responses without making real API calls.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

from brokers.core.schemas import (
    BrokerCapabilities,
    Funds,
    OrderRequest,
    OrderResponse,
    Position,
    Quote,
)
from brokers.core.enums import Exchange, OrderType, ProductType, TransactionType


class MockBrokerGateway:
    """
    Mock broker gateway for testing.

    Simulates broker behavior with configurable responses.
    No real API calls are made.
    """

    def __init__(self):
        self._positions: List[Position] = []
        self._orders: List[Dict[str, Any]] = []
        self._funds = Funds(
            equity=100000.0, available_cash=50000.0, used_margin=50000.0, net=100000.0
        )
        self._quote = Quote(
            symbol="NSE:NIFTY 50",
            exchange=Exchange.NSE,
            last_price=22500.0,
            bid=22499.0,
            ask=22501.0,
            volume=1000000,
        )
        self._capabilities = BrokerCapabilities()
        self._order_counter = 0
        self._should_fail = False
        self._failure_message = "Mock failure"

    def set_should_fail(self, fail: bool, message: str = "Mock failure"):
        """Configure mock to simulate failures."""
        self._should_fail = fail
        self._failure_message = message

    def add_position(self, position: Position):
        """Add a mock position."""
        self._positions.append(position)

    def clear_positions(self):
        """Clear all positions."""
        self._positions.clear()

    def add_order(self, order: Dict[str, Any]):
        """Add a mock order."""
        self._orders.append(order)

    def clear_orders(self):
        """Clear all orders."""
        self._orders.clear()

    def set_funds(self, funds: Funds):
        """Set mock funds."""
        self._funds = funds

    def set_quote(self, quote: Quote):
        """Set mock quote."""
        self._quote = quote

    # === BrokerGateway Interface ===

    def get_capabilities(self) -> BrokerCapabilities:
        """Get broker capabilities."""
        return self._capabilities

    def get_funds(self) -> Funds:
        """Get account funds."""
        if self._should_fail:
            raise Exception(self._failure_message)
        return self._funds

    def get_positions(self) -> List[Position]:
        """Get all positions."""
        if self._should_fail:
            raise Exception(self._failure_message)
        return self._positions.copy()

    def get_position(
        self, symbol: str, exchange: Optional[str] = None
    ) -> Optional[Position]:
        """Get specific position."""
        for pos in self._positions:
            if pos.symbol == symbol:
                if exchange is None or pos.exchange.value == exchange:
                    return pos
        return None

    def place_order(self, request: OrderRequest) -> OrderResponse:
        """Place an order."""
        if self._should_fail:
            return OrderResponse(
                status="error", order_id=None, message=self._failure_message
            )

        self._order_counter += 1
        order_id = f"ORDER{self._order_counter:04d}"

        # Add to orders list
        order = {
            "order_id": order_id,
            "symbol": request.symbol,
            "exchange": request.exchange.value,
            "transaction_type": request.transaction_type.value,
            "quantity": request.quantity,
            "price": request.price or 0,
            "status": "PENDING",
            "order_type": request.order_type.value,
            "product_type": request.product_type.value,
            "timestamp": datetime.now().isoformat(),
            "tag": request.tag,
        }
        self._orders.append(order)

        return OrderResponse(
            status="ok", order_id=order_id, message="Order placed successfully"
        )

    def cancel_order(self, order_id: str) -> OrderResponse:
        """Cancel an order."""
        if self._should_fail:
            return OrderResponse(
                status="error", order_id=None, message=self._failure_message
            )

        for order in self._orders:
            if order["order_id"] == order_id:
                order["status"] = "CANCELLED"
                return OrderResponse(
                    status="ok", order_id=order_id, message="Order cancelled"
                )

        return OrderResponse(status="error", order_id=None, message="Order not found")

    def modify_order(self, order_id: str, updates: Dict[str, Any]) -> OrderResponse:
        """Modify an order."""
        if self._should_fail:
            return OrderResponse(
                status="error", order_id=None, message=self._failure_message
            )

        for order in self._orders:
            if order["order_id"] == order_id:
                order.update(updates)
                return OrderResponse(
                    status="ok", order_id=order_id, message="Order modified"
                )

        return OrderResponse(status="error", order_id=None, message="Order not found")

    def get_orderbook(self) -> List[Dict[str, Any]]:
        """Get orderbook."""
        if self._should_fail:
            raise Exception(self._failure_message)
        return self._orders.copy()

    def get_tradebook(self) -> List[Dict[str, Any]]:
        """Get tradebook."""
        if self._should_fail:
            raise Exception(self._failure_message)
        # Return only completed orders as trades
        return [
            {
                "trade_id": f"TRADE{order['order_id']}",
                "order_id": order["order_id"],
                "symbol": order["symbol"],
                "exchange": order["exchange"],
                "transaction_type": order["transaction_type"],
                "quantity": order["quantity"],
                "price": order.get("average_price", order["price"]),
                "timestamp": order["timestamp"],
            }
            for order in self._orders
            if order["status"] == "COMPLETE"
        ]

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get specific order."""
        for order in self._orders:
            if order["order_id"] == order_id:
                return order.copy()
        return None

    def get_quote(self, symbol: str) -> Quote:
        """Get quote for symbol."""
        if self._should_fail:
            raise Exception(self._failure_message)
        return self._quote

    def get_quotes(self, symbols: List[str]) -> Dict[str, Quote]:
        """Get quotes for multiple symbols."""
        if self._should_fail:
            raise Exception(self._failure_message)
        return {symbol: self._quote for symbol in symbols}

    def get_history(
        self, symbol: str, interval: str, start: str, end: str
    ) -> List[Dict[str, Any]]:
        """Get historical data."""
        if self._should_fail:
            raise Exception(self._failure_message)
        # Return mock historical data
        return [
            {
                "timestamp": "2026-03-01T09:15:00",
                "open": 22400.0,
                "high": 22500.0,
                "low": 22350.0,
                "close": 22450.0,
                "volume": 100000,
            }
        ]

    def connect_websocket(self, **kwargs):
        """Mock WebSocket connection."""
        pass

    def symbols_to_subscribe(self, symbols: List[str]):
        """Mock subscribe."""
        pass

    def connect_order_websocket(self, **kwargs):
        """Mock order WebSocket."""
        pass

    def unsubscribe(self, symbols: List[str]):
        """Mock unsubscribe."""
        pass


def create_mock_position(
    symbol: str = "NIFTY26MAR22500CE",
    exchange: Exchange = Exchange.NFO,
    quantity: int = -50,
    average_price: float = 50.0,
    pnl: float = 1000.0,
    product_type: ProductType = ProductType.MARGIN,
) -> Position:
    """Helper to create a mock position."""
    return Position(
        symbol=symbol,
        exchange=exchange,
        quantity_total=quantity,
        quantity_available=quantity,
        average_price=average_price,
        pnl=pnl,
        product_type=product_type,
    )


def create_mock_order(
    order_id: str = "ORDER0001",
    symbol: str = "NIFTY26MAR22500CE",
    exchange: str = "NFO",
    transaction_type: str = "SELL",
    quantity: int = 50,
    price: float = 50.0,
    status: str = "PENDING",
    order_type: str = "LIMIT",
    product_type: str = "NRML",
    tag: Optional[str] = None,
) -> Dict[str, Any]:
    """Helper to create a mock order."""
    return {
        "order_id": order_id,
        "symbol": symbol,
        "exchange": exchange,
        "transaction_type": transaction_type,
        "quantity": quantity,
        "price": price,
        "status": status,
        "order_type": order_type,
        "product_type": product_type,
        "timestamp": datetime.now().isoformat(),
        "tag": tag,
    }
