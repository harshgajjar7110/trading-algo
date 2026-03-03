"""
Live Data Manager Service

Manages live streaming of positions and orders from broker.
All data is kept in-memory only - no persistence to disk.
When strategy is running, maintains continuous stream from broker.
When strategy stops, all data is cleared.
"""

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to Python path for imports
_project_root = Path(__file__).resolve().parents[4]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from app.websocket.manager import connection_manager


class LiveDataManager:
    """
    Manages live data streaming from broker.

    Key Features:
    - In-memory only (no persistence)
    - Continuous stream when strategy is running
    - Real-time updates via WebSocket
    - Empty data when strategy stopped
    """

    _instance: Optional["LiveDataManager"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._positions: Dict[str, Dict[str, Any]] = {}  # symbol -> position
        self._orders: Dict[str, Dict[str, Any]] = {}  # order_id -> order
        self._is_streaming = False
        self._stream_task: Optional[asyncio.Task] = None
        self._broker: Optional[Any] = None
        self._lock = asyncio.Lock()
        self._last_update: Optional[datetime] = None
        self._error_count = 0
        self._max_errors = 5  # Stop streaming after this many consecutive errors

    def _ensure_broker(self) -> Any:
        """Ensure broker is initialized."""
        if self._broker is None:
            from brokers import BrokerGateway

            self._broker = BrokerGateway.from_name(os.getenv("BROKER_NAME", "zerodha"))
        return self._broker

    @property
    def is_streaming(self) -> bool:
        """Check if live streaming is active."""
        return self._is_streaming

    @property
    def last_update(self) -> Optional[datetime]:
        """Get timestamp of last successful update."""
        return self._last_update

    async def start_stream(self) -> bool:
        """
        Start continuous data stream from broker.

        Returns:
            bool: True if started successfully, False otherwise
        """
        if self._is_streaming:
            print("[LiveDataManager] Stream already active")
            return True

        try:
            # Initialize broker connection
            self._ensure_broker()

            # Clear any existing data
            async with self._lock:
                self._positions.clear()
                self._orders.clear()
                self._error_count = 0

            self._is_streaming = True

            # Start background stream task
            self._stream_task = asyncio.create_task(self._stream_loop())

            print("[LiveDataManager] Live data stream started")
            return True

        except Exception as e:
            print(f"[LiveDataManager] Failed to start stream: {e}")
            self._is_streaming = False
            return False

    async def stop_stream(self) -> None:
        """Stop streaming and clear all data."""
        if not self._is_streaming:
            return

        self._is_streaming = False

        # Cancel stream task
        if self._stream_task:
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
            self._stream_task = None

        # Clear all data
        async with self._lock:
            self._positions.clear()
            self._orders.clear()
            self._last_update = None
            self._error_count = 0

        print("[LiveDataManager] Live data stream stopped and cleared")

    async def _stream_loop(self) -> None:
        """
        Background loop that fetches live data from broker.
        Runs continuously while _is_streaming is True.
        """
        while self._is_streaming:
            try:
                await self._fetch_and_update()
                self._error_count = 0  # Reset error count on success
                await asyncio.sleep(1)  # Update every second

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._error_count += 1
                print(
                    f"[LiveDataManager] Stream error ({self._error_count}/{self._max_errors}): {e}"
                )

                if self._error_count >= self._max_errors:
                    print("[LiveDataManager] Too many errors, stopping stream")
                    self._is_streaming = False
                    break

                await asyncio.sleep(2)  # Wait longer on error

    async def _fetch_and_update(self) -> None:
        """Fetch fresh data from broker and update in-memory storage."""
        try:
            broker = self._ensure_broker()

            # Fetch positions
            positions_data = broker.get_positions()
            positions_dict = {}
            for pos in positions_data:
                symbol = pos.symbol
                positions_dict[symbol] = {
                    "symbol": pos.symbol,
                    "exchange": pos.exchange.value
                    if hasattr(pos.exchange, "value")
                    else str(pos.exchange),
                    "quantity": pos.quantity_total,
                    "average_price": pos.average_price,
                    "pnl": pos.pnl if hasattr(pos, "pnl") else 0.0,
                    "product_type": pos.product_type.value
                    if hasattr(pos.product_type, "value")
                    else str(pos.product_type),
                }

            # Fetch orders
            orders_data = broker.get_orderbook()
            orders_dict = {}
            for order in orders_data:
                order_id = str(order.get("order_id", order.get("id", "")))
                if order_id:
                    orders_dict[order_id] = {
                        "order_id": order_id,
                        "symbol": order.get("symbol", ""),
                        "exchange": order.get("exchange", "NFO"),
                        "transaction_type": order.get("transaction_type", "BUY"),
                        "quantity": order.get("quantity", 0),
                        "price": order.get("price", 0),
                        "status": order.get("status", "PENDING"),
                        "order_type": order.get("order_type", "MARKET"),
                        "product_type": order.get("product", "NRML"),
                        "timestamp": order.get(
                            "order_timestamp", order.get("timestamp", "")
                        ),
                        "tag": order.get("tag"),
                    }

            # Update storage
            async with self._lock:
                self._positions = positions_dict
                self._orders = orders_dict
                self._last_update = datetime.now()

            # Broadcast updates via WebSocket
            await self._broadcast_updates()

        except Exception as e:
            raise Exception(f"Failed to fetch data: {e}") from e

    async def _broadcast_updates(self) -> None:
        """Broadcast position and order updates to connected WebSocket clients."""
        try:
            # Broadcast positions update
            if self._positions:
                await connection_manager.broadcast(
                    {
                        "type": "POSITIONS_UPDATE",
                        "data": {
                            "positions": list(self._positions.values()),
                            "timestamp": self._last_update.isoformat()
                            if self._last_update
                            else None,
                        },
                    }
                )

            # Broadcast orders update
            if self._orders:
                await connection_manager.broadcast(
                    {
                        "type": "ORDERS_UPDATE",
                        "data": {
                            "orders": list(self._orders.values()),
                            "timestamp": self._last_update.isoformat()
                            if self._last_update
                            else None,
                        },
                    }
                )
        except Exception as e:
            # Don't let broadcast errors stop the stream
            print(f"[LiveDataManager] Broadcast error: {e}")

    def get_positions(self) -> List[Dict[str, Any]]:
        """
        Get current positions.

        Returns:
            List of positions (empty if not streaming)
        """
        if not self._is_streaming:
            return []
        return list(self._positions.values())

    def get_orders(self) -> List[Dict[str, Any]]:
        """
        Get current orders.

        Returns:
            List of orders (empty if not streaming)
        """
        if not self._is_streaming:
            return []
        return list(self._orders.values())

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get specific position by symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Position dict or None
        """
        if not self._is_streaming:
            return None
        return self._positions.get(symbol)

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """
        Get specific order by ID.

        Args:
            order_id: Order ID

        Returns:
            Order dict or None
        """
        if not self._is_streaming:
            return None
        return self._orders.get(order_id)

    async def force_refresh(self) -> bool:
        """
        Force immediate refresh from broker.

        Returns:
            bool: True if refresh successful
        """
        if not self._is_streaming:
            return False

        try:
            await self._fetch_and_update()
            return True
        except Exception as e:
            print(f"[LiveDataManager] Force refresh failed: {e}")
            return False

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of live data.

        Returns:
            Dict with streaming status and data counts
        """
        return {
            "is_streaming": self._is_streaming,
            "positions_count": len(self._positions) if self._is_streaming else 0,
            "orders_count": len(self._orders) if self._is_streaming else 0,
            "last_update": self._last_update.isoformat() if self._last_update else None,
            "error_count": self._error_count,
        }


# Global live data manager instance
live_data_manager = LiveDataManager()
