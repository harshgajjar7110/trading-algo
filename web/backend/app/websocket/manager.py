"""
WebSocket Connection Manager

Manages WebSocket connections and broadcasts real-time data to clients.
"""
import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from fastapi import WebSocket


class ConnectionManager:
    """
    Manages WebSocket connections for real-time data streaming.
    
    Features:
    - Connection management (connect/disconnect)
    - Broadcasting to all clients
    - Symbol-based subscriptions
    - Heartbeat/ping-pong for connection health
    """
    
    _instance: Optional['ConnectionManager'] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        self._active_connections: List[WebSocket] = []
        self._subscriptions: Dict[str, Set[WebSocket]] = {}  # symbol -> connections
        self._connection_data: Dict[WebSocket, Set[str]] = {}  # connection -> subscribed symbols
        
    async def connect(self, websocket: WebSocket) -> None:
        """
        Accept a new WebSocket connection.
        
        Args:
            websocket: The WebSocket connection to accept
        """
        await websocket.accept()
        self._active_connections.append(websocket)
        self._connection_data[websocket] = set()
        
    def disconnect(self, websocket: WebSocket) -> None:
        """
        Remove a WebSocket connection.
        
        Args:
            websocket: The WebSocket connection to remove
        """
        if websocket in self._active_connections:
            self._active_connections.remove(websocket)
            
        # Clean up subscriptions
        if websocket in self._connection_data:
            for symbol in self._connection_data[websocket]:
                if symbol in self._subscriptions:
                    self._subscriptions[symbol].discard(websocket)
            del self._connection_data[websocket]
            
    async def send_personal_message(self, message: Dict[str, Any], websocket: WebSocket) -> None:
        """
        Send a message to a specific client.
        
        Args:
            message: The message to send
            websocket: The target WebSocket connection
        """
        try:
            await websocket.send_json(message)
        except Exception:
            self.disconnect(websocket)
            
    async def broadcast(self, message: Dict[str, Any]) -> None:
        """
        Broadcast a message to all connected clients.
        
        Args:
            message: The message to broadcast
        """
        message['timestamp'] = datetime.utcnow().isoformat()
        
        disconnected = []
        for connection in self._active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
                
        # Clean up disconnected clients
        for connection in disconnected:
            self.disconnect(connection)
            
    async def broadcast_to_subscribers(self, symbol: str, message: Dict[str, Any]) -> None:
        """
        Broadcast a message to clients subscribed to a specific symbol.
        
        Args:
            symbol: The symbol to broadcast to
            message: The message to broadcast
        """
        if symbol not in self._subscriptions:
            return
            
        message['timestamp'] = datetime.utcnow().isoformat()
        
        disconnected = []
        for connection in self._subscriptions[symbol]:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
                
        # Clean up disconnected clients
        for connection in disconnected:
            self.disconnect(connection)
            
    def subscribe(self, websocket: WebSocket, symbols: List[str]) -> None:
        """
        Subscribe a connection to specific symbols.
        
        Args:
            websocket: The WebSocket connection
            symbols: List of symbols to subscribe to
        """
        if websocket not in self._connection_data:
            return
            
        for symbol in symbols:
            if symbol not in self._subscriptions:
                self._subscriptions[symbol] = set()
            self._subscriptions[symbol].add(websocket)
            self._connection_data[websocket].add(symbol)
            
    def unsubscribe(self, websocket: WebSocket, symbols: List[str]) -> None:
        """
        Unsubscribe a connection from specific symbols.
        
        Args:
            websocket: The WebSocket connection
            symbols: List of symbols to unsubscribe from
        """
        if websocket not in self._connection_data:
            return
            
        for symbol in symbols:
            if symbol in self._subscriptions:
                self._subscriptions[symbol].discard(websocket)
            self._connection_data[websocket].discard(symbol)
            
    def get_connection_count(self) -> int:
        """Get the number of active connections."""
        return len(self._active_connections)
    
    def get_subscribers(self, symbol: str) -> int:
        """Get the number of subscribers for a symbol."""
        return len(self._subscriptions.get(symbol, set()))


# Global connection manager instance
connection_manager = ConnectionManager()


# =============================================================================
# Message Builders
# =============================================================================

def create_price_update(symbol: str, last_price: float, change: Optional[float] = None) -> Dict[str, Any]:
    """Create a price update message."""
    return {
        'type': 'PRICE_UPDATE',
        'data': {
            'symbol': symbol,
            'last_price': last_price,
            'change': change
        }
    }


def create_strategy_state_update(state: Dict[str, Any]) -> Dict[str, Any]:
    """Create a strategy state update message."""
    return {
        'type': 'STRATEGY_STATE',
        'data': state
    }


def create_order_update(order: Dict[str, Any]) -> Dict[str, Any]:
    """Create an order update message."""
    return {
        'type': 'ORDER_UPDATE',
        'data': order
    }


def create_position_update(position: Dict[str, Any]) -> Dict[str, Any]:
    """Create a position update message."""
    return {
        'type': 'POSITION_UPDATE',
        'data': position
    }


def create_error_message(code: str, message: str) -> Dict[str, Any]:
    """Create an error message."""
    return {
        'type': 'ERROR',
        'data': {
            'code': code,
            'message': message
        }
    }


def create_heartbeat() -> Dict[str, Any]:
    """Create a heartbeat message."""
    return {
        'type': 'HEARTBEAT',
        'data': {
            'time': datetime.utcnow().isoformat()
        }
    }
