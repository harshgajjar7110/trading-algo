"""
Broker Service

Wrapper service for broker operations.
Provides a clean interface for API routes to interact with the broker.
"""
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add project root to Python path for imports
# Path structure: web/backend/app/services/ -> need to go up 4 levels to project root
_project_root = Path(__file__).resolve().parents[4]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from app.models.schemas import (
    Position, PositionsResponse,
    Order, OrdersResponse,
    Trade, TradesResponse,
    Quote, NiftyData,
    Funds, OrderStatus, TransactionType
)


class BrokerService:
    """
    Service for broker operations.
    
    Provides methods to:
    - Get positions
    - Get orders and trades
    - Get market quotes
    - Get account funds
    """
    
    _instance: Optional['BrokerService'] = None
    _broker: Optional[Any] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = False  # Will be initialized on first use
    
    def _ensure_broker(self) -> Any:
        """Ensure broker is initialized."""
        if self._broker is None:
            from brokers import BrokerGateway
            self._broker = BrokerGateway.from_name(os.getenv("BROKER_NAME"))
        return self._broker
    
    def reinitialize(self) -> None:
        """Re-initialize the broker with current credentials."""
        # Clear the cached broker instance
        BrokerService._broker = None
        self._broker = None
        print("[BrokerService] Broker reinitialized - will create new instance on next use")
    
    def get_positions(self) -> PositionsResponse:
        """Get all current positions."""
        try:
            broker = self._ensure_broker()
            positions_data = broker.get_positions()
            positions = []
            total_pnl = 0.0
            
            # Check if broker is authenticated (driver returns empty list if not)
            # We can detect this by checking if we got an empty list AND funds also fail
            if not positions_data:
                try:
                    funds = broker.get_funds()
                    if funds.raw and (funds.raw.get("error") or funds.net == 0):
                        # Likely authentication error
                        return PositionsResponse(
                            positions=[],
                            total_pnl=0.0,
                            total_pnl_percent=0.0,
                            error="Not authenticated with broker. Please go to Auth page and login."
                        )
                except Exception:
                    pass
            
            for pos in positions_data:
                # Get PnL from broker (already calculated correctly)
                pnl = pos.pnl if hasattr(pos, 'pnl') else 0.0
                
                # Calculate PnL percentage based on position value
                pnl_percent = 0.0
                if pos.average_price and pos.average_price > 0 and pos.quantity_total != 0:
                    # For options selling, margin is typically 1.5-2x the premium received
                    # PnL % = PnL / (Average Price * |Quantity|) * 100
                    position_value = pos.average_price * abs(pos.quantity_total)
                    if position_value > 0:
                        pnl_percent = (pnl / position_value) * 100
                
                position = Position(
                    symbol=pos.symbol,
                    exchange=pos.exchange.value if hasattr(pos.exchange, 'value') else str(pos.exchange),
                    quantity=pos.quantity_total,
                    average_price=pos.average_price,
                    current_price=None,  # Will be updated via quotes
                    pnl=pnl,
                    pnl_percent=pnl_percent,
                    product_type=pos.product_type.value if hasattr(pos.product_type, 'value') else str(pos.product_type),
                    side="SHORT" if pos.quantity_total < 0 else "LONG"
                )
                positions.append(position)
                total_pnl += pnl
            
            total_pnl_percent = 0.0  # Would need account value to calculate
            
            return PositionsResponse(
                positions=positions,
                total_pnl=total_pnl,
                total_pnl_percent=total_pnl_percent
            )
            
        except Exception as e:
            error_msg = str(e)
            print(f"Error getting positions: {error_msg}")
            # Check for common auth errors
            if "api_key" in error_msg.lower() or "access_token" in error_msg.lower() or "unauthenticated" in error_msg.lower():
                error_msg = "Authentication failed. Please go to Auth page and re-authenticate."
            return PositionsResponse(
                positions=[],
                total_pnl=0.0,
                total_pnl_percent=0.0,
                error=error_msg
            )
    
    def get_orders(self) -> OrdersResponse:
        """Get all orders."""
        broker = self._ensure_broker()
        
        try:
            orders_data = broker.get_orderbook()
            orders = []
            
            for order in orders_data:
                # Map status
                status_str = order.get('status', 'PENDING').upper()
                try:
                    status = OrderStatus(status_str)
                except ValueError:
                    status = OrderStatus.PENDING
                
                # Map transaction type
                txn_type_str = order.get('transaction_type', 'BUY').upper()
                try:
                    txn_type = TransactionType(txn_type_str)
                except ValueError:
                    txn_type = TransactionType.BUY
                
                order_obj = Order(
                    order_id=str(order.get('order_id', order.get('id', ''))),
                    symbol=order.get('symbol', ''),
                    exchange=order.get('exchange', 'NFO'),
                    transaction_type=txn_type,
                    quantity=order.get('quantity', 0),
                    price=order.get('price', 0),
                    status=status,
                    order_type=order.get('order_type', 'MARKET'),
                    product_type=order.get('product', 'NRML'),
                    timestamp=order.get('order_timestamp', order.get('timestamp', '')),
                    tag=order.get('tag'),
                    filled_quantity=order.get('filled_quantity'),
                    average_price=order.get('average_price')
                )
                orders.append(order_obj)
            
            return OrdersResponse(
                orders=orders,
                count=len(orders)
            )
            
        except Exception as e:
            return OrdersResponse(orders=[], count=0)
    
    def get_trades(self) -> TradesResponse:
        """Get all trades."""
        broker = self._ensure_broker()
        
        try:
            trades_data = broker.get_tradebook()
            trades = []
            
            for trade in trades_data:
                txn_type_str = trade.get('transaction_type', 'BUY').upper()
                try:
                    txn_type = TransactionType(txn_type_str)
                except ValueError:
                    txn_type = TransactionType.BUY
                
                trade_obj = Trade(
                    trade_id=str(trade.get('trade_id', '')),
                    order_id=str(trade.get('order_id', '')),
                    symbol=trade.get('symbol', ''),
                    exchange=trade.get('exchange', 'NFO'),
                    transaction_type=txn_type,
                    quantity=trade.get('quantity', 0),
                    price=trade.get('price', 0),
                    timestamp=trade.get('timestamp', ''),
                    tag=trade.get('tag')
                )
                trades.append(trade_obj)
            
            return TradesResponse(
                trades=trades,
                count=len(trades)
            )
            
        except Exception as e:
            return TradesResponse(trades=[], count=0)
    
    def get_quote(self, symbol: str) -> Quote:
        """Get quote for a symbol."""
        broker = self._ensure_broker()
        
        try:
            quote_data = broker.get_quote(symbol)
            
            # Calculate change
            change = None
            change_percent = None
            if hasattr(quote_data, 'raw') and quote_data.raw:
                close = quote_data.raw.get('ohlc', {}).get('close', 0)
                if close > 0:
                    change = quote_data.last_price - close
                    change_percent = (change / close) * 100
            
            return Quote(
                symbol=quote_data.symbol,
                exchange=quote_data.exchange.value if hasattr(quote_data.exchange, 'value') else str(quote_data.exchange),
                last_price=quote_data.last_price,
                change=change,
                change_percent=change_percent,
                bid=quote_data.bid,
                ask=quote_data.ask,
                volume=quote_data.volume,
                timestamp=quote_data.timestamp
            )
            
        except Exception as e:
            return Quote(
                symbol=symbol,
                exchange='NSE',
                last_price=0.0
            )
    
    def get_nifty_data(self, pe_reference: Optional[float] = None, ce_reference: Optional[float] = None,
                       pe_gap: Optional[int] = None, ce_gap: Optional[int] = None) -> NiftyData:
        """Get NIFTY index data with strategy context."""
        quote = self.get_quote("NSE:NIFTY 50")
        
        pe_gap_to_trigger = None
        ce_gap_to_trigger = None
        
        if pe_reference is not None and quote.last_price > 0:
            pe_gap_to_trigger = pe_reference + pe_gap - quote.last_price if pe_gap else None
            
        if ce_reference is not None and quote.last_price > 0:
            ce_gap_to_trigger = ce_reference - ce_gap - quote.last_price if ce_gap else None
        
        return NiftyData(
            quote=quote,
            pe_reference=pe_reference,
            ce_reference=ce_reference,
            pe_gap_to_trigger=pe_gap_to_trigger,
            ce_gap_to_trigger=ce_gap_to_trigger
        )
    
    def get_funds(self) -> Funds:
        """Get account funds."""
        broker = self._ensure_broker()
        
        try:
            funds_data = broker.get_funds()
            
            return Funds(
                equity=funds_data.equity,
                available_cash=funds_data.available_cash,
                used_margin=funds_data.used_margin,
                net=funds_data.net
            )
            
        except Exception as e:
            return Funds(
                equity=0.0,
                available_cash=0.0,
                used_margin=0.0,
                net=0.0
            )


# Global broker service instance
broker_service = BrokerService()
