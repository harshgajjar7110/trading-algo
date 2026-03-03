"""
Position Manager - SL Order Reconciliation for Survivor Strategy

This module handles:
1. Detection of open positions from broker at algo startup
2. SL order placement for unprotected positions
3. Position-SL mapping (in-memory only, no file persistence)
4. Reconciliation logic for Survivor Strategy (Zerodha only)

CRITICAL: Positions are always fetched fresh from broker - NO state file persistence.
Saving state to files creates stale positions and causes false entries/charges.

Assumptions (per requirements):
- Survivor strategy only (enhanced version)
- Algo-based orders only (no manual orders tracked)
- Zerodha is the only broker
- No existing SL orders at reconciliation (simplified)
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

# Use unified logger (with backward compatibility)
try:
    from utils.logging import get_strategy_logger
    logger = get_strategy_logger("position_manager")
except ImportError:
    # Fallback to legacy logger for backward compatibility
    from logger import strategy_logger as logger

from brokers import BrokerGateway, OrderRequest, Exchange, OrderType, TransactionType, ProductType
from brokers.core.schemas import Position, OrderResponse


@dataclass
class PositionState:
    """
    Tracks a position and its associated SL order.
    
    This is used for in-memory tracking only - NEVER persisted to disk.
    Positions are always fetched fresh from broker to avoid stale data.
    """
    symbol: str                           # Trading symbol e.g., NIFTY24FEB18000CE
    exchange: str                         # Exchange (NFO)
    quantity: int                         # Position quantity (negative for short)
    average_price: float                  # Average entry price (renamed from entry_price)
    product_type: str                     # NRML/MIS
    sl_order_id: Optional[str] = None    # Linked SL order ID
    sl_trigger_price: float = 0.0         # SL trigger price
    sl_limit_price: float = 0.0           # SL limit price
    sl_percentage: float = 60.0           # SL percentage used
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())
    position_id: str = ""                 # Unique identifier
    
    def __post_init__(self):
        """Generate position_id if not provided"""
        if not self.position_id:
            self.position_id = f"{self.symbol}_{self.exchange}_{self.product_type}"


@dataclass
class SLReconciliationResult:
    """Result of SL reconciliation at startup"""
    total_positions: int = 0
    protected_positions: int = 0          # Already have SL orders
    new_sl_placed: int = 0                # New SL orders placed
    sl_orders_placed: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    skipped_positions: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return {
            'total_positions': self.total_positions,
            'protected_positions': self.protected_positions,
            'new_sl_placed': self.new_sl_placed,
            'errors': self.errors,
            'skipped_positions': self.skipped_positions,
            'sl_orders_placed': self.sl_orders_placed
        }


class PositionManager:
    """
    Manages position tracking and SL order reconciliation for Survivor Strategy.
    
    Responsibilities:
    - Track open positions with SL order mapping (in-memory only)
    - Reconcile broker positions with local state at startup
    - Place missing SL orders for unprotected positions
    - NEVER persist position state to disk (prevents false entries)
    
    CRITICAL: Positions are always fetched fresh from broker. No state files.
    
    NOTE: This implementation is specific to:
    - Survivor Strategy (enhanced version)
    - Zerodha broker only
    - Algo-placed orders only (manual orders not tracked)
    """
    
    def __init__(
        self,
        broker: BrokerGateway,
        config: Dict[str, Any]
    ):
        """
        Initialize PositionManager.
        
        Args:
            broker: BrokerGateway instance for order operations
            config: Strategy configuration dict
        """
        self.broker = broker
        self._position_states: Dict[str, PositionState] = {}
        
        # Extract SL settings from config
        # NOTE: PositionManager is only created when sl_enabled=True (checked in survivor_enhanced.py)
        # So we force it to True here to avoid config parsing issues
        self.sl_enabled = True
        self.sl_percentage = config.get('sl_percentage', 60)
        self.sl_order_type_str = config.get('sl_order_type', 'STOP_LIMIT')
        self.sl_limit_buffer = config.get('sl_limit_buffer', 5.0)
        self.tag_prefix = config.get('tag', 'SurvivorEnhanced')
        
        logger.info(f"PositionManager initialized with SL enabled={self.sl_enabled} (forced True)")
    
    def reconcile_at_startup(self) -> SLReconciliationResult:
        """
        Main reconciliation logic - call this once at algo startup.
        
        CRITICAL: Prevents duplicate SL orders by:
        1. Fetching ALL pending/open orders from broker
        2. Matching orders to positions by symbol
        3. Calculating protected quantity per position
        4. Only placing orders for remaining unmatched quantity
        
        Returns:
            SLReconciliationResult with details of actions taken
        """
        result = SLReconciliationResult()
        
        if not self.sl_enabled:
            logger.info("SL reconciliation skipped (sl_enabled=false)")
            return result
        
        logger.info("=" * 80)
        logger.info("SL RECONCILIATION: Starting (Duplicate Protection Enabled)")
        logger.info("=" * 80)
        
        try:
            # Step 1: Clear any stale in-memory state (NO file persistence - always use fresh broker data)
            self._position_states.clear()
            logger.info(f"[RECON] Using fresh broker data only - no state file loaded")
            
            # Step 2: Fetch current broker positions
            broker_positions = self._fetch_broker_positions()
            result.total_positions = len(broker_positions)
            logger.info(f"[RECON] Found {len(broker_positions)} open positions from broker")
            
            # Step 3: Fetch ALL open/pending orders from broker
            open_orders = self._fetch_open_orders()
            logger.info(f"[RECON] Found {len(open_orders)} open/pending orders from broker")
            
            # Step 4: Match orders to positions and calculate protected quantities
            position_protection = self._calculate_protected_quantities(
                broker_positions, open_orders
            )
            
            # Step 5: Place SL orders for unprotected quantities only
            for position in broker_positions:
                # Only process short NFO MARGIN positions
                if not self._is_short_nfo_margin(position):
                    logger.info(f"[RECON] Skipping {position.symbol}: not short+NFO+MARGIN")
                    continue
                
                position_id = f"{position.symbol}_{position.exchange.value}_{position.product_type.value}"
                protected_qty = position_protection.get(position_id, 0)
                total_qty = abs(position.quantity_total)
                unprotected_qty = total_qty - protected_qty
                
                logger.info(
                    f"[RECON] Position {position.symbol}: "
                    f"total={total_qty}, protected={protected_qty}, unprotected={unprotected_qty}"
                )
                
                if unprotected_qty <= 0:
                    logger.info(f"[RECON] {position.symbol} fully protected, skipping")
                    result.protected_positions += 1
                    continue
                
                # Place SL order for unprotected quantity only
                try:
                    sl_result = self._place_sl_for_position(position, unprotected_qty)
                    if sl_result:
                        result.new_sl_placed += 1
                        result.sl_orders_placed.append(sl_result)
                        
                        # Update position state
                        position_state = self._create_position_state(position, sl_result)
                        self._position_states[position_state.position_id] = position_state
                        logger.info(
                            f"[RECON] Placed SL for {position.symbol}: "
                            f"qty={unprotected_qty}, order_id={sl_result['sl_order_id']}"
                        )
                    else:
                        error_msg = f"Failed to place SL for {position.symbol}"
                        logger.error(f"[RECON] {error_msg}")
                        result.errors.append(error_msg)
                        
                except Exception as e:
                    error_msg = f"Error placing SL for {position.symbol}: {e}"
                    logger.error(f"[RECON] {error_msg}")
                    result.errors.append(error_msg)
            
            # Step 6: NO state persistence - positions remain in memory only
            # State files create stale data and false entries - always use fresh broker data
            
            # Log summary
            logger.info("=" * 80)
            logger.info("SL RECONCILIATION: Summary")
            logger.info("=" * 80)
            logger.info(f"[RECON] Total positions: {result.total_positions}")
            logger.info(f"[RECON] Protected positions: {result.protected_positions}")
            logger.info(f"[RECON] New SL orders placed: {result.new_sl_placed}")
            logger.info(f"[RECON] Errors: {len(result.errors)}")
            if result.errors:
                for error in result.errors:
                    logger.error(f"[RECON] Error: {error}")
            logger.info("=" * 80)
            
        except Exception as e:
            error_msg = f"SL reconciliation failed: {e}"
            logger.error(f"[RECON] {error_msg}")
            result.errors.append(error_msg)
        
        return result
    
    def _fetch_broker_positions(self) -> List[Position]:
        """Fetch all open positions from broker."""
        try:
            positions = self.broker.get_positions()
            logger.debug(f"Fetched {len(positions)} positions from broker")
            return positions
        except Exception as e:
            logger.error(f"Failed to fetch positions from broker: {e}")
            return []
    
    def _fetch_open_orders(self) -> List[Dict[str, Any]]:
        """
        Fetch all open/pending orders from broker.
        
        Returns:
            List of open/pending orders that could be SL orders
        """
        try:
            orderbook = self.broker.get_orderbook()
            open_orders = []
            
            for order in orderbook:
                status = order.get('status', '').upper()
                # Consider these statuses as "active" orders
                if status in ('OPEN', 'PENDING', 'TRIGGER PENDING', 'AMO REQ RECEIVED',
                             'PUT ORDER REQ RECEIVED', 'VALIDATION PENDING', 'OPEN PENDING',
                             'MODIFY PENDING'):
                    open_orders.append(order)
            
            logger.info(f"[RECON] Fetched {len(open_orders)} open orders from orderbook")
            return open_orders
            
        except Exception as e:
            logger.error(f"[RECON] Failed to fetch orderbook: {e}")
            return []
    
    def _calculate_protected_quantities(
        self, 
        positions: List[Position], 
        open_orders: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Calculate how much quantity is already protected by open SL orders.
        
        Matches orders to positions by symbol and transaction type (BUY for shorts).
        
        Args:
            positions: List of broker positions
            open_orders: List of open/pending orders from broker
            
        Returns:
            Dict mapping position_id -> protected_quantity
        """
        protection = {}
        
        # Build set of valid position symbols for quick lookup
        position_symbols = {pos.symbol for pos in positions}
        
        for order in open_orders:
            symbol = order.get('tradingsymbol', '')
            transaction_type = order.get('transaction_type', '').upper()
            quantity = order.get('quantity', 0)
            
            # For Survivor strategy, SL orders are BUY orders to cover shorts
            if symbol not in position_symbols or transaction_type != 'BUY':
                continue
            
            # Find matching position
            for pos in positions:
                if pos.symbol == symbol and self._is_short_nfo_margin(pos):
                    position_id = f"{pos.symbol}_{pos.exchange.value}_{pos.product_type.value}"
                    
                    if position_id not in protection:
                        protection[position_id] = 0
                    
                    protection[position_id] += quantity
                    logger.info(
                        f"[RECON] Found open SL order: {symbol}, "
                        f"order_id={order.get('order_id')}, qty={quantity}, "
                        f"total_protected={protection[position_id]}"
                    )
                    break
        
        return protection
    
    def get_all_positions(self) -> Dict[str, PositionState]:
        """
        Get all tracked positions.
        
        Returns:
            Dict mapping position_id -> PositionState
        """
        return self._position_states.copy()
    
    def _is_short_nfo_margin(self, position: Position) -> bool:
        """Check if position is a short NFO MARGIN position that needs SL."""
        return (
            position.quantity_total < 0 and  # Short position
            position.exchange == Exchange.NFO and  # NFO exchange
            position.product_type == ProductType.MARGIN  # MARGIN product
        )
    
    def _place_sl_for_position(
        self, 
        position: Position, 
        quantity: int
    ) -> Optional[Dict[str, Any]]:
        """
        Place SL order for a specific quantity of a position.
        
        Args:
            position: Position object from broker
            quantity: Quantity to place SL for (may be partial)
            
        Returns:
            Dict with SL order details if successful, None otherwise
        """
        # Calculate SL prices
        trigger_price, limit_price = self._calculate_sl_prices(position.average_price)
        
        # Log the calculated values for debugging
        logger.info(
            f"[RECON] SL Calculation for {position.symbol}: "
            f"entry={position.average_price}, sl%={self.sl_percentage}, "
            f"trigger={trigger_price}, limit={limit_price}"
        )
        
        # Determine order type
        if self.sl_order_type_str == "STOP":
            order_type = OrderType.STOP  # SL-M (Stop Loss Market)
            sl_price = trigger_price
        else:
            order_type = OrderType.STOP_LIMIT  # SL (Stop Loss Limit)
            sl_price = limit_price
        
        # Create unique tag for this position (max 20 chars allowed)
        timestamp = datetime.now().strftime("%H%M%S")
        tag = f"SE_SL_{timestamp}"
        
        # Create SL order request (BUY to cover short position)
        req = OrderRequest(
            symbol=position.symbol,
            exchange=position.exchange,
            transaction_type=TransactionType.BUY,  # Buy to cover short
            quantity=quantity,
            product_type=position.product_type,
            order_type=order_type,
            price=sl_price if order_type == OrderType.STOP_LIMIT else 0,
            stop_price=trigger_price,
            tag=tag
        )
        
        # Place SL order
        order_resp = self.broker.place_order(req)
        
        if order_resp.status == "ok":
            sl_order_id = str(order_resp.order_id)
            logger.info(
                f"[RECON] SL order placed for {position.symbol}: "
                f"ID={sl_order_id}, Qty={quantity}, Trigger=₹{trigger_price}, Limit=₹{limit_price}"
            )
            return {
                'symbol': position.symbol,
                'sl_order_id': sl_order_id,
                'trigger_price': trigger_price,
                'limit_price': limit_price,
                'quantity': quantity,
                'tag': tag
            }
        else:
            logger.error(
                f"[RECON] Failed to place SL order for {position.symbol}: "
                f"{order_resp.message}"
            )
            return None
    
    def _calculate_sl_prices(self, entry_price: float) -> Tuple[float, float]:
        """
        Calculate SL trigger and limit prices for short positions.
        
        For short positions:
        - SL trigger = entry_price * (1 + sl_percentage/100)
        - SL limit = trigger_price (same as trigger for tighter control)
        
        Args:
            entry_price: Position average entry price
            
        Returns:
            Tuple of (trigger_price, limit_price)
        """
        # For short positions, SL is above entry (loss happens when price rises)
        trigger_price = entry_price * (1 + self.sl_percentage / 100)
        
        # Round to nearest tick size (0.05 for NIFTY options)
        tick_size = 0.05
        trigger_price = round(trigger_price / tick_size) * tick_size
        
        # Limit price is same as trigger price
        limit_price = trigger_price
        
        return trigger_price, limit_price
    
    def _create_position_state(
        self, 
        position: Position, 
        sl_result: Dict[str, Any]
    ) -> PositionState:
        """Create PositionState from position and SL result."""
        return PositionState(
            symbol=position.symbol,
            exchange=position.exchange.value,
            quantity=position.quantity_total,
            average_price=position.average_price,
            product_type=position.product_type.value,
            sl_order_id=sl_result['sl_order_id'],
            sl_trigger_price=sl_result['trigger_price'],
            sl_limit_price=sl_result['limit_price'],
            sl_percentage=self.sl_percentage,
            last_updated=datetime.now().isoformat(),
            position_id=f"{position.symbol}_{position.exchange.value}_{position.product_type.value}"
        )
    
    def register_sl_order(
        self, 
        position: Position, 
        sl_order_id: str,
        trigger_price: float,
        limit_price: float
    ) -> None:
        """
        Register an SL order for a position (called when SL is placed during normal trading).
        
        Args:
            position: The position being protected
            sl_order_id: The SL order ID from broker
            trigger_price: SL trigger price
            limit_price: SL limit price
        """
        position_id = f"{position.symbol}_{position.exchange.value}_{position.product_type.value}"
        
        self._position_states[position_id] = PositionState(
            symbol=position.symbol,
            exchange=position.exchange.value,
            quantity=position.quantity_total,
            average_price=position.average_price,
            product_type=position.product_type.value,
            sl_order_id=sl_order_id,
            sl_trigger_price=trigger_price,
            sl_limit_price=limit_price,
            sl_percentage=self.sl_percentage,
            last_updated=datetime.now().isoformat(),
            position_id=position_id
        )
        
        logger.info(f"Registered SL order {sl_order_id} for {position.symbol}")
    
    def clear_position(self, symbol: str, exchange: str = "NFO", product_type: str = "NRML") -> None:
        """
        Clear a position from tracking (called when position is closed).
        
        Args:
            symbol: Trading symbol
            exchange: Exchange (default NFO)
            product_type: Product type (default NRML)
        """
        position_id = f"{symbol}_{exchange}_{product_type}"
        
        if position_id in self._position_states:
            del self._position_states[position_id]
            logger.info(f"Cleared position {position_id} from tracking")
    
    def update_position_sl(
        self, 
        symbol: str, 
        sl_order_id: str,
        sl_trigger: float,
        sl_limit: float,
        exchange: str = "NFO",
        product_type: str = "NRML"
    ) -> None:
        """
        Update SL order information for an existing position.
        
        Args:
            symbol: Trading symbol
            sl_order_id: The SL order ID
            sl_trigger: SL trigger price
            sl_limit: SL limit price
            exchange: Exchange (default NFO)
            product_type: Product type (default NRML)
        """
        position_id = f"{symbol}_{exchange}_{product_type}"
        
        if position_id in self._position_states:
            self._position_states[position_id].sl_order_id = sl_order_id
            self._position_states[position_id].sl_trigger_price = sl_trigger
            self._position_states[position_id].sl_limit_price = sl_limit
            self._position_states[position_id].last_updated = datetime.now().isoformat()
            logger.info(f"Updated SL order {sl_order_id} for {symbol}")
        else:
            # Position not tracked yet, create new state
            logger.warning(f"Position {position_id} not found for SL update, creating new entry")
            self._position_states[position_id] = PositionState(
                symbol=symbol,
                exchange=exchange,
                quantity=0,  # Unknown quantity
                average_price=0.0,
                product_type=product_type,
                sl_order_id=sl_order_id,
                sl_trigger_price=sl_trigger,
                sl_limit_price=sl_limit,
                sl_percentage=self.sl_percentage,
                last_updated=datetime.now().isoformat(),
                position_id=position_id
            )
    
    def remove_position(self, symbol: str, exchange: str = "NFO", product_type: str = "NRML") -> None:
        """
        Remove a position from tracking (alias for clear_position).
        
        Args:
            symbol: Trading symbol
            exchange: Exchange (default NFO)
            product_type: Product type (default NRML)
        """
        self.clear_position(symbol, exchange, product_type)
