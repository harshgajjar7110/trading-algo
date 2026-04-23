import os
import json
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime
from logger import logger
from brokers import Exchange, OrderType, TransactionType, ProductType, OrderRequest

@dataclass
class SpreadPosition:
    spread_id: str              # uuid
    signal_type: str            # "PE" or "CE"
    short_symbol: str           # e.g. "NIFTY26127P24000"
    hedge_symbol: str           # e.g. "NIFTY26127P23700"
    short_order_id: str
    hedge_order_id: str
    short_entry_price: float    # premium collected
    hedge_entry_price: float    # premium paid
    net_credit: float           # short_entry_price - hedge_entry_price
    quantity: int
    index_price_at_entry: float
    sl_trigger: float           # short_entry_price * 1.30
    target_trigger: float       # short_entry_price * 0.40
    timestamp: str              # ISO8601
    status: str                 # "open" | "sl_hit" | "target_hit" | "manual_exit" | "gap_bailout"

class PositionLifecycleManager:
    def __init__(self, broker, config: dict):
        self.broker = broker
        self.config = config
        # Use absolute path for artifacts
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.artifacts_dir = os.path.join(base_dir, "artifacts")
        self.positions_file = os.path.join(self.artifacts_dir, "positions.json")
        self.positions: list[SpreadPosition] = []
        self._load_positions()

    def _load_positions(self):
        """Loads positions from JSON file."""
        if not os.path.exists(self.artifacts_dir):
            os.makedirs(self.artifacts_dir)
        
        if os.path.exists(self.positions_file):
            try:
                with open(self.positions_file, "r") as f:
                    data = json.load(f)
                    self.positions = [SpreadPosition(**p) for p in data]
            except Exception as e:
                logger.error(f"Error loading positions: {e}")
                self.positions = []

    def _save_positions(self):
        """Saves positions to JSON file."""
        try:
            with open(self.positions_file, "w") as f:
                json.dump([asdict(p) for p in self.positions], f, indent=4)
        except Exception as e:
            logger.error(f"Error saving positions: {e}")

    def get_total_mtm_loss(self) -> float:
        """
        Calculates total MTM loss for open positions.
        Includes both short and hedge legs.
        """
        open_positions = [p for p in self.positions if p.status == "open"]
        if not open_positions:
            return 0.0

        # Collect all symbols needed
        symbols = []
        for p in open_positions:
            symbols.append(f"NFO:{p.short_symbol}")
            symbols.append(f"NFO:{p.hedge_symbol}")
        
        try:
            # Batch fetch quotes
            quotes = self.broker.get_quotes(list(set(symbols)))

            total_mtm = 0.0
            for p in open_positions:
                short_quote = quotes.get(f"NFO:{p.short_symbol}")
                hedge_quote = quotes.get(f"NFO:{p.hedge_symbol}")
                
                if short_quote and hedge_quote:
                    # MTM = (Entry - Current) for Sell side, (Current - Entry) for Buy side
                    short_mtm = (p.short_entry_price - short_quote.last_price) * p.quantity
                    hedge_mtm = (hedge_quote.last_price - p.hedge_entry_price) * p.quantity
                    total_mtm += (short_mtm + hedge_mtm)
            
            # Risk limit usually checks LOSS, so return negative MTM as positive loss
            return -total_mtm if total_mtm < 0 else 0.0
        except Exception as e:
            logger.error(f"Error calculating MTM loss: {e}")
            return 0.0

    def open_spread(self, signal_type, short_symbol, hedge_symbol, quantity, index_price, min_price_to_sell=0) -> SpreadPosition | None:
        """
        Places spread legs atomically.
        """
        logger.info(f"Opening {signal_type} spread: Short={short_symbol}, Hedge={hedge_symbol}, Qty={quantity}")
        
        try:
            # 1. Get quotes for both legs
            quotes = self.broker.get_quotes([f"NFO:{short_symbol}", f"NFO:{hedge_symbol}"])
            short_quote = quotes.get(f"NFO:{short_symbol}")
            hedge_quote = quotes.get(f"NFO:{hedge_symbol}")
            
            if not short_quote or not hedge_quote:
                logger.error("Could not get quotes for spread legs")
                return None

            if short_quote.last_price < min_price_to_sell:
                logger.info(f"{signal_type} short premium {short_quote.last_price} < min_price_to_sell {min_price_to_sell}. Skipping.")
                return None

            # 2. Place short leg
            short_req = OrderRequest(
                symbol=short_symbol, exchange=Exchange.NFO, transaction_type=TransactionType.SELL,
                quantity=quantity, product_type=ProductType.MARGIN, order_type=OrderType.MARKET,
                price=0, tag="Survivor"
            )
            short_resp = self.broker.place_order(short_req)
            if not short_resp or short_resp.status != "ok":
                logger.error(f"Failed to place short leg: {short_resp.message if short_resp else 'No response'}")
                return None
            
            # 3. Place hedge leg
            hedge_req = OrderRequest(
                symbol=hedge_symbol, exchange=Exchange.NFO, transaction_type=TransactionType.BUY,
                quantity=quantity, product_type=ProductType.MARGIN, order_type=OrderType.MARKET,
                price=0, tag="Survivor"
            )
            hedge_resp = self.broker.place_order(hedge_req)
            
            # 4. If hedge fails, reverse short leg
            if not hedge_resp or hedge_resp.status != "ok":
                logger.error(f"Failed to place hedge leg: {hedge_resp.message if hedge_resp else 'No response'}. REVERSING short leg!")
                reverse_req = OrderRequest(
                    symbol=short_symbol, exchange=Exchange.NFO, transaction_type=TransactionType.BUY,
                    quantity=quantity, product_type=ProductType.MARGIN, order_type=OrderType.MARKET,
                    price=0, tag="Survivor_Reversal"
                )
                self.broker.place_order(reverse_req)
                return None

            # 5. Build SpreadPosition
            pos = SpreadPosition(
                spread_id=str(uuid.uuid4()),
                signal_type=signal_type,
                short_symbol=short_symbol,
                hedge_symbol=hedge_symbol,
                short_order_id=str(short_resp.order_id),
                hedge_order_id=str(hedge_resp.order_id),
                short_entry_price=short_quote.last_price,
                hedge_entry_price=hedge_quote.last_price,
                net_credit=short_quote.last_price - hedge_quote.last_price,
                quantity=quantity,
                index_price_at_entry=index_price,
                sl_trigger=round(short_quote.last_price * 1.30, 2),
                target_trigger=round(short_quote.last_price * 0.40, 2),
                timestamp=datetime.now().isoformat(),
                status="open"
            )
            
            self.positions.append(pos)
            self._save_positions()
            logger.info(f"Spread opened successfully: {pos.spread_id}")
            return pos

        except Exception as e:
            logger.error(f"Exception in open_spread: {e}")
            return None

    def reconcile_on_open(self):
        """
        Bailout logic for overnight gaps.
        """
        logger.info("Reconciling open positions at startup...")
        open_positions = [p for p in self.positions if p.status == "open"]
        if not open_positions:
            logger.info("No open positions to reconcile.")
            return

        # Fetch quotes for all open short legs
        symbols = [f"NFO:{p.short_symbol}" for p in open_positions]
        quotes = self.broker.get_quotes(symbols)

        for p in open_positions:
            try:
                # Basic sanity check: ensure we have order IDs
                if not p.short_order_id or p.short_order_id == "None":
                    logger.error(f"Position {p.spread_id} has invalid short_order_id. Marking closed to avoid accidents.")
                    p.status = "error_cleanup"
                    continue

                quote = quotes.get(f"NFO:{p.short_symbol}")
                if quote and quote.last_price > 2 * p.short_entry_price:
                    logger.warning(f"GAP BAILOUT: {p.short_symbol} LTP {quote.last_price} > 2x Entry {p.short_entry_price}")
                    self.exit_spread(p, "gap_bailout")
            except Exception as e:
                logger.error(f"Error reconciling {p.short_symbol}: {e}")
        
        self._save_positions()

    def on_tick(self, current_index_price: float):
        """
        Monitors SL/Target for each open position.
        """
        open_positions = [p for p in self.positions if p.status == "open"]
        if not open_positions:
            return

        # Fetch quotes for all unique short symbols in batch
        symbols = list(set([f"NFO:{p.short_symbol}" for p in open_positions]))
        try:
            quotes = self.broker.get_quotes(symbols)
        except Exception as e:
            logger.error(f"Error fetching batch quotes on tick: {e}")
            return

        for p in open_positions:
            quote = quotes.get(f"NFO:{p.short_symbol}")
            if not quote:
                continue
            
            current_price = quote.last_price
            if current_price >= p.sl_trigger:
                logger.info(f"SL HIT: {p.short_symbol} LTP {current_price} >= SL {p.sl_trigger}")
                self.exit_spread(p, "sl_hit")
            elif current_price <= p.target_trigger:
                logger.info(f"TARGET HIT: {p.short_symbol} LTP {current_price} <= Target {p.target_trigger}")
                self.exit_spread(p, "target_hit")

    def exit_spread(self, position: SpreadPosition, status: str):
        """Exits both legs of a spread."""
        logger.info(f"Exiting spread {position.spread_id} with status {status}")
        
        # Guard against double-exit or degenerate states
        if position.status != "open":
            return

        try:
            # 1. Set status immediately to prevent re-triggering during network latency
            position.status = status
            self._save_positions()

            # 2. Close short leg (Buy)
            buy_req = OrderRequest(
                symbol=position.short_symbol, exchange=Exchange.NFO, transaction_type=TransactionType.BUY,
                quantity=position.quantity, product_type=ProductType.MARGIN, order_type=OrderType.MARKET,
                price=0, tag="Survivor_Exit"
            )
            resp_short = self.broker.place_order(buy_req)
            if not resp_short or resp_short.status != "ok":
                logger.error(f"Failed to exit short leg for {position.spread_id}!")
                # We don't revert status here because one leg might have succeeded or we'll retry next tick
            
            # 3. Close hedge leg (Sell) - ONLY if it exists
            if position.hedge_symbol and position.hedge_symbol != "None":
                sell_req = OrderRequest(
                    symbol=position.hedge_symbol, exchange=Exchange.NFO, transaction_type=TransactionType.SELL,
                    quantity=position.quantity, product_type=ProductType.MARGIN, order_type=OrderType.MARKET,
                    price=0, tag="Survivor_Exit"
                )
                resp_hedge = self.broker.place_order(sell_req)
                if not resp_hedge or resp_hedge.status != "ok":
                    logger.error(f"Failed to exit hedge leg for {position.spread_id}!")

        except Exception as e:
            logger.error(f"Error exiting spread {position.spread_id}: {e}")
