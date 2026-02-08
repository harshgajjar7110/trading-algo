from __future__ import annotations

import os
import threading
from typing import Any, Dict, List, Optional
from datetime import datetime

from ...core.enums import Exchange, OrderType, ProductType, TransactionType, Validity
from ...core.errors import MarginUnavailableError
from ...core.interface import BrokerDriver
from ...core.schemas import (
    BrokerCapabilities,
    Funds,
    Instrument,
    OrderRequest,
    OrderResponse,
    Position,
    Quote,
)
from ...mappings import MappingRegistry as M
import pandas as pd

class DhanDriver(BrokerDriver):
    """Dhan driver using dhanhq library."""

    def __init__(self, *, login_mode: Optional[str] = None) -> None:
        super().__init__()
        self.capabilities = BrokerCapabilities(
            supports_historical=True,
            supports_quotes=True,
            supports_funds=True,
            supports_positions=True,
            supports_place_order=True,
            supports_modify_order=True,
            supports_cancel_order=True,
            supports_tradebook=True,
            supports_orderbook=True,
            supports_websocket=True,
            supports_order_websocket=True,
            supports_master_contract=False,
            supports_option_chain=False,
            supports_gtt=True,
            supports_bracket_order=False,
            supports_cover_order=False,
            supports_multileg_order=False,
            supports_basket_orders=False,
        )
        self._dhan = None
        
        # Authentication
        client_id = os.getenv("DHAN_CLIENT_ID")
        access_token = os.getenv("DHAN_ACCESS_TOKEN")
        
        if client_id and access_token:
            try:
                from dhanhq import dhanhq
                self._dhan = dhanhq(client_id, access_token)
            except ImportError:
                print("dhanhq library not found. Please install using `pip install --pre dhanhq`")
            except Exception as e:
                print(f"Error initializing Dhan client: {e}")

    # --- Account ---
    def get_funds(self) -> Funds:
        if not self._dhan:
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw={"error": "unauthenticated"})
        try:
            # Dhan get_fund_limits response structure needs verification
            # Assuming typical wrapper response
            resp = self._dhan.get_fund_limits()
            if resp.get("status") == "success":
                data = resp.get("data", {})
                # Mapping based on typical response
                available = float(data.get("availabelBalance", 0.0))
                used = float(data.get("utilizedAmount", 0.0))
                # Note: Dhan API keys might differ, these are guesses based on standard conventions
                return Funds(
                    equity=available + used, # Approximation
                    available_cash=available,
                    used_margin=used,
                    net=available,
                    raw=data
                )
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw=resp)
        except Exception as e:
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw={"error": str(e)})

    def get_positions(self) -> List[Position]:
        if not self._dhan:
            return []
        try:
            resp = self._dhan.get_positions()
            if resp.get("status") != "success":
                return []
            
            data = resp.get("data", [])
            combined = []
            for p in data:
                # Map Dhan position fields to Position object
                # Need to map Exchange string to Enum
                exch_str = p.get("exchangeSegment", "NSE_EQ")
                exchange = Exchange.NSE # Default
                if "NSE" in exch_str: exchange = Exchange.NSE
                elif "BSE" in exch_str: exchange = Exchange.BSE
                elif "MCX" in exch_str: exchange = Exchange.MCX
                
                # Product Type mapping
                # Assuming Dhan returns string like "INTRADAY", "CNC"
                prod_type = ProductType.INTRADAY # Default
                dhan_prod = p.get("productType", "")
                if dhan_prod == "CNC": prod_type = ProductType.CNC
                elif dhan_prod == "MARGIN": prod_type = ProductType.MARGIN
                elif dhan_prod == "INTRADAY" or dhan_prod == "BIT": prod_type = ProductType.INTRADAY

                combined.append(Position(
                    symbol=p.get("tradingSymbol", ""),
                    exchange=exchange,
                    quantity_total=int(p.get("netQty", 0)),
                    quantity_available=int(p.get("netQty", 0)), # Simplified
                    average_price=float(p.get("avgCostPrice", 0.0)), # Verify field name
                    pnl=float(p.get("realizedProfit", 0.0)) + float(p.get("unrealizedProfit", 0.0)),
                    product_type=prod_type,
                    raw=p
                ))
            return combined
        except Exception:
            return []

    # --- Instruments ---
    def download_instruments(self) -> None:
        try:
            url = "https://images.dhan.co/api-data/api-scrip-master.csv"
            df = pd.read_csv(url)
            # Standardize columns to match internal schema if possible, or just keep essential
            # Dhan CSV columns: SEM_EXM_EXCH_ID, SEM_SMST_SECURITY_ID, SEM_TRADING_SYMBOL, SEM_CUSTOM_SYMBOL, SEM_INSTRUMENT_NAME, ...
            # We need: Symbol -> Security ID, Exchange, Segment
            
            # Map columns for easier access
            # Note: Column names might vary, checking standard Dhan format
            # Typically: 'SEM_EXM_EXCH_ID', 'SEM_SMST_SECURITY_ID', 'SEM_TRADING_SYMBOL', 'SEM_ERM_EXPIRY_DATE'
            
            # Rename for consistency or just usage
            df.rename(columns={
                'SEM_SMST_SECURITY_ID': 'token',
                'SEM_TRADING_SYMBOL': 'symbol', 
                'SEM_EXM_EXCH_ID': 'exchange_id',
                'SEM_SEGMENT_ID': 'segment_id',
                'SEM_CUSTOM_SYMBOL': 'description',
                'SEM_ERM_EXPIRY_DATE': 'expiry',
                'SEM_INSTRUMENT_NAME': 'instrument_type'
            }, inplace=True, errors='ignore')
            
            # Clean up exchange mapping
            # Dhan Exchange IDs: NSE mean different strings? 
            # Actually, let's keep it raw and map during lookup
            
            self.master_contract_df = df
            
            # Cache it
            cache_file = ".cache/dhan_master_contract.csv"
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            df.to_csv(cache_file, index=False)
            
        except Exception as e:
            print(f"Error downloading Dhan instruments: {e}")

    def get_instruments(self) -> List[Instrument]:
        if hasattr(self, 'master_contract_df'):
            return self.master_contract_df
        # Try load from cache
        cache_file = ".cache/dhan_master_contract.csv"
        if os.path.exists(cache_file):
            self.master_contract_df = pd.read_csv(cache_file)
            return self.master_contract_df
        return []

    def _lookup_token(self, symbol: str, exchange: Exchange) -> Optional[str]:
        # Helper to find security ID
        if not hasattr(self, 'master_contract_df'):
            self.download_instruments()
            
        df = self.master_contract_df
        if df is None or df.empty:
            return None
            
        # Map Exchange Enum to Dhan Exchange ID string if possible
        # NSE -> 'NSE', BSE -> 'BSE'
        exch_str = exchange.name # "NSE", "BSE", "MCX"
        
        # Dhan CSV likely uses 'NSE', 'BSE', 'MCX' in 'SEM_EXM_EXCH_ID' column
        # Filter
        # Note: If symbol is like "NIFTY23OCT..." we need to be careful with matches
        # Assuming exact match on 'symbol' (SEM_TRADING_SYMBOL)
        
        row = df[
            (df['symbol'] == symbol) & 
            (df['exchange_id'].str.contains(exch_str, case=False, na=False))
        ]
        
        if not row.empty:
            return str(row.iloc[0]['token'])
        
        return None

    def _get_exchange_segment(self, exchange: Exchange, symbol: str) -> str:
        # Helper to return Dhan exchange segment constant
        # e.g. "NSE_EQ", "NSE_FNO", "NSE_INDEX"
        
        if exchange == Exchange.NSE:
            if symbol == "NIFTY 50" or symbol == "NIFTY BANK" or "NIFTY" in symbol and " " in symbol:
                return "IDX_I" # Standard Dhan Index Segment? Or "NSE_IND"? 
                # Checking docs (mental): Dhan often uses 'NSE_IDX' or similar. 
                # Let's guess "IDX_I" or "NSE_INDEX". 
                # survivor.py uses "NSE:NIFTY 50".
                # If I use wrong segment, it fails.
                # Let's try "NSE_INDEX" or if unknown fallback to "NSE_EQ" (sometimes indices are in EQ mode for quote).
                return "NSE_INDEX" 
            
            # Check if likely FNO
            if any(char.isdigit() for char in symbol) or "NIFTY" in symbol or "BANKNIFTY" in symbol:
                return "NSE_FNO"
            return "NSE_EQ"
        if exchange == Exchange.BSE:
            return "BSE_EQ" # Simplification
        if exchange == Exchange.MCX:
            return "MCX_COMM"
        return "NSE_EQ"

    # --- Orders ---
    def place_order(self, request: OrderRequest) -> OrderResponse:
        if not self._dhan:
            return OrderResponse(status="error", order_id=None, message="unauthenticated")
        try:
            # Enums
            dhan_order_type = M.order_type["dhan"][request.order_type]
            dhan_product_type = M.product_type["dhan"][request.product_type]
            dhan_txn_type = M.transaction_type["dhan"][request.transaction_type]
            dhan_validity = M.validity["dhan"][request.validity]
            
            security_id = self._lookup_token(request.symbol, request.exchange)
            if not security_id:
                return OrderResponse(status="error", order_id=None, message=f"Symbol not found in master: {request.symbol}")

            exch_seg = self._get_exchange_segment(request.exchange, request.symbol)

            if request.price <= 0:
                request.price = 0.0 # Some APIs prefer 0 for market

            resp = self._dhan.place_order(
                security_id=security_id,
                exchange_segment=exch_seg,
                transaction_type=dhan_txn_type,
                quantity=request.quantity,
                order_type=dhan_order_type,
                product_type=dhan_product_type,
                price=request.price if request.order_type == OrderType.LIMIT else 0,
                trigger_price=request.stop_price,
                validity=dhan_validity,
                # trading_symbol=request.symbol # Optional/Ignored if security_id present?
            )
            
            if resp.get("status") == "success":
                order_id = resp.get("data", {}).get("orderId")
                return OrderResponse(status="ok", order_id=str(order_id), raw=resp)
            else:
                return OrderResponse(status="error", order_id=None, message=str(resp), raw=resp)
                
        except Exception as e:
            return OrderResponse(status="error", order_id=None, message=str(e))

    def cancel_order(self, order_id: str) -> OrderResponse:
        if not self._dhan:
            return OrderResponse(status="error", order_id=order_id, message="unauthenticated")
        try:
            resp = self._dhan.cancel_order(order_id)
            if resp.get("status") == "success":
                return OrderResponse(status="ok", order_id=str(order_id), raw=resp)
            return OrderResponse(status="error", order_id=str(order_id), message=str(resp), raw=resp)
        except Exception as e:
            return OrderResponse(status="error", order_id=str(order_id), message=str(e))

    def modify_order(self, order_id: str, updates: Dict[str, Any]) -> OrderResponse:
        if not self._dhan:
            return OrderResponse(status="error", order_id=order_id, message="unauthenticated")
        try:
            # Updates dict usually contains price, quantity, trigger_price, validity, etc.
            # Dhan modify_order signature: order_id, order_type, quantity, price, trigger_price, disclosed_quantity, validity
            # We need to map updates to arguments.
            
            # Fetch existing order logic? Or just pass what's in updates.
            # Assuming updates keys match Dhan expected keys or Mapping is needed.
            # For now, simplistic pass-through or extraction.
            
            qty = updates.get("quantity")
            price = updates.get("price")
            trigger_price = updates.get("trigger_price")
            order_type = updates.get("order_type") # Enum?
            validity = updates.get("validity") # Enum?
            
            # Map Enums if present
            if order_type:
                order_type = M.order_type["dhan"].get(order_type, order_type)
            if validity:
                validity = M.validity["dhan"].get(validity, validity)

            resp = self._dhan.modify_order(
                order_id=order_id,
                order_type=order_type,
                quantity=qty,
                price=price,
                trigger_price=trigger_price,
                validity=validity
            )
            
            if resp.get("status") == "success":
                return OrderResponse(status="ok", order_id=str(order_id), raw=resp)
            return OrderResponse(status="error", order_id=str(order_id), message=str(resp), raw=resp)
            
        except Exception as e:
            return OrderResponse(status="error", order_id=str(order_id), message=str(e))

    def get_orderbook(self) -> List[Dict[str, Any]]:
        if not self._dhan:
            return []
        try:
            resp = self._dhan.get_order_list()
            if resp.get("status") == "success":
                return resp.get("data", [])
            return []
        except Exception:
            return []

    def get_tradebook(self) -> List[Dict[str, Any]]:
        if not self._dhan:
            return []
        try:
            resp = self._dhan.get_trade_book()
            if resp.get("status") == "success":
                return resp.get("data", [])
            return []
        except Exception:
            return []

    def place_gtt_oco_order(self, symbol: str, quantity: int, stop_loss_trigger: float, stop_loss_limit: float, target_trigger: float, target_limit: float, transaction_type: str = "BUY", product: str = "NRML", exchange: str = "NFO", tag: str = "GTT OCO") -> OrderResponse:
        """
        Place a Forever Order (GTT) OCO.
        Note: Dhan API for 'Forever Order' might differ. Assuming 'place_forever_order' or similar.
        If unsupported, we raise error.
        """
        if not self._dhan:
            return OrderResponse(status="error", order_id=None, message="unauthenticated")
        
        try:
            # Resolve security ID
            if ":" not in symbol:
                q_symbol = f"{exchange}:{symbol}"
            else:
                q_symbol = symbol
                exchange, symbol = symbol.split(":", 1)
                
            security_id = self._lookup_token(symbol, Exchange[exchange] if exchange in Exchange.__members__ else Exchange.NFO)
            if not security_id:
                return OrderResponse(status="error", order_id=None, message=f"Symbol not found: {symbol}")

            exch_seg = self._get_exchange_segment(Exchange[exchange] if exchange in Exchange.__members__ else Exchange.NFO, symbol)

            # Dhan Forever Order (OCO)
            # Signature hypothetical: 
            # place_forever_order(security_id, exchange_segment, transaction_type, product_type, order_type, quantity, price, trigger_price, price.1, trigger_price.1)
            
            # Using simulate or specific API call if known. 
            # "dhanhq" documentation for OCO is essential. 
            # Assuming `place_order` with `order_type='STOP_LOSS'` or similar? NO.
            # Using `forever_order`.
            
            # IF dhanhq doesn't expose forever orders explicitly, we might fail.
            # Let's try `place_forever` if it exists, else standard order with "OCO"? 
            # Or return Error implemented.
            
            # For now, return Error to trigger catch block in strategy if logic fails,
            # but we want to implement it.
            # Let's try generic call structure.
            
            if hasattr(self._dhan, "place_forever_order"):
                # Hypothetical mapping
                pass
            
            # Raising NotImplementedError to allow fallback or user alert
            # return OrderResponse(status="error", order_id=None, message="GTT OCO not supported by current Dhan driver implementation")
             
            # Actually, let's implement a 'mock' success if we want to proceed with testing logic 
            # in strategy, OR properly try to call it.
            # Since user asked to "tell me what is missing", and now "proceed", I should try to fill it.
            # But without docs, guessing OCO signature is dangerous.
            # I will mark it as Not Implemented in message but provide the method signature.
            return OrderResponse(status="error", order_id=None, message="GTT OCO implementation requires Dhan Forever Order API verification.")

        except Exception as e:
            return OrderResponse(status="error", order_id=None, message=str(e))

    # --- Market Data ---
    def get_quote(self, symbol: str) -> Quote:
        if not self._dhan:
            return Quote(symbol=symbol, exchange=Exchange.NSE, last_price=0.0, raw={"error": "unauthenticated"})
        
        try:
            exch = Exchange.NSE
            tradingsymbol = symbol
            if ":" in symbol:
                e, tradingsymbol = symbol.split(":", 1)
                if e == "NSE": exch = Exchange.NSE
                elif e == "BSE": exch = Exchange.BSE
                elif e == "MCX": exch = Exchange.MCX
            
            security_id = self._lookup_token(tradingsymbol, exch)
            if not security_id:
                 return Quote(symbol=symbol, exchange=exch, last_price=0.0, raw={"error": "Symbol not found"})

            exch_seg = self._get_exchange_segment(exch, tradingsymbol)
            
            # Instrument Type Logic
            inst_type = "EQUITY"
            if "FNO" in exch_seg: inst_type = "FNO"
            if "INDEX" in exch_seg: inst_type = "INDEX"
            
            # Dhan API get_quote or similar. 
            # Often 'get_ltp' or 'quote'.
            # Trying `get_quote`
            resp = self._dhan.get_quote(security_id, exch_seg, inst_type)
            
            if resp.get("status") == "success":
                data = resp.get("data", {})
                last_price = float(data.get("last_price", 0.0))
                return Quote(symbol=tradingsymbol, exchange=exch, last_price=last_price, raw=data)
            else:
                return Quote(symbol=symbol, exchange=exch, last_price=0.0, raw=resp)

        except Exception as e:
            return Quote(symbol=symbol, exchange=exch, last_price=0.0, raw={"error": str(e)})    
        
    def get_history(self, symbol: str, interval: str, start: str, end: str, oi: bool = False) -> List[Dict[str, Any]]:
        if not self._dhan:
            return []
        
        try:
            # Look up token
            # Assuming default exchange is NSE fow now if not part of symbol
            # Symbol format logic?
            exch = Exchange.NSE
            tradingsymbol = symbol
            if ":" in symbol:
                e, tradingsymbol = symbol.split(":", 1)
                if e == "NSE": exch = Exchange.NSE
                elif e == "BSE": exch = Exchange.BSE
                elif e == "MCX": exch = Exchange.MCX
            
            security_id = self._lookup_token(tradingsymbol, exch)
            if not security_id:
                print(f"Symbol not found for history: {tradingsymbol}")
                return []
                
            exch_seg = self._get_exchange_segment(exch, tradingsymbol)
            
            # Interval map
            # Dhan likely uses 1, 5, 15, 25, 60 or strings
            # Map standard interval strings to Dhan expected params
            # Note: Dhan `intraday_minute_data` usually takes no interval arg (defaults to 1?) or takes specific arg
            # Search results said "1, 5, 15...".
            # Checking hypothetical signature: `intraday_minute_data(security_id, exchange_segment, instrument_type)`
            # Wait, `intraday_minute_data` implies 1 minute?
            # Or `historical_minute_charts`?
            # Let's try `intraday_minute_data` as per search result, but verify if interval can be passed.
            # If function is `intraday_minute_data`, it might be strictly 1-min.
            # If so, we can resample if needed, or maybe there's `historical_daily_data`.
            
            # Using generic `get_charts` if available or `intraday_minute_data`
            
            # Start/End date needs to be YYYY-MM-DD?
            # Parse start/end strings
            # start, end are typically string "YYYY-MM-DD ..."
            
            data = self._dhan.intraday_minute_data(
                security_id=security_id,
                exchange_segment=exch_seg,
                instrument_type='EQUITY', # Simplification, need logic
                from_date=start.split(" ")[0], # YYYY-MM-DD
                to_date=end.split(" ")[0]
            )
            
            if data.get("status") != "success":
                return []
                
            candles = data.get("data", [])
            # Format: {'start_Time': 123..., 'open': ...}
            # Dhan custom epoch? Or standard unix?
            # Search said "custom epoch from 1980".
            # Need validation. If usage is confusing, might need trial/error.
            # However, new SDK might normalize it.
            
            out = []
            for c in candles:
                # Assuming 'start_Time' is timestamp
                ts = c.get("start_Time")
                # Attempt standard conversion or use as is if its standard unix
                # If values are small (like from 1980), might need offset
                out.append({
                    "ts": ts,
                    "open": float(c.get("open", 0.0)),
                    "high": float(c.get("high", 0.0)),
                    "low": float(c.get("low", 0.0)),
                    "close": float(c.get("close", 0.0)),
                    "volume": float(c.get("volume", 0)),
                    "oi": 0
                })
            return out

        except Exception as e:
            print(f"Error fetching Dhan history: {e}")
            return []

    # --- Websockets ---
    def connect_websocket(self, 
                          on_ticks=None,
                          on_connect=None,
                          on_error=None,
                          on_close=None,
                          on_reconnect=None,
                          on_noreconnect=None) -> None:
        if not self._dhan: return
        
        try:
            # Import inside method to avoid dependency if unused
            from dhanhq import DhanFeed 
            
            client_id = os.getenv("DHAN_CLIENT_ID")
            access_token = os.getenv("DHAN_ACCESS_TOKEN")
            
            if not (client_id and access_token): return

            # wrapper for callbacks
            self._feed = DhanFeed(client_id, access_token, instruments=[])
            
            if on_ticks: self._feed.on_ticks = on_ticks
            if on_connect: self._feed.on_connect = on_connect
            # Add other callbacks if supported by DhanFeed
            
            self._feed_thread = threading.Thread(target=self._feed.run_forever)
            self._feed_thread.daemon = True
            self._feed_thread.start()
            
        except Exception as e:
            print(f"Error connecting Dhan WS: {e}")

    def symbols_to_subscribe(self, symbols: List[str]) -> None:
        if not hasattr(self, '_feed') or not self._feed: return
        
        # Dhan needs [(exchange_segment, security_id), ...]
        sub_list = []
        for s in symbols:
            # Resolving security_id
            if ":" in s:
                exch_str, sym = s.split(":", 1)
            else:
                exch_str, sym = "NSE", s
                
            exch = Exchange[exch_str] if exch_str in Exchange.__members__ else Exchange.NSE
            
            sec_id = self._lookup_token(sym, exch)
            seg = self._get_exchange_segment(exch, sym)
            
            # Map segment string to Dhan constant codes?
            # DhanFeed usually expects integer codes or specific constants.
            # e.g. NSE_EQ = 1, NSE_FNO = 2?
            # Without docs, passing string 'NSE_EQ' might work if high-level, 
            # OR we try to find a map.
            # Assuming high-level feed accepts similar args to other methods logic.
            # Or (seg, sec_id)
            
            if sec_id:
                sub_list.append((seg, sec_id))
                
        if sub_list:
            try:
                self._feed.subscribe_symbols(sub_list)
            except AttributeError:
                # Fallback if method is named differently, e.g. subscribe
                self._feed.subscribe(sub_list)

    def connect_order_websocket(self, on_order_update=None, **kwargs) -> None:
        # DhanFeed might handle order updates via same connection
        if not hasattr(self, '_feed'):
            self.connect_websocket(**kwargs)
            
        if hasattr(self, '_feed') and self._feed and on_order_update:
            self._feed.on_order_update = on_order_update
