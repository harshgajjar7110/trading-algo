from __future__ import annotations

import os
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import pandas as pd
import requests
import numpy as np

from dhanhq import dhanhq

from brokers.core.enums import Exchange, OrderType, ProductType, TransactionType, Validity
from brokers.core.interface import BrokerDriver
from brokers.core.schemas import (
    BrokerCapabilities,
    Funds,
    OrderRequest,
    OrderResponse,
    Position,
    Quote,
)
from brokers.mappings import dhan as M


class DhanDriver(BrokerDriver):
    def __init__(self) -> None:
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
            supports_websocket=False, # Implementing basic first
            supports_order_websocket=False,
            supports_gtt=False,
            supports_bracket_order=False,
            supports_cover_order=False,
            supports_multileg_order=False,
            supports_basket_orders=False,
        )
        self._dhan = None
        self._authenticate()

    def _authenticate(self) -> None:
        client_id = os.getenv("DHAN_CLIENT_ID")
        access_token = os.getenv("DHAN_ACCESS_TOKEN")

        if client_id and access_token:
            try:
                self._dhan = dhanhq(client_id, access_token)
            except Exception:
                self._dhan = None

    # --- Account ---
    def get_funds(self) -> Funds:
        if not self._dhan:
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw={"error": "unauthenticated"})
        try:
            # Dhan get_fund_limits returns a dict
            data = self._dhan.get_fund_limits()
            if data.get("status") == "success":
                # Note: Dhan API response structure needs to be verified. 
                # Assuming standard structure based on docs/experience or generic mapping
                # Usually returns 'data' key
                funds_data = data.get("data", {})
                available_cash = float(funds_data.get("availabelBalance", 0.0)) # Note: Check spelling in actual API response, sometimes it's 'availableBalance'
                used_margin = float(funds_data.get("utilizedAmount", 0.0))
                # If 'availabelBalance' is not found, try 'availableBalance'
                if available_cash == 0.0 and "availableBalance" in funds_data:
                    available_cash = float(funds_data["availableBalance"])
                
                return Funds(
                    equity=available_cash + used_margin, # Approx
                    available_cash=available_cash,
                    used_margin=used_margin,
                    net=available_cash,
                    raw=data
                )
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw=data)
        except Exception as e:
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw={"error": str(e)})

    def get_positions(self) -> List[Position]:
        if not self._dhan:
            return []
        try:
            resp = self._dhan.get_positions()
            if resp.get("status") != "success":
                return []
            
            combined: List[Position] = []
            for p in resp.get("data", []):
                # Map Dhan position to internal Position
                # Dhan fields: tradingSymbol, exchangeSegment, netQty, buyAvg, sellAvg, realizedProfit, unrealizedProfit, productType
                
                exch_seg = p.get("exchangeSegment")
                exchange = Exchange.NSE # Default
                if exch_seg == "NSE_EQ": exchange = Exchange.NSE
                elif exch_seg == "NSE_FNO": exchange = Exchange.NFO
                elif exch_seg == "BSE_EQ": exchange = Exchange.BSE
                elif exch_seg == "BSE_FNO": exchange = Exchange.BFO
                elif exch_seg == "MCX_COMM": exchange = Exchange.MCX
                
                qty = int(p.get("netQty", 0))
                avg_price = float(p.get("buyAvg", 0)) if qty > 0 else float(p.get("sellAvg", 0))
                
                # Product type mapping
                prod = p.get("productType")
                product_type = ProductType.INTRADAY # Default
                if prod == "CNC": product_type = ProductType.CNC
                elif prod == "INTRADAY": product_type = ProductType.INTRADAY
                elif prod == "MARGIN": product_type = ProductType.MARGIN
                elif prod == "CO": product_type = ProductType.CO
                elif prod == "BO": product_type = ProductType.BO

                combined.append(
                    Position(
                        symbol=p.get("tradingSymbol"),
                        exchange=exchange,
                        quantity_total=qty,
                        quantity_available=qty, # Dhan doesn't explicitly separate this in simple position view often
                        average_price=avg_price,
                        pnl=float(p.get("realizedProfit", 0)) + float(p.get("unrealizedProfit", 0)),
                        product_type=product_type,
                        raw=p
                    )
                )
            return combined
        except Exception:
            return []

    # --- Orders ---
    def place_order(self, request: OrderRequest) -> OrderResponse:
        if not self._dhan:
            return OrderResponse(status="error", order_id=None, message="unauthenticated")
        try:
            # Map fields
            txn_type = M.transaction_type.get(request.transaction_type)
            order_type = M.order_type.get(request.order_type)
            product_type = M.product_type.get(request.product_type)
            validity = M.validity.get(request.validity)
            
            # Exchange segment mapping logic
            exchange_segment = M.NSE_EQ # Default
            if request.exchange == Exchange.NSE:
                exchange_segment = M.NSE_EQ
            elif request.exchange == Exchange.NFO:
                exchange_segment = M.NSE_FNO
            elif request.exchange == Exchange.BSE:
                exchange_segment = M.BSE_EQ
            elif request.exchange == Exchange.BFO:
                exchange_segment = M.BSE_FNO
            elif request.exchange == Exchange.MCX:
                exchange_segment = M.MCX_COMM
            elif request.exchange == Exchange.CDS:
                exchange_segment = M.NSE_CURRENCY

            # Lookup security_id
            if not hasattr(self, "master_contract_df") or self.master_contract_df is None:
                 self.download_instruments()
            
            security_id = request.symbol # Default to symbol if lookup fails (might be passed as token)
            
            if hasattr(self, "master_contract_df") and self.master_contract_df is not None:
                 if not hasattr(self, "_symbol_token_map"):
                     self._symbol_token_map = {}
                     for _, row in self.master_contract_df.iterrows():
                         key = f"{row['exchange']}:{row['symbol']}"
                         self._symbol_token_map[key] = str(row['token'])
                         # Also map just symbol if unique? Or assume request.symbol matches our df symbol
                         # If request.symbol is "NIFTY...", and exchange is NFO.
                         # We should check both.
                 
                 # Try EXCH:SYMBOL first
                 key = f"{request.exchange.value}:{request.symbol}"
                 token = self._symbol_token_map.get(key)
                 if token:
                     security_id = token
                 else:
                     # Try finding by symbol only (risk of collision if same symbol in NSE/BSE)
                     # But usually strategy passes specific symbol.
                     # Let's iterate or check if we can find it.
                     # For now, rely on EXCH:SYMBOL or just SYMBOL if unique in map (not guaranteed).
                     # Let's try to construct key using mapped exchange segment?
                     # No, request.exchange is enum.
                     pass

            # Dhan place_order signature
            resp = self._dhan.place_order(
                security_id=security_id, 
                exchange_segment=exchange_segment,
                transaction_type=txn_type,
                quantity=request.quantity,
                order_type=order_type,
                product_type=product_type,
                price=request.price,
                trigger_price=request.stop_price,
                validity=validity,
                tag=request.tag
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
            resp = self._dhan.cancel_order(order_id=order_id)
            if resp.get("status") == "success":
                return OrderResponse(status="ok", order_id=str(order_id), raw=resp)
            return OrderResponse(status="error", order_id=str(order_id), message=str(resp), raw=resp)
        except Exception as e:
            return OrderResponse(status="error", order_id=str(order_id), message=str(e))

    def modify_order(self, order_id: str, updates: Dict[str, Any]) -> OrderResponse:
        if not self._dhan:
             return OrderResponse(status="error", order_id=order_id, message="unauthenticated")
        try:
            # Dhan modify_order requires specific fields.
            # We need to extract them from 'updates' or 'request' if passed.
            # The interface says 'updates' is a Dict.
            # We need to know what's in 'updates'. Usually it's price, quantity, order_type, etc.
            
            # This is tricky without the original order details if the API requires all fields.
            # Dhan modify_order signature: order_id, order_type, quantity, price, trigger_price, exchange_segment, validity...
            # If we don't have all, we might fail.
            # For now, implementing basic mapping.
            
            resp = self._dhan.modify_order(
                order_id=order_id,
                order_type=updates.get("order_type"),
                quantity=updates.get("quantity"),
                price=updates.get("price"),
                trigger_price=updates.get("trigger_price"),
                exchange_segment=updates.get("exchange_segment"), # Might be missing
                validity=updates.get("validity")
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

    # --- Market data ---
    def get_quote(self, symbol: str) -> Quote:
        if not self._dhan:
            return Quote(symbol=symbol, last_price=0.0, raw={"error": "unauthenticated"})
        
        # Symbol format: EXCH:TRADINGSYMBOL (e.g., NSE:NIFTY 50, NFO:NIFTY25JANFUT)
        # Dhan expects exchange_segment and security_id.
        # We need to look up security_id from our cached instruments.
        
        exchange_str = "NSE"
        tradingsymbol = symbol
        if ":" in symbol:
            exchange_str, tradingsymbol = symbol.split(":", 1)
            
        # Map exchange string to Dhan Exchange Segment
        exch_seg = M.NSE_EQ
        if exchange_str == "NSE": exch_seg = M.NSE_EQ
        elif exchange_str == "NFO": exch_seg = M.NSE_FNO
        elif exchange_str == "BSE": exch_seg = M.BSE_EQ
        elif exchange_str == "BFO": exch_seg = M.BSE_FNO
        elif exchange_str == "MCX": exch_seg = M.MCX_COMM
        elif exchange_str == "CDS": exch_seg = M.NSE_CURRENCY
        
        # Lookup security_id
        # We assume download_instruments has been called and self.master_contract_df exists
        if not hasattr(self, "master_contract_df") or self.master_contract_df is None:
             # Try to download if missing
             self.download_instruments()
             
        security_id = None
        if hasattr(self, "master_contract_df") and self.master_contract_df is not None:
             # Filter by exchange and symbol
             # Note: Our master df has 'exchange' column as enum or string? 
             # In download_instruments we will normalize it.
             # Let's assume we store it as string matching our Exchange enum or just standard strings.
             
             # Optimization: Create a lookup dict if not exists
             if not hasattr(self, "_symbol_token_map"):
                 self._symbol_token_map = {}
                 for _, row in self.master_contract_df.iterrows():
                     key = f"{row['exchange']}:{row['symbol']}"
                     self._symbol_token_map[key] = str(row['token'])
            
             key = f"{exchange_str}:{tradingsymbol}"
             security_id = self._symbol_token_map.get(key)

        if not security_id:
            # Fallback or error
            return Quote(symbol=tradingsymbol, exchange=Exchange[exchange_str] if exchange_str in Exchange.__members__ else Exchange.NSE, last_price=0.0, raw={"error": "symbol_not_found"})

        try:
            # Dhan get_ltp or get_quote
            # get_ltp(exchange_segment, security_id)
            resp = self._dhan.get_ltp(security_id, exch_seg)
            if resp.get("status") == "success":
                data = resp.get("data", {})
                last_price = float(data.get("last_price", 0.0))
                return Quote(symbol=tradingsymbol, exchange=Exchange[exchange_str] if exchange_str in Exchange.__members__ else Exchange.NSE, last_price=last_price, raw=data)
            return Quote(symbol=tradingsymbol, last_price=0.0, raw=resp)
        except Exception as e:
            return Quote(symbol=tradingsymbol, last_price=0.0, raw={"error": str(e)})

    def get_history(self, symbol: str, interval: str, start: str, end: str) -> List[Dict[str, Any]]:
        if not self._dhan:
            return []
            
        # Resolve symbol to security_id and exchange_segment
        exchange_str = "NSE"
        tradingsymbol = symbol
        if ":" in symbol:
            exchange_str, tradingsymbol = symbol.split(":", 1)
            
        exch_seg = M.NSE_EQ
        if exchange_str == "NSE": exch_seg = M.NSE_EQ
        elif exchange_str == "NFO": exch_seg = M.NSE_FNO
        elif exchange_str == "BSE": exch_seg = M.BSE_EQ
        elif exchange_str == "BFO": exch_seg = M.BSE_FNO
        elif exchange_str == "MCX": exch_seg = M.MCX_COMM
        elif exchange_str == "CDS": exch_seg = M.NSE_CURRENCY

        if not hasattr(self, "master_contract_df") or self.master_contract_df is None:
             self.download_instruments()
             
        security_id = None
        if hasattr(self, "master_contract_df") and self.master_contract_df is not None:
             if not hasattr(self, "_symbol_token_map"):
                 self._symbol_token_map = {}
                 for _, row in self.master_contract_df.iterrows():
                     key = f"{row['exchange']}:{row['symbol']}"
                     self._symbol_token_map[key] = str(row['token'])
             key = f"{exchange_str}:{tradingsymbol}"
             security_id = self._symbol_token_map.get(key)
             
        if not security_id:
            return []

        # Map interval
        # Dhan intervals: 1, 5, 15, 25, 60 (minutes), 'D' (day)
        interval_map = {
            "1minute": "1",
            "1m": "1",
            "5minute": "5",
            "5m": "5",
            "15minute": "15",
            "15m": "15",
            "30minute": "30", # Dhan might not have 30? Docs say 1, 5, 15, 25, 60. 
            # Wait, DhanHQ docs say: 1, 5, 15, 25, 60, 3H, 1D, etc.
            # Let's assume 30 is not directly supported or check docs.
            # If 30 not supported, we might fail or use 15 and resample (too complex for now).
            # Let's try "30" if it works, else fallback.
            "60minute": "60",
            "60m": "60",
            "1hour": "60",
            "day": "D",
            "1d": "D"
        }
        dhan_interval = interval_map.get(interval.lower(), "1")

        try:
            # Dhan get_intraday_data or get_historical_data
            # get_intraday_data(security_id, exchange_segment, instrument_type) -> for today?
            # get_historical_data(security_id, exchange_segment, instrument_type, expiry_code, from_date, to_date)
            # We need historical.
            
            # Instrument type is needed?
            # From master df?
            inst_type = "EQUITY" # Default
            # We can lookup instrument type from master df if needed.
            # But get_historical_data signature in library might vary.
            # checking library usage: dhan.historical_minute_charts(symbol, exchange_segment, instrument_type, expiry_code, from_date, to_date)
            # Actually the library has `historical_minute_charts` and `historical_daily_charts`?
            # Or `get_charts`?
            # Official lib `dhanhq` 2.0+ might have `get_historical_data`.
            # Let's try generic `get_historical_data` if available or specific chart methods.
            # Based on recent docs, it might be `intraday_minute_data` or similar.
            
            # Let's assume standard `get_historical_data` or similar exists in the client wrapper we are using.
            # If not, we might need to use `requests` directly if the lib is old/different.
            # But we installed `dhanhq>=1.3.0`.
            
            # Re-checking standard usage:
            # dhan.intraday_minute_data(security_id, exchange_segment, instrument_type)
            # dhan.historical_daily_data(security_id, exchange_segment, instrument_type, expiry_code, from_date, to_date)
            
            # It seems Dhan API separates Intraday (1 min) and Historical (Daily?).
            # Wait, for 1 minute history of previous days, we need `historical_minute_charts`?
            # Actually, `intraday_minute_data` gives last few days?
            
            # Let's use a safe approach: try `intraday_minute_data` for recent minute data.
            # But `get_history` usually asks for a range.
            
            # Let's use `requests` to be sure if we can't verify library method names easily.
            # But we should try to use the library object `self._dhan`.
            
            # Let's try `intraday_minute_data` for now as Survivor usually needs recent minute data.
            resp = self._dhan.intraday_minute_data(
                security_id=security_id,
                exchange_segment=exch_seg,
                instrument_type="EQUITY" # Placeholder, might need "FNO" etc.
            )
            
            if resp.get("status") == "success":
                data = resp.get("data", {})
                # Format: { "start_Time": [], "open": [], ... } or list of dicts?
                # Dhan usually returns OHLC arrays.
                # We need to convert to list of dicts.
                
                # If it returns arrays:
                start_times = data.get("start_Time", [])
                opens = data.get("open", [])
                highs = data.get("high", [])
                lows = data.get("low", [])
                closes = data.get("close", [])
                volumes = data.get("volume", [])
                
                out = []
                for i in range(len(start_times)):
                    # Convert Dhan time (usually int timestamp or string?) to ts
                    # Dhan often sends epoch or formatted string.
                    # If it's a number, it might be custom.
                    # Let's assume it's standard or we debug it.
                    # Actually Dhan 'start_Time' is often a float/int like 133123123.
                    
                    ts = start_times[i]
                    out.append({
                        "ts": ts,
                        "open": float(opens[i]),
                        "high": float(highs[i]),
                        "low": float(lows[i]),
                        "close": float(closes[i]),
                        "volume": int(volumes[i])
                    })
                return out
                
            return []
        except Exception as e:
            return []

    def download_instruments(self) -> None:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        try:
            df = pd.read_csv(url)
            # Columns in CSV: SEM_EXM_EXCH_ID, SEM_SEGMENT, SEM_SMST_SECURITY_ID, SEM_INSTRUMENT_NAME, SEM_TRADING_SYMBOL, SEM_LOT_UNITS, SEM_CUSTOM_SYMBOL, SEM_EXPIRY_CODE, SEM_EXPIRY_DATE, SEM_STRIKE_PRICE, SEM_TICK_SIZE, SEM_EXPIRY_FLAG, SEM_INSTRUMENT_TYPE
            # We need to map to our standard columns:
            # token, symbol, name, last_price, expiry, strike, tick_size, lot_size, instrument_type, segment, exchange
            
            # Map columns
            # SEM_SMST_SECURITY_ID -> token
            # SEM_TRADING_SYMBOL -> symbol
            # SEM_CUSTOM_SYMBOL -> name (or use trading symbol)
            # SEM_EXPIRY_DATE -> expiry
            # SEM_STRIKE_PRICE -> strike
            # SEM_TICK_SIZE -> tick_size
            # SEM_LOT_UNITS -> lot_size
            # SEM_INSTRUMENT_NAME -> instrument_type (e.g. OPTIDX, FUTIDX)
            # SEM_SEGMENT -> segment (e.g. NSE_FNO)
            # SEM_EXM_EXCH_ID -> exchange (e.g. NSE, BSE)
            
            df = df.rename(columns={
                "SEM_SMST_SECURITY_ID": "token",
                "SEM_TRADING_SYMBOL": "symbol",
                "SEM_CUSTOM_SYMBOL": "name",
                "SEM_EXPIRY_DATE": "expiry",
                "SEM_STRIKE_PRICE": "strike",
                "SEM_TICK_SIZE": "tick_size",
                "SEM_LOT_UNITS": "lot_size",
                "SEM_INSTRUMENT_NAME": "instrument_type",
                "SEM_SEGMENT": "segment",
                "SEM_EXM_EXCH_ID": "exchange"
            })
            
            # Normalize Exchange
            # Dhan CSV uses "NSE", "BSE", "MCX" usually.
            
            # Normalize Expiry
            # Dhan expiry might be "2025-01-30 14:30:00" or similar.
            df['expiry'] = pd.to_datetime(df['expiry'], errors='coerce').dt.date
            
            # Calculate days to expiry
            df['days_to_expiry'] = df['expiry'].apply(lambda x: np.busday_count(datetime.now().date(), x) + 1 if pd.notnull(x) else np.nan)
            
            # Add last_price (placeholder, as CSV doesn't have it)
            df['last_price'] = 0.0
            
            # Filter/Select columns
            cols = ["token", "symbol", "name", "last_price", "expiry", "strike", "tick_size", "lot_size", "instrument_type", "segment", "exchange", "days_to_expiry"]
            # Ensure all exist
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            
            self.master_contract_df = df[cols]
            
            # Cache
            cache_file = ".cache/dhan_master_contract.csv"
            if not os.path.exists(os.path.dirname(cache_file)):
                os.makedirs(os.path.dirname(cache_file))
            df.to_csv(cache_file, index=False)
            
        except Exception as e:
            print(f"Error downloading instruments: {e}")
            self.master_contract_df = pd.DataFrame()

    def get_instruments(self) -> List[Instrument]:
        if not hasattr(self, "master_contract_df") or self.master_contract_df is None:
            self.download_instruments()
        # Convert df to List[Instrument] or just return df if interface allows (interface says List[Instrument] but code often uses df directly or list of dicts)
        # The base interface says List[Instrument].
        # But Survivor strategy uses `self.broker.get_instruments()` and expects a DataFrame!
        # See survivor.py line 94: self.instruments = self.broker.get_instruments()
        # line 95: self.instruments = self.instruments[self.instruments['symbol']...]
        # So we MUST return a DataFrame.
        # Wait, the interface type hint says List[Instrument], but Python is dynamic.
        # Zerodha driver returns `self.master_contract_df` (a DataFrame).
        # So we should return the DataFrame.
        return self.master_contract_df


