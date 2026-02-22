from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import request

from ...core.enums import Exchange, OrderType, ProductType, TransactionType, Validity
from ...core.errors import MarginUnavailableError, UnsupportedOperationError
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
import numpy as np


def _get_env_file_path() -> Path:
    """Get the path to the .env file in project root."""
    # Navigate up from this file to find project root (where .env would be)
    current = Path(__file__).resolve()
    # Go up: integrations/zerodha -> integrations -> brokers -> trading-algo (project root)
    for _ in range(4):
        current = current.parent
        if (current / ".env").exists():
            return current / ".env"
    # Fallback: create in current working directory
    return Path.cwd() / ".env"


def _save_refresh_token_to_env(refresh_token: str) -> None:
    """Save refresh token to .env file."""
    env_path = _get_env_file_path()
    
    # Read existing content
    existing_content = ""
    if env_path.exists():
        with open(env_path, "r") as f:
            existing_content = f.read()
    
    lines = existing_content.splitlines()
    new_lines = []
    refresh_token_found = False
    
    for line in lines:
        if line.startswith("BROKER_REFRESH_TOKEN="):
            new_lines.append(f"BROKER_REFRESH_TOKEN={refresh_token}")
            refresh_token_found = True
        else:
            new_lines.append(line)
    
    if not refresh_token_found:
        # Add refresh token after other broker config
        broker_lines = [l for l in lines if l.startswith("BROKER_")]
        if broker_lines:
            # Insert after last BROKER_ line
            last_broker_idx = max(i for i, l in enumerate(lines) if l.startswith("BROKER_"))
            new_lines = lines[:last_broker_idx + 1] + [f"BROKER_REFRESH_TOKEN={refresh_token}"] + lines[last_broker_idx + 1:]
        else:
            new_lines.append(f"BROKER_REFRESH_TOKEN={refresh_token}")
    
    # Write back
    with open(env_path, "w") as f:
        f.write("\n".join(new_lines) + "\n")
    
    print(f"[ZerodhaDriver] Refresh token saved to {env_path}")


def _save_access_token_to_env(access_token: str) -> None:
    """Save access token to .env file."""
    env_path = _get_env_file_path()
    
    # Read existing content
    existing_content = ""
    if env_path.exists():
        with open(env_path, "r") as f:
            existing_content = f.read()
    
    lines = existing_content.splitlines()
    new_lines = []
    access_token_found = False
    
    for line in lines:
        if line.startswith("BROKER_ACCESS_TOKEN="):
            new_lines.append(f"BROKER_ACCESS_TOKEN={access_token}")
            access_token_found = True
        else:
            new_lines.append(line)
    
    if not access_token_found:
        # Add access token after other broker config
        broker_lines = [l for l in lines if l.startswith("BROKER_")]
        if broker_lines:
            # Insert after last BROKER_ line
            last_broker_idx = max(i for i, l in enumerate(lines) if l.startswith("BROKER_"))
            new_lines = lines[:last_broker_idx + 1] + [f"BROKER_ACCESS_TOKEN={access_token}"] + lines[last_broker_idx + 1:]
        else:
            new_lines.append(f"BROKER_ACCESS_TOKEN={access_token}")
    
    # Write back
    with open(env_path, "w") as f:
        f.write("\n".join(new_lines) + "\n")
    
    print(f"[ZerodhaDriver] Access token saved to {env_path}")


def _prompt_for_refresh_token() -> Optional[str]:
    """Prompt user to enter refresh token via console."""
    print("\n" + "=" * 60)
    print("ZERODHA AUTHENTICATION - Refresh Token Required")
    print("=" * 60)
    print("\nTo get your refresh token:")
    print("1. Login to Kite (kite.zerodha.com) in your browser")
    print("2. After login, go to Profile > API")
    print("3. Or use the TOTP login flow once to generate a refresh token")
    print("\nAlternatively, enter your refresh token below (or press Enter to skip):")
    
    try:
        refresh_token = input("Refresh Token: ").strip()
        if refresh_token:
            # Save it to .env
            _save_refresh_token_to_env(refresh_token)
            return refresh_token
    except (EOFError, KeyboardInterrupt):
        print("\n[ZerodhaDriver] Skipping refresh token input")
    
    return None

class ZerodhaDriver(BrokerDriver):
    """Zerodha driver using kiteconnect when available.

    This initial pass exposes the interface; concrete methods will be implemented
    incrementally to keep changes reviewable.
    """

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
            supports_master_contract=True,
            supports_option_chain=False,
            supports_gtt=True,
            supports_bracket_order=False,
            supports_cover_order=True,
            supports_multileg_order=False,
            supports_basket_orders=True,
            supports_exit_positions=True,
            supports_convert_position=True,
        )
        self._kite = None  # kiteconnect client if available
        self._kite_ws = None
        self.master_contract_df = None  # Initialize master_contract_df

        # Try to wire a ready KiteConnect if env provides api_key + access_token
        import os
        api_key = os.getenv("BROKER_API_KEY") or os.getenv("KITE_API_KEY") or os.getenv("ZERODHA_API_KEY")
        access_token = (
            os.getenv("BROKER_ACCESS_TOKEN") or os.getenv("KITE_ACCESS_TOKEN") or os.getenv("ZERODHA_ACCESS_TOKEN")
        )
        if api_key and access_token:
            try:  # pragma: no cover - external package
                from kiteconnect import KiteConnect  # type: ignore

                kite = KiteConnect(api_key=api_key)
                kite.set_access_token(access_token)
                self._kite = kite
            except Exception:
                self._kite = None

        # Manual login: use API key + secret to get login URL, then exchange request token for access token
        # Skip interactive login if running in non-interactive mode (web server, etc.)
        if self._kite is None:
            # Check if running in non-interactive mode
            import sys
            is_non_interactive = not sys.stdin.isatty() or os.getenv("BROKER_NON_INTERACTIVE") == "true"
            
            if is_non_interactive:
                # In non-interactive mode, don't block waiting for input
                # The web UI should handle authentication via /api/auth endpoints
                print("[ZerodhaDriver] Running in non-interactive mode. Use the web UI to authenticate:")
                print("[ZerodhaDriver] 1. Open http://localhost:3000/auth (or your frontend URL)")
                print("[ZerodhaDriver] 2. Follow the authentication flow")
            else:
                try:  # pragma: no cover - interactive
                    from kiteconnect import KiteConnect  # type: ignore
                    from ...auth.manual import manual_exchange_request_token

                    api_key2 = api_key or os.getenv("KITE_API_KEY") or os.getenv("ZERODHA_API_KEY")
                    api_secret = os.getenv("BROKER_API_SECRET") or os.getenv("KITE_API_SECRET") or os.getenv("ZERODHA_API_SECRET")
                    if api_key2 and api_secret:
                        kite2 = KiteConnect(api_key=api_key2)
                        url = kite2.login_url()
                        print(f"\n" + "=" * 60)
                        print("ZERODHA MANUAL LOGIN")
                        print("=" * 60)
                        print(f"\n1. Open this URL in your browser:\n{url}\n")
                        print("2. Login with your Zerodha credentials")
                        print("3. After login, you will be redirected to a page with 'request_token'")
                        print("4. Copy the request_token value from the URL and paste below\n")
                        request_token = manual_exchange_request_token(url)
                        sess = kite2.generate_session(request_token, api_secret)
                        token = sess.get("access_token")
                        if token:
                            kite2.set_access_token(token)
                            self._kite = kite2
                            print("[ZerodhaDriver] Authentication successful!")
                            # Save access token to .env for future use
                            _save_access_token_to_env(token)
                except Exception as e:
                    # Keep unauthenticated if manual flow fails
                    print(f"[ZerodhaDriver] Manual login failed: {e}")
                    pass

    def _authenticate_via_totp(self) -> Optional[Any]:
        """Programmatic TOTP login using Zerodha web endpoints to obtain access token.

        First checks for existing refresh token in env vars. If found, uses it to
        get a new access token. Otherwise, performs full TOTP login and saves the
        refresh token to .env for future use.

        Requires env vars:
        - BROKER_API_KEY (or KITE_API_KEY/ZERODHA_API_KEY)
        - BROKER_API_SECRET (or KITE_API_SECRET/ZERODHA_API_SECRET)
        - BROKER_ID (for TOTP login)
        - BROKER_TOTP_KEY (for TOTP login)
        - BROKER_PASSWORD (for TOTP login)
        - BROKER_REFRESH_TOKEN (optional - if provided, skips TOTP login)
        """
        import os
        try:  # pragma: no cover - external packages
            import requests  # type: ignore
            import pyotp  # type: ignore
            from kiteconnect import KiteConnect  # type: ignore
        except Exception:
            return None

        api_key = os.getenv("BROKER_API_KEY") or os.getenv("KITE_API_KEY") or os.getenv("ZERODHA_API_KEY")
        api_secret = os.getenv("BROKER_API_SECRET") or os.getenv("KITE_API_SECRET") or os.getenv("ZERODHA_API_SECRET")
        
        if not all([api_key, api_secret]):
            return None

        # Step 1: Check for existing refresh token
        refresh_token = os.getenv("BROKER_REFRESH_TOKEN")
        if refresh_token:
            print("[ZerodhaDriver] Found refresh token in env, attempting to get new access token...")
            kite = self._try_refresh_token(api_key, api_secret, refresh_token)
            if kite:
                return kite
            print("[ZerodhaDriver] Refresh token failed, falling back to TOTP login...")

        # Step 2: Check for TOTP credentials
        broker_id = os.getenv("BROKER_ID")
        totp_secret = os.getenv("BROKER_TOTP_KEY")
        password = os.getenv("BROKER_PASSWORD")
        
        if not all([broker_id, totp_secret, password]):
            # Try prompting for refresh token via console
            print("[ZerodhaDriver] Missing TOTP credentials (BROKER_ID, BROKER_TOTP_KEY, BROKER_PASSWORD)")
            refresh_token = _prompt_for_refresh_token()
            if refresh_token:
                kite = self._try_refresh_token(api_key, api_secret, refresh_token)
                if kite:
                    return kite
            return None

        # Step 3: Perform full TOTP login
        try:
            session = requests.Session()
            login_resp = session.post(
                "https://kite.zerodha.com/api/login",
                data={"user_id": broker_id, "password": password},
                timeout=30,
            )
            login_data = login_resp.json()
            if "data" not in login_data:
                return None
            request_id = login_data["data"]["request_id"]

            twofa_resp = session.post(
                "https://kite.zerodha.com/api/twofa",
                data={
                    "user_id": broker_id,
                    "request_id": request_id,
                    "twofa_value": pyotp.TOTP(totp_secret).now(),
                },
                timeout=30,
            )
            twofa_data = twofa_resp.json()
            if "data" not in twofa_data:
                return None

            connect_url = f"https://kite.trade/connect/login?api_key={api_key}"
            connect_resp = session.get(connect_url, allow_redirects=True, timeout=30)
            if "request_token=" not in connect_resp.url:
                return None
            request_token = connect_resp.url.split("request_token=")[1].split("&")[0]

            kite = KiteConnect(api_key=api_key)
            sess = kite.generate_session(request_token, api_secret)
            access_token = sess.get("access_token")
            if not access_token:
                return None
            kite.set_access_token(access_token)
            
            # Save refresh token for future use
            new_refresh_token = sess.get("refresh_token")
            if new_refresh_token:
                _save_refresh_token_to_env(new_refresh_token)
                print("[ZerodhaDriver] Authentication successful, refresh token saved to .env")
            
            return kite
        except Exception as e:
            print(f"[ZerodhaDriver] TOTP login failed: {e}")
            return None

    def _try_refresh_token(self, api_key: str, api_secret: str, refresh_token: str) -> Optional[Any]:
        """Try to get a new access token using refresh token.
        
        Note: KiteConnect doesn't have a direct refresh token API. The refresh token
        is used to get a new request token via the Kite web session.
        """
        try:  # pragma: no cover
            import requests  # type: ignore
            from kiteconnect import KiteConnect  # type: ignore
            
            # Create a session with the refresh token
            session = requests.Session()
            
            # Set up cookies with the refresh token
            # The refresh token works as a session cookie for kite.zerodha.com
            session.cookies.set("enctoken", refresh_token, domain=".zerodha.com")
            
            # Try to access the connect endpoint to get a new request token
            connect_url = f"https://kite.trade/connect/login?api_key={api_key}"
            connect_resp = session.get(connect_url, allow_redirects=True, timeout=30)
            
            if "request_token=" in connect_resp.url:
                request_token = connect_resp.url.split("request_token=")[1].split("&")[0]
                
                kite = KiteConnect(api_key=api_key)
                sess = kite.generate_session(request_token, api_secret)
                access_token = sess.get("access_token")
                
                if access_token:
                    kite.set_access_token(access_token)
                    print("[ZerodhaDriver] Successfully obtained access token from refresh token")
                    
                    # Save new refresh token if provided
                    new_refresh_token = sess.get("refresh_token")
                    if new_refresh_token and new_refresh_token != refresh_token:
                        _save_refresh_token_to_env(new_refresh_token)
                    
                    return kite
            
            print("[ZerodhaDriver] Could not get request token from refresh token")
            return None
            
        except Exception as e:
            print(f"[ZerodhaDriver] Refresh token authentication failed: {e}")
            return None

    # --- Account ---
    def get_funds(self) -> Funds:
        if not self._kite:
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw={"error": "unauthenticated"})
        try:
            data = self._kite.margins(segment="equity")
            equity = float(data.get("net", 0))
            available_cash = float(data.get("available", {}).get("cash", 0))
            used_margin = float(data.get("utilised", {}).get("debits", 0))
            net = float(data.get("net", 0))
            return Funds(equity=equity, available_cash=available_cash, used_margin=used_margin, net=net, raw=data)
        except Exception as e:  # noqa: BLE001
            return Funds(equity=0.0, available_cash=0.0, used_margin=0.0, net=0.0, raw={"error": str(e)})

    def get_positions(self) -> List[Position]:
        if not self._kite:
            return []
        try:
            pos = self._kite.positions()
            combined: List[Position] = []
            # Use 'net' positions only to avoid double counting
            # 'net' contains the overall position including day trades
            for p in pos.get("net", []):
                # Skip positions with zero quantity
                quantity_total = int(p.get("quantity", 0))
                if quantity_total == 0:
                    continue
                    
                exchange = Exchange[p.get("exchange", "NSE").upper()]
                quantity_available = int(p.get("quantity", 0)) - int(p.get("overnight_quantity", 0))
                avg_price = float(p.get("average_price", 0))
                pnl = float(p.get("pnl", 0))
                combined.append(
                    Position(
                        symbol=p.get("tradingsymbol"),
                        exchange=exchange,
                        quantity_total=quantity_total,
                        quantity_available=quantity_available,
                        average_price=avg_price,
                        pnl=pnl,
                        product_type=(
                            ProductType.MARGIN
                            if p.get("product") == "NRML"
                            else (ProductType.INTRADAY if p.get("product") == "MIS" else ProductType.CNC)
                        ),
                        raw=p,
                    )
                )
            return combined
        except Exception:
            return []

    # --- Orders ---
    def place_order(self, request: OrderRequest) -> OrderResponse:
        if not self._kite:
            return OrderResponse(status="error", order_id=None, message="unauthenticated")
        try:
            order_type = M.order_type["zerodha"][request.order_type]
            product = M.product_type["zerodha"][request.product_type]
            txn_type = M.transaction_type["zerodha"][request.transaction_type]
            validity = M.validity["zerodha"][request.validity]
            if request.price <= 0:
                request.price = 0.05
            order_id = self._kite.place_order(
                variety=self._kite.VARIETY_REGULAR,
                exchange=request.exchange.value,
                tradingsymbol=request.symbol,
                transaction_type=txn_type,
                quantity=request.quantity,
                product=product,
                order_type=order_type,
                price=request.price if request.order_type == OrderType.LIMIT else None,
                validity=validity,
                trigger_price=request.stop_price,
                tag=request.tag,
            )
            resp = OrderResponse(status="ok", order_id=str(order_id), raw={"order_id": order_id})
            # Optional: immediately notify via callback that order placement succeeded
            if isinstance(resp, OrderResponse) and resp.status == "ok":
                if getattr(self, "_on_order_update_cb", None):
                    try:
                        self._on_order_update_cb(None, {"event": "order_update", "status": "ok", "order_id": str(order_id), "message": None, "raw": {"order_id": order_id}})
                    except Exception:
                        pass
                return resp
            
            if isinstance(resp, OrderResponse) and resp.status == "error":
                return OrderResponse(status="error", order_id=str(resp.order_id), raw=resp.to_dict() if isinstance(resp, OrderResponse) else None)
            
            return OrderResponse(status="error", order_id=-1, message=str(resp), raw=resp.to_dict() if isinstance(resp, OrderResponse) else None)
        except Exception as e:  # noqa: BLE001
            # Emit synthetic order error update to mimic broker event stream for testing
            if getattr(self, "_on_order_update_cb", None):
                try:
                    self._on_order_update_cb(None, {"event": "order_update", "status": "error", "order_id": None, "message": str(e)})
                except Exception:
                    pass
            return OrderResponse(status="error", order_id=None, message=str(e))

    def cancel_order(self, order_id: str) -> OrderResponse:
        if not self._kite:
            return OrderResponse(status="error", order_id=order_id, message="unauthenticated")
        try:
            resp = self._kite.cancel_order(variety=self._kite.VARIETY_REGULAR, order_id=order_id)
            return OrderResponse(status="ok", order_id=str(order_id), raw=resp)
        except Exception as e:  # noqa: BLE001
            return OrderResponse(status="error", order_id=str(order_id), message=str(e))

    def modify_order(self, order_id: str, updates: Dict[str, Any]) -> OrderResponse:
        if not self._kite:
            return OrderResponse(status="error", order_id=order_id, message="unauthenticated")
        try:
            resp = self._kite.modify_order(variety=self._kite.VARIETY_REGULAR, order_id=order_id, **updates)
            return OrderResponse(status="ok", order_id=str(order_id), raw=resp)
        except Exception as e:  # noqa: BLE001
            return OrderResponse(status="error", order_id=str(order_id), message=str(e))

    def get_orderbook(self) -> List[Dict[str, Any]]:
        if not self._kite:
            return []
        try:
            return self._kite.orders()
        except Exception:
            return []

    def get_tradebook(self) -> List[Dict[str, Any]]:
        if not self._kite:
            return []
        try:
            return self._kite.trades()
        except Exception:
            return []

    # --- Market data ---
    def get_quote(self, symbol: str) -> Quote:
        if not self._kite:
            return Quote(symbol=symbol.split(":", 1)[-1], exchange=Exchange[symbol.split(":", 1)[0]] if ":" in symbol else Exchange.NSE, last_price=0.0, raw={"error": "unauthenticated"})
        data = self._kite.quote(symbol)
        payload = next(iter(data.values()))
        last_price = float(payload.get("last_price", 0.0))
        exch, tradingsymbol = symbol.split(":", 1)
        return Quote(symbol=tradingsymbol, exchange=Exchange[exch], last_price=last_price, raw=data)

    def get_history(self, symbol: str, interval: str, start: str, end: str, oi: bool = False) -> List[Dict[str, Any]]:
        if not self._kite:
            return []
        exch, tradingsymbol = symbol.split(":", 1)
        # Normalize common interval aliases to Kite format
        imap = {
            "3m": "3minute",
            "5m": "5minute",
            "10m": "10minute",
            "15m": "15minute",
            "30m": "30minute",
            "60m": "60minute",
            "1d": "day",
            "day": "day",
        }
        key = interval.strip().lower()
        interval_kite = imap.get(key, None)

        if interval_kite is None:
            raise Exception(f"Invalid interval: {interval}")
        try:
            try:
                instruments = self._kite.instruments(exch)
            except Exception:
                instruments = self._kite.instruments()
            token = None
            for inst in instruments:
                if inst.get("exchange") == exch and inst.get("tradingsymbol") == tradingsymbol:
                    token = inst.get("instrument_token")
                    break
            if token is None and exch == "NSE":
                for inst in self._kite.instruments("NFO"):
                    if inst.get("tradingsymbol") == tradingsymbol:
                        token = inst.get("instrument_token")
                        break
            if token is None:
                return []
            data = self._kite.historical_data(token, from_date=start, to_date=end, interval=interval_kite)
            # Normalize to [{ts, open, high, low, close, volume}]
            out: List[Dict[str, Any]] = []
            for c in data or []:
                try:
                    dt = c.get("date")
                    ts = int(getattr(dt, "timestamp", lambda: None)()) if dt is not None else None
                    if ts is None:
                        # Attempt to coerce using pandas-like to_pydatetime if present
                        ts = int(dt.to_pydatetime().timestamp()) if hasattr(dt, "to_pydatetime") else None
                except Exception:
                    ts = None
                out.append({
                    "ts": ts,
                    "open": float(c.get("open", 0.0)),
                    "high": float(c.get("high", 0.0)),
                    "low": float(c.get("low", 0.0)),
                    "close": float(c.get("close", 0.0)),
                    "volume": int(c.get("volume", 0)) if c.get("volume") is not None else None,
                    "oi": int(c.get("oi", 0)) if c.get("oi") is not None else None,
                })
            return out
        except Exception as e:
            print(f"Error getting history: {e}")
            return []

    # --- Instruments ---
    def download_instruments(self) -> None:
        self.cache_file = ".cache/zerodha_master_contract.csv"
        
        # Try to load from cache first
        if os.path.exists(self.cache_file):
            try:
                self.master_contract_df = pd.read_csv(self.cache_file)
                self.master_contract_df['expiry'] = pd.to_datetime(self.master_contract_df['expiry']).dt.date
                return  # Successfully loaded from cache
            except Exception as e:
                print(f"Warning: Failed to load instruments from cache: {e}")
                self.master_contract_df = None
        
        # If cache doesn't exist or failed to load, fetch from API
        if not self._kite:
            print("Warning: No kite connection and no cache available for instruments")
            return
        
        df = pd.DataFrame(self._kite.instruments())
        columns = ["instrument_token", "exchange_token", "tradingsymbol", "name", "last_price", "expiry", "strike", "tick_size", "lot_size", "instrument_type", "segment", "exchange"]
        header_mapping = {
            "instrument_token": "token",
            "exchange_token": "exchange_token",
            "tradingsymbol": "symbol",
            "name": "name",
            "last_price": "last_price",
            "expiry": "expiry",
            "strike": "strike",
            "tick_size": "tick_size",
            "lot_size": "lot_size",
            "instrument_type": "instrument_type",
            "segment": "segment",
            "exchange": "exchange"
        }
        df = df[columns]
        df.columns = list(header_mapping.values())
        df['expiry'] = pd.to_datetime(df['expiry']).dt.date
        df['days_to_expiry'] = df['expiry'].apply(lambda x: np.busday_count(datetime.now().date(), x) + 1 if not pd.isna(x) else np.nan)
        self.master_contract_df = df
        
        # Save to cache
        try:
            if not os.path.exists(os.path.dirname(self.cache_file)):
                os.makedirs(os.path.dirname(self.cache_file))
            df.to_csv(self.cache_file, index=False)
        except Exception as e:
            print(f"Warning: Failed to save instruments to cache: {e}")

    def get_instruments(self) -> List[Instrument]:
        if self.master_contract_df is None:
            # Try to download if not already done
            self.download_instruments()
        return self.master_contract_df

    # --- Option chain ---
    def get_option_chain(self, underlying: str, exchange: str, **kwargs: Any) -> List[Dict[str, Any]]:
        if not self._kite:
            return []
        # Accept either raw underlying name or EXCH:UNDERLYING
        if ":" in underlying:
            _, underlying_name = underlying.split(":", 1)
        else:
            underlying_name = underlying
        try:
            instruments = self._kite.instruments(exchange)
        except Exception:
            instruments = []
        out = [
            i
            for i in instruments
            if i.get("name") == underlying_name and i.get("segment") in ("NFO-OPT", "BFO-OPT")
        ]
        return out

    # --- WS ---
    def connect_websocket(
        self,
        *,
        on_ticks: Any | None = None,
        on_connect: Any | None = None,
        on_error: Any | None = None,
        on_close: Any | None = None,
        on_reconnect: Any | None = None,
        on_noreconnect: Any | None = None,
    ) -> None:
        if not self._kite:
            return
        try:  # pragma: no cover - external package
            from kiteconnect import KiteTicker  # type: ignore

            # Obtain existing tokens from client
            api_key = getattr(self._kite, "api_key", None)
            access_token = getattr(self._kite, "access_token", None) or getattr(self._kite, "_access_token", None)
            if not (api_key and access_token):
                return
            ws = KiteTicker(api_key=api_key, access_token=access_token)
            # Assign callbacks if provided
            if on_ticks is not None:
                ws.on_ticks = on_ticks
            if on_connect is not None:
                ws.on_connect = on_connect
            if on_error is not None:
                ws.on_error = on_error
            if on_close is not None:
                ws.on_close = on_close
            if on_reconnect is not None and hasattr(ws, "on_reconnect"):
                ws.on_reconnect = on_reconnect
            if on_noreconnect is not None and hasattr(ws, "on_noreconnect"):
                ws.on_noreconnect = on_noreconnect
            self._kite_ws = ws
            ws.connect(threaded=True)
        except Exception:
            return

    def symbols_to_subscribe(self, symbols: List[str]) -> None:  # type: ignore[override]
        # Zerodha expects instrument tokens. We need to map EXCH:SYMBOL to tokens using instruments API.
        if not self._kite_ws or not self._kite:
            return
        try:
            instruments = []
            try:
                instruments = self._kite.instruments()
            except Exception:
                instruments = []
            index: Dict[str, int] = {}
            for inst in instruments:
                key = f"{inst.get('exchange')}:{inst.get('tradingsymbol')}"
                tok = inst.get("instrument_token")
                if key and tok is not None:
                    index[key] = int(tok)
            tokens: List[int] = []
            for s in symbols:
                if isinstance(s, int):
                    tokens.append(int(s))
                elif isinstance(s, str) and ":" in s:
                    tok = index.get(s)
                    if tok is not None:
                        tokens.append(int(tok))
            if tokens:
                self._kite_ws.subscribe(tokens)
                if hasattr(self._kite_ws, "set_mode"):
                    self._kite_ws.set_mode(self._kite_ws.MODE_FULL, tokens)
        except Exception:
            return

    def connect_order_websocket(
        self,
        *,
        on_order_update: Any | None = None,
        on_trades: Any | None = None,
        on_positions: Any | None = None,
        on_general: Any | None = None,
        on_error: Any | None = None,
        on_close: Any | None = None,
        on_connect: Any | None = None,
    ) -> None:
        # KiteTicker uses on_order_update on the same socket
        # If data websocket already connected, attach order update callback
        ws = getattr(self, "_kite_ws", None)
        if on_order_update is not None:
            setattr(self, "_on_order_update_cb", on_order_update)
        if ws is None:
            # If not connected, attempt a connection with provided callbacks
            self.connect_websocket(on_ticks=None, on_connect=on_connect, on_error=on_error, on_close=on_close)
            ws = getattr(self, "_kite_ws", None)
        if ws is not None and on_order_update is not None and hasattr(ws, "on_order_update"):
            try:
                ws.on_order_update = on_order_update
            except Exception:
                pass

    def unsubscribe(self, symbols: List[str]) -> None:  # type: ignore[override]
        return None

    # --- Margins ---
    def get_margins_required(self, orders: List[Dict[str, Any]] | List[OrderRequest]) -> Any:
        if not self._kite:
            raise MarginUnavailableError("Zerodha margins unavailable: unauthenticated")
        try:
            payload: List[Dict[str, Any]] = []
            for o in orders:
                if isinstance(o, OrderRequest):
                    payload.append(
                        {
                            "exchange": o.exchange.value,
                            "tradingsymbol": o.symbol,
                            "transaction_type": M.transaction_type["zerodha"][o.transaction_type],
                            "variety": "regular",
                            "product": M.product_type["zerodha"][o.product_type],
                            "order_type": M.order_type["zerodha"][o.order_type],
                            "quantity": int(o.quantity),
                            "price": float(o.price) if o.price is not None else None,
                            "trigger_price": float(o.stop_price) if o.stop_price is not None else None,
                        }
                    )
                else:
                    payload.append(o)
            return self._kite.order_margins(payload)
        except Exception as e:  # noqa: BLE001
            raise MarginUnavailableError(f"Zerodha order_margins failed: {e}") from e

    def get_span_margin(self, orders: List[Dict[str, Any]]) -> Any:
        return self.get_margins_required(orders)

    def get_multiorder_margin(self, orders: List[Dict[str, Any]]) -> Any:
        return self.get_margins_required(orders)

    # --- Profile ---
    def get_profile(self) -> Dict[str, Any]:
        if not self._kite:
            return {"error": "unauthenticated"}
        try:
            return self._kite.profile()
        except Exception as e:  # noqa: BLE001
            return {"error": str(e)}

    def exit_positions(self, symbol: Optional[str] = None, exchange: Optional[str] = None, 
                      product_type: Optional[ProductType] = None, cancel_pending_orders: bool = True) -> Dict[str, Any]:
        """
        Bulk exit all or filtered positions.
        
        Args:
            symbol: Optional symbol to filter positions (exit only this symbol)
            exchange: Optional exchange to filter positions
            product_type: Optional product type to filter positions
            cancel_pending_orders: Whether to cancel pending orders for the symbols being exited
            
        Returns:
            Dict with 'success', 'failed', 'errors' lists and summary
        """
        if not self._kite:
            return {"error": "unauthenticated", "success": [], "failed": [], "errors": ["Not authenticated"]}
        
        results = {
            "success": [],
            "failed": [],
            "errors": [],
            "cancelled_orders": [],
            "summary": {
                "total_positions": 0,
                "exited": 0,
                "failed": 0
            }
        }
        
        try:
            # Get current positions
            positions = self.get_positions()
            
            # Filter positions if needed
            filtered_positions = []
            for pos in positions:
                # Skip positions with zero quantity
                if pos.quantity_total == 0:
                    continue
                    
                # Apply filters
                if symbol and pos.symbol != symbol:
                    continue
                if exchange and pos.exchange.value != exchange:
                    continue
                if product_type and pos.product_type != product_type:
                    continue
                    
                filtered_positions.append(pos)
            
            results["summary"]["total_positions"] = len(filtered_positions)
            
            if not filtered_positions:
                results["errors"].append("No matching positions found")
                return results
            
            # Cancel pending orders for these symbols if requested
            if cancel_pending_orders:
                try:
                    orders = self.get_orderbook()
                    for order in orders:
                        order_status = order.get("status", "").upper()
                        if order_status in ["OPEN", "PENDING", "AMO"]:
                            order_symbol = order.get("tradingsymbol", "")
                            # Check if this order is for a position we're exiting
                            for pos in filtered_positions:
                                if pos.symbol == order_symbol:
                                    try:
                                        cancel_resp = self.cancel_order(str(order.get("order_id")))
                                        if cancel_resp.status == "ok":
                                            results["cancelled_orders"].append({
                                                "order_id": order.get("order_id"),
                                                "symbol": order_symbol,
                                                "status": "cancelled"
                                            })
                                    except Exception as e:
                                        results["errors"].append(f"Failed to cancel order {order.get('order_id')}: {str(e)}")
                                    break
                except Exception as e:
                    results["errors"].append(f"Error cancelling pending orders: {str(e)}")
            
            # Place exit orders for each position
            for pos in filtered_positions:
                try:
                    # Determine transaction type (opposite of position)
                    # Positive quantity = long position -> SELL to exit
                    # Negative quantity = short position -> BUY to exit
                    if pos.quantity_total > 0:
                        txn_type = TransactionType.SELL
                        quantity = pos.quantity_total
                    else:
                        txn_type = TransactionType.BUY
                        quantity = abs(pos.quantity_total)
                    
                    # Create exit order request
                    exit_request = OrderRequest(
                        symbol=pos.symbol,
                        exchange=pos.exchange,
                        quantity=quantity,
                        order_type=OrderType.MARKET,
                        transaction_type=txn_type,
                        product_type=pos.product_type,
                        tag="bulk_exit"
                    )
                    
                    # Place the exit order
                    resp = self.place_order(exit_request)
                    
                    if resp.status == "ok":
                        results["success"].append({
                            "symbol": pos.symbol,
                            "exchange": pos.exchange.value,
                            "quantity": quantity,
                            "transaction_type": txn_type.value,
                            "order_id": resp.order_id,
                            "product_type": pos.product_type.value
                        })
                        results["summary"]["exited"] += 1
                    else:
                        results["failed"].append({
                            "symbol": pos.symbol,
                            "error": resp.message or "Unknown error"
                        })
                        results["summary"]["failed"] += 1
                        
                except Exception as e:
                    results["failed"].append({
                        "symbol": pos.symbol,
                        "error": str(e)
                    })
                    results["summary"]["failed"] += 1
            
            return results
            
        except Exception as e:
            results["errors"].append(f"Unexpected error in exit_positions: {str(e)}")
            return results

    def convert_position(self, exchange: str, symbol: str, transaction_type: TransactionType,
                        position_type: str, quantity: int, old_product: ProductType, 
                        new_product: ProductType) -> OrderResponse:
        """
        Convert position from one product type to another (e.g., MIS to NRML).
        
        Args:
            exchange: Exchange code (e.g., "NSE", "NFO")
            symbol: Trading symbol
            transaction_type: BUY or SELL
            position_type: "overnight" or "day"
            quantity: Quantity to convert
            old_product: Current product type
            new_product: Target product type
            
        Returns:
            OrderResponse with status and order_id
        """
        if not self._kite:
            return OrderResponse(status="error", order_id=None, message="unauthenticated")
        
        try:
            # Map product types to Zerodha format
            old_product_str = "NRML" if old_product == ProductType.MARGIN else "MIS"
            new_product_str = "NRML" if new_product == ProductType.MARGIN else "MIS"
            
            txn_type_str = "BUY" if transaction_type == TransactionType.BUY else "SELL"
            
            resp = self._kite.convert_position(
                exchange=exchange,
                tradingsymbol=symbol,
                transaction_type=txn_type_str,
                position_type=position_type,
                quantity=quantity,
                old_product=old_product_str,
                new_product=new_product_str
            )
            
            return OrderResponse(
                status="ok",
                order_id=str(resp.get("order_id", "")),
                raw=resp
            )
        except Exception as e:
            return OrderResponse(status="error", order_id=None, message=str(e))

    def place_basket_orders(self, requests: List[OrderRequest]) -> List[OrderResponse]:
        """
        Place multiple orders atomically (best effort).
        For Zerodha, we place orders sequentially but return all results.
        This is useful for placing fresh order + SL order together.
        
        Args:
            requests: List of OrderRequest objects
            
        Returns:
            List of OrderResponse objects (one per request)
        """
        if not self._kite:
            return [OrderResponse(status="error", order_id=None, message="unauthenticated") for _ in requests]
        
        results: List[OrderResponse] = []
        
        for req in requests:
            try:
                resp = self.place_order(req)
                results.append(resp)
            except Exception as e:
                results.append(OrderResponse(status="error", order_id=None, message=str(e)))
        
        return results

    def place_gtt_order(self, symbol: str, quantity: int, price: float, transaction_type: str, order_type: str, exchange: str, product: str, tag: str = "Unknown", limit_price: Optional[float] = None) -> OrderResponse:
        """
        Place a Single-leg GTT order.
        
        Args:
            symbol: Trading symbol (e.g., "INFY", "NIFTY23OCT19500CE")
            quantity: Quantity to transact
            price: The trigger price
            transaction_type: "BUY" or "SELL"
            order_type: "LIMIT" (GTT orders are typically LIMIT)
            exchange: Exchange (e.g., "NSE", "NFO")
            product: Product type (e.g., "CNC", "NRML", "MIS")
            tag: Optional tag
            limit_price: The execution price (for LIMIT orders). If None, defaults to trigger price.
        """
        if not self._kite:
            return OrderResponse(status="error", order_id=None, message="unauthenticated")
        
        try:
            # Use provided limit_price or fallback to trigger price
            execution_price = limit_price if limit_price is not None else price
            
            # Construct the single leg order
            order_obj = {
                "exchange": exchange,
                "tradingsymbol": symbol,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "order_type": order_type,
                "product": product,
                "price": execution_price,
                "tag": tag
            }
            
            # Prepare symbol for quote fetching
            if ":" not in symbol:
                quote_symbol = f"{exchange}:{symbol}"
            else:
                quote_symbol = symbol
            
            # Get last price
            current_last_price = 0.0
            try:
                q = self.get_quote(quote_symbol)
                current_last_price = q.last_price
            except Exception:
                pass
            
            # Place GTT
            resp = self._kite.place_gtt(
                trigger_type=self._kite.GTT_TYPE_SINGLE,
                tradingsymbol=symbol if ":" not in symbol else symbol.split(":")[1],
                exchange=exchange,
                trigger_values=[price],
                last_price=current_last_price,
                orders=[order_obj]
            )
            
            # Resp example: {'trigger_id': 12345}
            t_id = str(resp.get('trigger_id'))
            return OrderResponse(status="ok", order_id=t_id, raw=resp)
            
        except Exception as e:
            return OrderResponse(status="error", order_id=None, message=str(e))

    def place_gtt_oco_order(
        self,
        symbol: str,
        exchange: str,
        product: str,
        transaction_type: str,
        quantity: int,
        stop_loss_trigger: float,
        stop_loss_limit: float,
        target_trigger: float,
        target_limit: float,
        tag: str = "Unknown"
    ) -> OrderResponse:
        """
        Place an OCO (Two-leg) GTT order (One Cancels Other).
        Typically used for Stop Loss and Target.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            product: Product type
            transaction_type: Transaction type for the exit orders (usually opposite of entry)
            quantity: Quantity to transact
            stop_loss_trigger: Trigger price for Stop Loss
            stop_loss_limit: Execution price for Stop Loss
            target_trigger: Trigger price for Target
            target_limit: Execution price for Target
            tag: Optional tag
        """
        if not self._kite:
            return OrderResponse(status="error", order_id=None, message="unauthenticated")

        try:
            # Construct the orders list
            # Order 1: Stop Loss
            order_sl = {
                "exchange": exchange,
                "tradingsymbol": symbol,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "order_type": "LIMIT",
                "product": product,
                "price": stop_loss_limit,
                "tag": tag
            }

            # Order 2: Target
            order_target = {
                "exchange": exchange,
                "tradingsymbol": symbol,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "order_type": "LIMIT",
                "product": product,
                "price": target_limit,
                "tag": tag
            }

            # Prepare symbol for quote fetching
            if ":" not in symbol:
                quote_symbol = f"{exchange}:{symbol}"
            else:
                quote_symbol = symbol

            # Get last price
            current_last_price = 0.0
            try:
                q = self.get_quote(quote_symbol)
                current_last_price = q.last_price
            except Exception:
                pass

            # Place GTT OCO
            # Note: The order of trigger_values must match the order of orders.
            resp = self._kite.place_gtt(
                trigger_type=self._kite.GTT_TYPE_OCO,
                tradingsymbol=symbol if ":" not in symbol else symbol.split(":")[1],
                exchange=exchange,
                trigger_values=[stop_loss_trigger, target_trigger],
                last_price=current_last_price,
                orders=[order_sl, order_target]
            )

            # Resp example: {'trigger_id': 12345}
            t_id = str(resp.get('trigger_id'))
            return OrderResponse(status="ok", order_id=t_id, raw=resp)

        except Exception as e:
            return OrderResponse(status="error", order_id=None, message=str(e))



