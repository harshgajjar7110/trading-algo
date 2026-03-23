# Code Review Report: Automated Options Selling Trading Strategy

**Date:** October 2023
**Scope:** `strategy/` directory and `web/` directory
**Context:** Python-based automated trading strategy using Zerodha broker, focused on theta decay (options selling).

---

## 1. Strategy Module (`strategy/`)

### 1.1 `survivor.py`
**Overview:** The core strategy algorithm for the Survivor options selling approach. It places limit orders (SL and trailing SL), adjusts bounds dynamically with ATR, and uses RSI, EMA, and ADX filters.

**Strengths:**
- **Risk Management:** Excellent implementations for SL reconciliation, daily loss limits, maximum consecutive losses, and trailing stop losses.
- **Modularity:** Advanced breakdown of methods (`_handle_pe_trade_enhanced`, `_handle_ce_trade_enhanced`).
- **Dynamic Adaptability:** Utilization of ATR to adapt gap sizing (`_get_dynamic_gap`).
- **Safety Flags:** Strict limits on position counts per side to avoid over-exposure.

**Areas for Improvement / Risks:**
- **Code Duplication:** `_handle_pe_trade_enhanced` and `_handle_ce_trade_enhanced` contain almost identical logic. They could be refactored into a single generic method `_handle_trade_enhanced(option_type: str, current_price, indicators)` to follow DRY principles.
- **Sync vs Async in Tick Handling:** The `on_ticks_update` acts synchronously inside a fast-paced tick update loop. Although it delegates state logic well, large computation inside the tick loop could cause tick drops during high volatility.
- **Profit Checks in Ticks:** `_check_profit_targets` runs inside the tick loop but has a time-gated check (`profit_check_interval`). This is acceptable but could be decoupled into a separate background asynchronous thread/task.

### 1.2 `wave.py`
**Overview:** Implements the "wave" trading system using `mibian` to compute Greeks (Delta) to enforce directional neutrality.

**Strengths:**
- **Greek-based Dynamic Constraints:** Outstanding implementation of Delta constraints to toggle buy/sell restrictions per instrument type.
- **Margin Computation Logic:** Calculates rough margin requirement limits to avoid account wipeout.

**Areas for Improvement / Risks:**
- **Synchronous `time.sleep`:** `_prepare_final_prices` and the `place_wave_order` main loops make use of `time.sleep()`. In an event-driven or async system (like websockets), `time.sleep` blocks the entire thread, halting tick processing. Consider migrating this to an asynchronous sleep or an event scheduler.
- **Order Tracking Cleanups:** Order references are kept in `self.orders`. There's potential for dictionary bloat if orders are rejected or drop out of websocket updates without cleanup.
- **Hardcoded Greeks Settings:** Values like `interest_rate = 10` are hardcoded defaults (even if configurable). Market parameters change, and dynamically fetching risk-free rates might be safer.

### 1.3 `position_manager.py`
**Overview:** Memory-based tracking of positions and Stop-Loss (SL) mappings. Reconciles state upon startup to prevent unprotected overnight/re-started positions.

**Strengths:**
- **Stateless/Memory Design:** Avoids local state file corruptions by reconciling directly with the broker.
- **Robust SL mapping:** Identifies unmatched position quantities and fires SL orders for unprotected legs.

**Areas for Improvement / Risks:**
- **Partial Fills Handling:** The manager effectively maps SL quantities. However, if a fresh SL order gets partially filled or rejected, there isn't an immediate retry loop in `_place_sl_for_position`, leaving the remaining quantity unprotected until the next reconciliation (which only happens at startup).
- **Hardcoded "NFO" Exchange:** The checks `exchange == Exchange.NFO` restrict the strategy strictly to NFO. This limits scalability if currency options (CDS) or commodity options (MCX) are added later.

### 1.4 `gap_risk_manager.py`
**Overview:** Modulates position sizes depending on the day of the week and handles overnight gap protections.

**Strengths:**
- **Weekend Risk Control:** Excellent logic mapping Friday (0.4x) and Monday (0.5x) multipliers to account for weekend theta decay but also the weekend gap risks.
- **GIFT Nifty Integration:** A smart approach to look at GIFT Nifty pre-market signals to adjust overnight gaps before Indian markets open.

**Areas for Improvement / Risks:**
- **Cache Invalidation:** The `_gift_nifty_cache` exists but the validation logic `_cache_validity_minutes` doesn't seem strongly tied into `assess_gap_risk`. It directly calls `pre_market_service.fetch_all_data()` in the strategy which might block execution if the API is slow.

---

## 2. Web / Backend Module (`web/backend/`)

### 2.1 `app/main.py`
**Overview:** FastAPI application initialization, websocket routes, and lifespan management.

**Strengths:**
- **Modern Async Setup:** Makes excellent use of FastAPI's `lifespan` for startup and shutdown background tasks (`heartbeat_loop`, `state_broadcast_loop`).
- **Clean Routing:** Very well structured modular router prefixing (`/api`).

**Areas for Improvement / Risks:**
- **Catch-All Exception in WebSocket:** In `websocket_endpoint`, a generic `except Exception as e` simply prints to console and disconnects the client. Logging the exception payload would aid in debugging silent websocket drops.
- **CORS Config:** The `allow_origins=settings.CORS_ORIGINS` is standard, but `allow_credentials=True` paired with `allow_origins=["*"]` (if misconfigured in settings) is a security hazard in production. Ensure `CORS_ORIGINS` is strictly defined.

### 2.2 `app/services/strategy_manager.py`
**Overview:** Responsible for spinning up strategy sub-processes (`multiprocessing`), passing configurations, and marshaling state between the backend and the active strategy.

**Strengths:**
- **Process Isolation:** The use of `multiprocessing.Process` ensures that the heavy computation of the trading strategy doesn't block the FastAPI event loop.
- **State Queue:** Employs a `multiprocessing.Queue` to stream `STATE_UPDATE` back to the FastAPI web server.
- **Auto-Stop on Critical Error:** Checks for `insufficient_fund_keywords` to autonomously stop a runaway algo. Excellent risk mitigation.

**Areas for Improvement / Risks:**
- **Process Cleanup:** `self._process.terminate()` followed by `kill()` is good. However, if the process spawned daemon threads (e.g. websocket connections to broker), terminating the process abruptly can leave dangling connections on the broker's side.
- **Queue Build-Up:** `_monitor_state` uses `get_nowait()`. If the strategy process floods the queue with `STATE_UPDATE` messages faster than the API consumes them, the queue will consume RAM infinitely. A max size on `multiprocessing.Queue(maxsize=1000)` is recommended.

### 2.3 `app/services/broker_service.py`
**Overview:** Interface between the application and the actual broker driver (Zerodha, etc.).

**Strengths:**
- **Robust Error Handling:** Checks if positions fail due to authentication and automatically prompts an actionable error (`"Not authenticated with broker"`).
- **Data Normalization:** Maps raw broker dictionaries into rigorous Pydantic schema models (`Position`, `Quote`, `Order`).

**Areas for Improvement / Risks:**
- **Authentication State Caching:** The broker is fetched via `BrokerGateway.from_name(os.getenv("BROKER_NAME"))`. If the access token expires mid-day, the user must explicitly trigger a re-auth. A token refresh auto-detection loop would make it truly hands-free.

---

## 3. General Architecture & Best Practices

1. **Test Coverage:** Ensure there are sufficient Pytest tests mocking the `BrokerGateway` to test the edge cases like Partial Order Fills and Rejections.
2. **Configuration Secrets:** Ensure `.env` files are used strictly and never checked into source control.
3. **Threading vs Async:** Both `survivor.py` and `wave.py` use synchronous Python logic. If they receive websocket ticks synchronously, the execution might drift behind live market time during high-tick-rate events (e.g., Fed interest rate announcements). Switching to `asyncio` inside the strategy tick handlers could improve throughput.

---

**Conclusion:**
The codebase is extremely solid and demonstrates a high level of domain expertise in algorithmic trading. The risk management layers (SL reconciliation, Gap risk modifiers, Volatility scaling) are exceptionally well-thought-out. The primary recommendations revolve around replacing synchronous blocks (`time.sleep`) in the strategies and refactoring duplicated code logic.