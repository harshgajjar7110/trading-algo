# Code Review Report: Automated Trading Strategy

This report details a comprehensive review of the Python-based automated trading strategy targeting the Zerodha broker. The review covers the `strategy` directory, the FastAPI backend (`web/backend/`), and the Next.js frontend (`web/frontend/`).

---

## 1. Strategy Modules (`strategy/`)

### 1.1 `strategy/survivor.py`
The Survivor strategy is a trend-following options selling strategy on NIFTY.

**Strengths:**
*   **Rich Feature Set**: Includes technical indicator filters (RSI, EMA, ADX), dynamic gap adjustment via ATR, trailing stops, stop-loss, profit targets, time-based exits, and volatility-based position sizing.
*   **Robust SL Handling**: Automatic SL order placement mapped back to a `PositionManager` ensuring positions remain protected even across restarts.
*   **Technical Integrity**: Uses Wilder's Smoothing for RSI computation correctly (via Pandas EWM), keeping adherence to traditional definitions.
*   **Extensive Logging**: Detailed diagnostics around filters, order executions, SL logic, and profit bookings, critical for debugging live algo systems.

**Issues / Areas for Improvement:**
*   **Hardcoded Values**: Many parameters, like default loop retries, tick sizes (e.g., `tick_size = 0.05`), or index strings, are hardcoded. These should ideally be driven by the broker's instrument dump or configuration files to support other assets (e.g., BankNifty) effortlessly.
*   **Data Structure Access Safety**: `ticks['last_price'] if 'last_price' in ticks else ticks['ltp']` is good, but deeper dictionary access in broker responses (like nested error dicts) should utilize `.get()` recursively to avoid `KeyError`.
*   **Redundant Profit Checks**: `check_profit_targets_at_startup` and `_check_profit_targets` share similar core logic. Abstracting the core profit calculation and exit logic into a helper method would DRY up the code.
*   **Historical Data Request Format Error (Memory Note)**: Ensure that dates passed to `get_history` use strict `YYYY-MM-DD` string formatting, as full timestamps can break the Kite API integration. The current `end_date = datetime.now().strftime('%Y-%m-%d')` is correct.

### 1.2 `strategy/wave.py`
The Wave strategy implements continuous buy/sell orders offset from the market price.

**Strengths:**
*   **Advanced Risk Restrictions**: Integrates Black-Scholes delta calculations via `mibian` to restrict one-sided runaway delta exposure (e.g., disabling sell orders if delta drops below `min_nifty_delta`).
*   **Adaptive Gaps**: Implements `multiplier_scale` to scale gaps based on position imbalance, helping to auto-hedge.

**Issues / Areas for Improvement:**
*   **Floating Point Math**: Price calculations involve standard floats (e.g., `price - scaled_buy_gap`). Prices should always be rounded to the nearest tick size of the instrument to prevent order rejection by the exchange due to invalid tick steps.
*   **Redundant Order Fetching/State Maintenance**: The code manually tracks `self.orders` and associated orders (`associated_order_id`). If an order executes or gets rejected and the WebSocket callback drops, the internal state will go out of sync with the broker. Utilizing the `OrderTracker` explicitly to reconcile with `broker.get_orderbook()` on intervals would improve resilience.
*   **Greeks Calculation Inefficiency**: `_get_portfolio_greeks` iterates over all open positions and recalculates Black-Scholes on the spot. In a high-frequency loop, this can be slow. Caching the greeks per instrument and updating them incrementally or lazily when price changes significantly would be more performant.
*   **Hardcoded Delta Bounds/Ticks**: Values like `/75` in margin calculation are hardcoded for specific lot sizes, breaking compatibility if exchange lot sizes change.

---

## 2. Web Backend (`web/backend/`)

### 2.1 `app/services/strategy_manager.py`
**Strengths:**
*   **Process Isolation**: Uses Python's `multiprocessing` to run strategies in completely isolated memory spaces, preventing the FastAPI event loop from being blocked by heavy strategy calculations.
*   **Queue-based IPC**: Uses `multiprocessing.Queue` to stream state updates back to the main API process safely.

**Issues / Areas for Improvement:**
*   **Process Teardown**: The stop method uses `.terminate()` followed by `.kill()` if it doesn't close in 10s. For trading algos, an abrupt kill might leave open unmanaged orders. A "soft stop" mechanism (like sending a poison pill via a queue) where the strategy cancels its pending orders and squares off before exiting gracefully is highly recommended.
*   **Global Singletons**: `StrategyManager` acts as a global singleton. While convenient, it makes unit testing the API harder.

### 2.2 `app/services/broker_service.py`
**Strengths:**
*   **Sanitized Errors**: The abstraction catches broker-specific exceptions and redacts sensitive info (API keys) effectively.
*   **Facade Pattern**: Acts as a clean facade so the rest of the application is agnostic to the underlying broker (Zerodha, Fyers, etc.).

**Issues / Areas for Improvement:**
*   **Reinitialization Race Conditions**: `reinitialize()` sets `self._broker = None`. In a highly concurrent async environment like FastAPI, two requests hitting this simultaneously might spawn duplicate broker clients. A threading/asyncio lock around `_ensure_broker()` is advised.

### 2.3 `app/main.py` & API Routes
**Strengths:**
*   **WebSockets**: Excellent implementation of a centralized `connection_manager` for real-time pushing of prices and strategy states.
*   **Lifespan Management**: Background tasks (`heartbeat_loop`, `state_broadcast_loop`) are cleanly scoped within the FastAPI lifespan context manager.

**Issues / Areas for Improvement:**
*   **WebSocket Broadcast Blocking**: `await connection_manager.broadcast()` in `state_broadcast_loop` could be slowed down if a client is misbehaving. The broadcast should ideally wrap individual client sends in `asyncio.create_task` so a slow client doesn't hold up the loop.

---

## 3. Web Frontend (`web/frontend/`)

### 3.1 `app/page.tsx` (Dashboard)
**Strengths:**
*   **Real-time Updates**: Reactivity is well managed. It uses standard HTTP polling (`fetchData` every 10s) coupled with WebSocket overrides for high-frequency data (like `NiftyData` quotes), striking a good balance between data freshness and server load.
*   **Responsive UI**: The UI cleanly handles varying states (loading, error, auth-required).
*   **Modular Rendering**: Complex data blocks (PayoffChart, GreeksDisplay, StrategySelector) are componentized, keeping the main page relatively clean.

**Issues / Areas for Improvement:**
*   **Stale State in Callbacks**: `handlePriceUpdate` uses the functional state update `setNiftyData(prev => ...)` correctly. However, if `niftyData` is initially null, the update might be ignored (`prev ? ... : null`). The initial fetch needs to ensure `niftyData` is populated, or the WS handler should initialize a stub if it arrives before the HTTP response.
*   **Sorting Logic Performance**: `getSortedPositions` sorts the array on every render if positions exist. For large position arrays, wrapping this in a `useMemo` dependent on `positions`, `sortField`, and `sortDirection` will prevent unnecessary sorting computations on unrelated re-renders (like price ticks).

---

## 4. Conclusion
The trading system architecture is robust, cleanly separating frontend display, backend coordination, and heavy strategy execution via isolated multiprocessing. The primary areas for improvement revolve around edge-case safety: enforcing tick-size rounding for dynamically calculated order prices, graceful process shutdowns to prevent orphaned orders, and optimizing React renders for high-frequency WebSocket updates.